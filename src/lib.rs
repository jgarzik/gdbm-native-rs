//
// lib.rs -- GDBM core library API routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

extern crate base64;

use std::{
    fs::OpenOptions,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write},
};

mod avail;
mod bucket;
pub mod dir;
mod hashutil;
mod header;
mod magic;
mod ser;
use avail::{AvailBlock, AvailElem};
use bucket::{Bucket, BucketCache};
use dir::Directory;
use hashutil::{key_loc, PartialKey};
use header::Header;
use ser::{write32, write64, Alignment, Endian};

// Our claimed GDBM lib version compatibility.  Appears in dump files.
const COMPAT_GDBM_VERSION: &str = "1.23";

const IGNORE_SMALL: usize = 4;

pub enum ExportBinMode {
    ExpNative,
    Exp32,
    Exp64,
}

// read and return file data stored at (ofs,total_size)
// todo:  use Read+Seek traits rather than File
fn read_ofs(f: &mut std::fs::File, ofs: u64, total_size: usize) -> io::Result<Vec<u8>> {
    let mut data: Vec<u8> = vec![0; total_size];

    f.seek(SeekFrom::Start(ofs))?;
    f.read_exact(&mut data)?;

    Ok(data)
}

#[derive(Copy, Clone)]
pub struct GdbmOptions {
    pub readonly: bool,
    pub creat: bool,
}

// #[derive(Debug)]
pub struct Gdbm {
    pathname: String,
    cfg: GdbmOptions,

    f: std::fs::File,
    pub header: Header,
    pub dir: Directory,
    bucket_cache: BucketCache,
    cur_bucket_ofs: u64,
    cur_bucket_dir: usize,

    iter_key: Option<Vec<u8>>,
}

impl Gdbm {
    // API: open database file, read and validate header
    pub fn open(pathname: &str, dbcfg: &GdbmOptions) -> io::Result<Gdbm> {
        // derive open options
        let opt_write: bool = !dbcfg.readonly;
        let opt_create = if dbcfg.readonly { false } else { dbcfg.creat };

        // open filesystem file
        let mut f = OpenOptions::new()
            .read(true)
            .write(opt_write)
            .create(opt_create)
            .open(pathname)?;
        let metadata = f.metadata()?;

        // read gdbm global header
        let header = Header::from_reader(&metadata, &mut f)?;
        println!("{:?}", header);

        // read gdbm hash directory
        f.seek(SeekFrom::Start(header.dir_ofs))?;
        let dir =
            Directory::from_reader(header.alignment(), header.endian(), header.dir_sz, &mut f)?;

        // ensure all bucket offsets are reasonable
        if !dir.validate(header.block_sz as u64, header.next_block, header.block_sz) {
            return Err(Error::new(
                ErrorKind::Other,
                "corruption: bucket offset outside of file",
            ))?;
        }

        let cur_bucket_dir: usize = 0;
        let cur_bucket_ofs = dir.dir[cur_bucket_dir];

        // success; create new Gdbm object
        Ok(Gdbm {
            pathname: pathname.to_string(),
            cfg: *dbcfg,
            f,
            header,
            dir,
            bucket_cache: BucketCache::new(),
            cur_bucket_ofs,
            cur_bucket_dir,
            iter_key: None,
        })
    }

    fn export_ascii_header(&self, outf: &mut std::fs::File) -> io::Result<()> {
        // TODO: add ctime() to "created by" output line
        writeln!(outf, "# GDBM dump file created by {}", COMPAT_GDBM_VERSION)?;
        writeln!(outf, "#:version=1.1")?;
        writeln!(outf, "#:file={}", self.pathname)?;
        writeln!(outf, "#:format=standard")?;
        writeln!(outf, "# End of header")?;
        Ok(())
    }

    fn export_ascii_datum(&self, outf: &mut std::fs::File, bindata: &[u8]) -> io::Result<()> {
        const MAX_DUMP_LINE_LEN: usize = 76;

        writeln!(outf, "#:len={}", bindata.len())?;

        let mut b64 = base64::encode(bindata);

        while b64.len() > MAX_DUMP_LINE_LEN {
            let line = &b64[..MAX_DUMP_LINE_LEN];
            let rem = &b64[MAX_DUMP_LINE_LEN..];

            writeln!(outf, "{}", line)?;

            b64 = rem.to_string();
        }
        writeln!(outf, "{}", b64)?;

        Ok(())
    }

    fn export_ascii_records(&mut self, outf: &mut std::fs::File) -> io::Result<usize> {
        let mut n_written: usize = 0;
        let mut key_res = self.first_key()?;
        while key_res.is_some() {
            let key = key_res.unwrap();
            let val_res = self.get(&key)?;
            let val = val_res.unwrap();

            self.export_ascii_datum(outf, &key)?;
            self.export_ascii_datum(outf, &val)?;

            key_res = self.next_key(&key)?;
            n_written += 1;
        }
        Ok(n_written)
    }

    fn export_ascii_footer(&self, outf: &mut std::fs::File, n_written: usize) -> io::Result<()> {
        writeln!(outf, "#:count={}", n_written)?;
        writeln!(outf, "# End of data")?;
        Ok(())
    }

    // API: export database to ASCII dump file
    pub fn export_ascii(&mut self, outf: &mut std::fs::File) -> io::Result<()> {
        self.export_ascii_header(outf)?;
        let n_written = self.export_ascii_records(outf)?;
        self.export_ascii_footer(outf, n_written)?;
        Ok(())
    }

    fn export_bin_header(&self, outf: &mut std::fs::File) -> io::Result<()> {
        write!(
            outf,
            "!\r\n! GDBM FLAT FILE DUMP -- THIS IS NOT A TEXT FILE\r\n"
        )?;
        write!(outf, "! {}\r\n!\r\n", COMPAT_GDBM_VERSION)?;
        Ok(())
    }

    fn export_bin_datum(
        &self,
        outf: &mut std::fs::File,
        alignment: Alignment,
        bindata: &[u8],
    ) -> io::Result<()> {
        // write metadata:  big-endian datum size, 32b or 64b
        match alignment {
            Alignment::Align32 => write32(Endian::Big, outf, bindata.len() as u32)?,
            Alignment::Align64 => write64(Endian::Big, outf, bindata.len() as u64)?,
        }

        // write datum
        outf.write_all(bindata)?;

        Ok(())
    }

    fn export_bin_records(
        &mut self,
        outf: &mut std::fs::File,
        alignment: Alignment,
    ) -> io::Result<()> {
        let mut key_res = self.first_key()?;
        while key_res.is_some() {
            let key = key_res.unwrap();
            let val_res = self.get(&key)?;
            let val = val_res.unwrap();

            self.export_bin_datum(outf, alignment, &key)?;
            self.export_bin_datum(outf, alignment, &val)?;

            key_res = self.next_key(&key)?;
        }
        Ok(())
    }

    // API: export database to binary dump file
    pub fn export_bin(&mut self, outf: &mut std::fs::File, mode: ExportBinMode) -> io::Result<()> {
        let alignment = match mode {
            ExportBinMode::ExpNative => self.header.alignment(),
            ExportBinMode::Exp32 => Alignment::Align32,
            ExportBinMode::Exp64 => Alignment::Align64,
        };

        self.export_bin_header(outf)?;
        self.export_bin_records(outf, alignment)?;
        Ok(())
    }

    // read bucket into bucket cache.
    fn cache_load_bucket(&mut self, bucket_dir: usize) -> io::Result<()> {
        self.cur_bucket_dir = bucket_dir;
        self.cur_bucket_ofs = self.dir.dir[bucket_dir];

        if !self.bucket_cache.contains(self.cur_bucket_ofs) {
            self.f.seek(SeekFrom::Start(self.cur_bucket_ofs))?;
            let bucket = Bucket::from_reader(&self.header, &mut self.f)?;
            self.bucket_cache.insert(self.cur_bucket_ofs, bucket);
        }

        Ok(())
    }

    // return a reference to the current bucket
    fn current_bucket(&self) -> &Bucket {
        // note: assumes will be called following cache_load_bucket() to cache
        // assignment of dir[0] to cur_bucket at Gdbm{} creation not sufficient.
        let bucket = self
            .bucket_cache
            .bucket_map
            .get(&self.cur_bucket_ofs)
            .unwrap();

        bucket
    }

    // return a mutable reference to the current bucket
    fn current_bucket_mut(&mut self) -> &mut Bucket {
        // note: assumes will be called following cache_load_bucket() to cache
        // assignment of dir[0] to cur_bucket at Gdbm{} creation not sufficient.
        let bucket = self
            .bucket_cache
            .bucket_map
            .get_mut(&self.cur_bucket_ofs)
            .unwrap();

        bucket
    }

    // since one bucket dir entry may duplicate another,
    // this function returns the next non-dup bucket dir
    fn next_bucket_dir(&self, bucket_dir_in: usize) -> usize {
        let dir_max_elem = self.dir.dir.len();
        if bucket_dir_in >= dir_max_elem {
            return dir_max_elem;
        }

        let mut bucket_dir = bucket_dir_in;

        let cur_ofs = self.dir.dir[bucket_dir];
        while bucket_dir < dir_max_elem && cur_ofs == self.dir.dir[bucket_dir] {
            bucket_dir += 1;
        }

        bucket_dir
    }

    // API: count entries in database
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&mut self) -> io::Result<usize> {
        let mut len: usize = 0;
        let mut cur_dir: usize = 0;
        let dir_max_elem = self.dir.dir.len();
        while cur_dir < dir_max_elem {
            self.cache_load_bucket(cur_dir)?;
            let bucket = self.current_bucket();
            len += bucket.count as usize;

            cur_dir = self.next_bucket_dir(cur_dir);
        }

        Ok(len)
    }

    // given a bucket ptr, return the next key
    fn int_next_key(&mut self, elem_opt: Option<usize>) -> io::Result<Option<Vec<u8>>> {
        let mut elem_ofs = elem_opt.map(|n| n + 1).unwrap_or_default();
        let mut found = None;
        while found.is_none() {
            // finished current bucket. get next bucket.
            if elem_ofs == self.header.bucket_elems as usize {
                elem_ofs = 0;

                // find next bucket.  many dir entries may duplicate
                // the current bucket, so skip dups.
                let cur_dir = self.next_bucket_dir(self.cur_bucket_dir);
                if cur_dir == self.dir.len() {
                    break; // reached end of bucket dir - no more keys
                }

                // load new bucket
                self.cache_load_bucket(cur_dir)?;
            }

            // any valid hash will do
            let elem = &self.current_bucket().tab[elem_ofs];
            found = elem.is_occupied().then_some((elem.data_ofs, elem.key_size));

            elem_ofs += 1;
        }

        let data = match found {
            Some((offset, size)) => Some(read_ofs(&mut self.f, offset, size as usize)?),
            None => None,
        };

        Ok(data)
    }

    // API: return first key in database, for sequential iteration start.
    pub fn first_key(&mut self) -> io::Result<Option<Vec<u8>>> {
        // get first bucket
        self.cache_load_bucket(0)?;

        // start iteration - return next key
        self.int_next_key(None)
    }

    // API: return next key, for given key, in db-wide sequential order.
    pub fn next_key(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let get_opt = self.int_get(key)?;
        if get_opt.is_none() {
            return Ok(None);
        }

        let (elem_ofs, _data) = get_opt.unwrap();

        self.int_next_key(Some(elem_ofs))
    }

    // API: does key exist?
    pub fn contains_key(&mut self, key: &[u8]) -> io::Result<bool> {
        let get_opt = self.int_get(key)?;
        match get_opt {
            None => Ok(false),
            Some(_v) => Ok(true),
        }
    }

    // retrieve record data, and element offset in bucket, for given key
    fn int_get(&mut self, key: &[u8]) -> io::Result<Option<(usize, Vec<u8>)>> {
        let (key_hash, bucket_dir, elem_ofs) =
            key_loc(self.header.dir_bits, self.header.bucket_elems, key);
        let key_start = PartialKey::new(key);

        self.cache_load_bucket(bucket_dir)?;

        let bucket_entries = (0..self.header.bucket_elems)
            .map(|index| ((index + elem_ofs) % self.header.bucket_elems) as usize)
            .map(|offset| (offset, self.current_bucket().tab[offset]))
            .take_while(|(_, elem)| elem.is_occupied())
            .filter(|(_, elem)| {
                elem.hash == key_hash
                    && elem.key_size == key.len() as u32
                    && elem.key_start == key_start
            })
            .collect::<Vec<_>>();

        let data_entries = bucket_entries
            .into_iter()
            .map(|(offset, elem)| {
                read_ofs(
                    &mut self.f,
                    elem.data_ofs,
                    (elem.key_size + elem.data_size) as usize,
                )
                .map(|data| (offset, data))
            })
            .collect::<io::Result<Vec<_>>>()?;

        let result = data_entries
            .into_iter()
            .filter(|(_, data)| data[..key.len()] == *key)
            .map(|(offset, data)| (offset, data[key.len()..].to_vec()))
            .next();

        Ok(result)
    }

    // API: Fetch record value, given a key
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let get_opt = self.int_get(key)?;
        match get_opt {
            None => Ok(None),
            Some(data) => Ok(Some(data.1)),
        }
    }

    // virtually allocate N blocks of data, at end of db file (no I/O)
    fn extend(&mut self, size: u32) -> io::Result<(u64, u32)> {
        let offset = self.header.next_block;
        let length = match size % self.header.block_sz {
            0 => size / self.header.block_sz,
            _ => size / self.header.block_sz + 1,
        } * self.header.block_sz;

        self.header.next_block += length as u64;
        self.header.dirty = true;

        Ok((offset, length))
    }

    // Free list is full.  Split in half, and store 1/2 in new list block.
    fn push_avail_block(&mut self) -> io::Result<()> {
        let (header_elems, new_elems) = avail::partition_elems(&self.header.avail.elems);

        // write extension block to storage (immediately)
        let new_blk_ofs = {
            let block = AvailBlock::new(
                new_elems.len() as u32,
                self.header.avail.next_block,
                new_elems,
            );
            let offset = self.allocate_record(block.extent(self.header.alignment()))?;
            self.f.seek(SeekFrom::Start(offset))?;
            block.serialize(self.header.alignment(), self.header.endian(), &mut self.f)?;
            offset
        };

        self.header.avail = AvailBlock::new(self.header.avail.sz, new_blk_ofs, header_elems);
        self.header.dirty = true;

        Ok(())
    }

    // pops a block of the avail block list into the header block, only if it can accommodate it
    fn pop_avail_block(&mut self) -> io::Result<()> {
        let next_addr = self.header.avail.next_block;

        let next = {
            self.f.seek(SeekFrom::Start(self.header.avail.next_block))?;
            AvailBlock::from_reader(self.header.alignment(), self.header.endian(), &mut self.f)?
        };

        if let Some(block) = self.header.avail.merge(&next) {
            self.header.avail = block;
            self.header.dirty = true;

            // free the block we just merged
            self.free_record(
                next_addr,
                AvailBlock::sizeof(self.header.alignment())
                    + next.sz * AvailElem::sizeof(self.header.alignment()),
            )?;
        }

        Ok(())
    }

    // Add (addr,sz) to db-wide free list
    fn free_record(&mut self, addr: u64, sz: u32) -> io::Result<()> {
        // simply forget elements too small to worry about
        if (sz as usize) <= IGNORE_SMALL {
            return Ok(());
        }

        // build element to be inserted into free-space list
        let elem = AvailElem { sz, addr };

        // smaller items go into bucket avail list
        let bucket = self.current_bucket();
        if sz < self.header.block_sz && (bucket.avail.len() as u32) < Bucket::AVAIL {
            // insort into bucket avail vector, sorted by size
            let pos = bucket.avail.binary_search(&elem).unwrap_or_else(|e| e);
            self.current_bucket_mut().avail.insert(pos, elem);
            self.bucket_cache.dirty(self.cur_bucket_ofs);

            // success (and no I/O performed)
            return Ok(());
        }

        // larger items go into the header avail list
        // (and also when bucket avail list is full)
        if self.header.avail.elems.len() == self.header.avail.sz as usize {
            self.push_avail_block()?;
        }
        assert!(self.header.avail.elems.len() < self.header.avail.sz as usize);

        // insort into header avail vector, sorted by size
        let pos = self
            .header
            .avail
            .elems
            .binary_search(&elem)
            .unwrap_or_else(|e| e);
        self.header.avail.elems.insert(pos, elem);

        // header needs to be written
        self.header.dirty = true;

        Ok(())
    }

    fn write_bucket(&mut self, bucket_ofs: u64, bucket: &Bucket) -> io::Result<()> {
        self.f.seek(SeekFrom::Start(bucket_ofs))?;
        bucket.serialize(self.header.alignment(), self.header.endian(), &mut self.f)?;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_buckets(&mut self) -> io::Result<()> {
        let dirty_list = self.bucket_cache.dirty_list();

        // write out each dirty bucket
        for bucket_ofs in dirty_list {
            assert!(self.bucket_cache.contains(bucket_ofs));
            let bucket = self.bucket_cache.bucket_map[&bucket_ofs].clone();

            self.write_bucket(bucket_ofs, &bucket)?;
        }

        // nothing in cache remains dirty
        self.bucket_cache.clear_dirty();

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_dir(&mut self) -> io::Result<()> {
        if !self.dir.dirty {
            return Ok(());
        }

        self.f.seek(SeekFrom::Start(self.header.dir_ofs))?;
        self.dir
            .serialize(self.header.alignment(), self.header.endian(), &mut self.f)?;

        self.dir.dirty = false;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_header(&mut self) -> io::Result<()> {
        if !self.header.dirty {
            return Ok(());
        }

        self.f.seek(SeekFrom::Start(0))?;
        self.header.serialize(&mut self.f)?;

        self.header.dirty = false;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_dirty(&mut self) -> io::Result<()> {
        self.write_buckets()?;
        self.write_dir()?;
        self.write_header()?;

        Ok(())
    }

    // API: ensure database is flushed to stable storage
    pub fn sync(&mut self) -> io::Result<()> {
        self.writeable()?;

        self.write_dirty()?;
        self.f.sync_data()?;

        Ok(())
    }

    // API: remove a key/value pair from db, given a key
    pub fn remove(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.writeable()?;

        let get_opt = self.int_get(key)?;
        if get_opt.is_none() {
            return Ok(None);
        }
        let (elem_ofs, data) = get_opt.unwrap();

        let elem = self.current_bucket_mut().remove(elem_ofs);
        self.bucket_cache.dirty(self.cur_bucket_ofs);

        // release record bytes to available-space pool
        self.free_record(elem.data_ofs, elem.key_size + elem.data_size)?;

        // flush any dirty pages to OS
        self.write_dirty()?;

        Ok(Some(data))
    }

    fn allocate_from_bucket(&mut self, size: u32) -> Option<(u64, u32)> {
        let bucket = self.current_bucket_mut();
        let elem = avail::remove_elem(&mut bucket.avail, size);

        if elem.is_some() {
            self.bucket_cache.dirty(self.cur_bucket_ofs);
        }

        elem.map(|elem| (elem.addr, elem.sz))
    }

    fn allocate_from_header(&mut self, size: u32) -> io::Result<Option<(u64, u32)>> {
        if self.header.avail.elems.len() as u32 > self.header.avail.sz / 2 {
            self.pop_avail_block()?;
        }

        let elem = self.header.avail.remove_elem(size);

        if elem.is_some() {
            self.header.dirty = true;
        }

        Ok(elem.map(|elem| (elem.addr, elem.sz)))
    }

    fn allocate_record(&mut self, size: u32) -> io::Result<u64> {
        let (offset, length) = match self.allocate_from_bucket(size) {
            Some((offset, length)) => (offset, length),
            None => match self.allocate_from_header(size)? {
                Some((offset, length)) => (offset, length),
                None => self.extend(size)?,
            },
        };

        self.free_record(offset + size as u64, length - size)?;

        Ok(offset)
    }

    pub fn iter_reset(&mut self) {
        self.iter_key = None;
    }

    // Convenience function to convert readonly flag into an error if we want to write
    fn writeable(&self) -> io::Result<()> {
        (!self.cfg.readonly)
            .then_some(())
            .ok_or_else(|| Error::new(ErrorKind::Other, "write to readonly db"))
    }
}

impl Iterator for Gdbm {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_key = match self.iter_key.take() {
            None => self.first_key().expect("DB first_key I/O error"),
            Some(key) => self.next_key(&key).expect("DB next_key I/O error"),
        };

        self.iter_key.clone_from(&next_key);

        next_key
    }
}
