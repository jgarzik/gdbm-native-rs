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

use base64::Engine;
use std::{
    fs::OpenOptions,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write},
};

mod avail;
mod bucket;
pub mod dir;
mod hashutil;
mod header;
mod import;
pub mod magic;
pub mod ser;

use avail::AvailBlock;
use bucket::{Bucket, BucketCache, BucketElement};
use dir::{build_dir_size, Directory};
use hashutil::{bucket_dir, key_loc, PartialKey};
use header::Header;
use import::{ASCIIImportIterator, BinaryImportIterator};
use ser::{write32, write64, Alignment, Endian, Layout, Offset};

#[cfg(target_os = "linux")]
use std::os::linux::fs::MetadataExt;
#[cfg(target_os = "macos")]
use std::os::macos::fs::MetadataExt;

// Our claimed GDBM lib version compatibility.  Appears in dump files.
const COMPAT_GDBM_VERSION: &str = "1.23";

const IGNORE_SMALL: usize = 4;

pub const DEFAULT_CACHESIZE: usize = 4 * 1024 * 1024;

#[derive(Copy, Clone, Debug)]
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

#[derive(Copy, Clone, Debug)]
pub struct GdbmOptions {
    /// Override default alignement when opening a database.
    /// Explicitly sel alignment when creating a new DB. Default is 64bit.
    pub alignment: Option<Alignment>,
    /// Explicitly set offset width when creating a new DB. Default is 64bit (LFS).
    pub offset: Option<Offset>,
    /// Explicitly set edianness when creating a new DB. Default is LE.
    pub endian: Option<Endian>,
    /// Open readonly, conflicts with creat and newdb.
    pub readonly: bool,
    /// Create a new database if DB file is non-existent or has 0 length.
    /// Conflicts with readonly.
    pub creat: bool,
    /// Force creation of new DB. Conflicts with readonly.
    pub newdb: bool,
    /// Explicitly set block size, or use system default.
    /// If block size is less than 512, the system default is used.
    pub block_size: Option<u32>,
    /// Only create DB if exact block_size can be accommodated.
    pub bsexact: bool,
    /// use numsync when creating a new DB.
    pub numsync: bool,
    /// Bytesize of in-memory bucket cache (defaults to DEFAULT_CACHESIZE)
    pub cachesize: Option<usize>,
}

#[derive(Copy, Clone, Debug)]
pub struct ConvertOptions {
    pub numsync: bool,
}

// #[derive(Debug)]
pub struct Gdbm {
    pathname: String,
    cfg: GdbmOptions,

    f: std::fs::File,
    pub header: Header,
    pub dir: Directory,
    bucket_cache: BucketCache,
}

impl Gdbm {
    // API: open database file, read and validate header
    pub fn open(pathname: &str, dbcfg: &GdbmOptions) -> io::Result<Gdbm> {
        if dbcfg.readonly && (dbcfg.newdb || dbcfg.creat) {
            Err(Error::new(
                ErrorKind::Other,
                "readonly conflicts with newdb or creat",
            ))?;
        }

        let mut f = OpenOptions::new()
            .read(true)
            .write(!dbcfg.readonly)
            .create(dbcfg.creat | dbcfg.newdb)
            .truncate(dbcfg.newdb)
            .open(pathname)?;

        let metadata = f.metadata()?;

        if metadata.len() == 0 && !(dbcfg.creat || dbcfg.newdb) {
            Err(Error::new(ErrorKind::Other, "empty database"))?;
        }

        let (header, dir, initial_bucket) = match metadata.len() {
            0 => {
                let layout = Layout {
                    offset: dbcfg.offset.unwrap_or(Offset::LFS),
                    alignment: dbcfg.alignment.unwrap_or(Alignment::Align64),
                    endian: dbcfg.endian.unwrap_or(Endian::Little),
                };
                let (block_size, dir_bits) = build_dir_size(
                    layout.offset,
                    match dbcfg.block_size {
                        Some(size) if size >= 512 => size,
                        _ => f.metadata()?.st_blksize() as u32,
                    },
                );

                if dbcfg.bsexact && Some(block_size) != dbcfg.block_size {
                    Err(Error::new(ErrorKind::Other, "no exact blocksize"))?;
                }

                let header = Header::new(block_size, &layout, dir_bits, dbcfg.numsync);
                let bucket = Bucket::new(0, header.bucket_elems as usize, vec![], vec![]);
                let bucket_offset = header.next_block - block_size as u64;
                let dir = Directory::new(vec![bucket_offset; 1 << header.dir_bits]);

                (header, dir, Some((bucket_offset, bucket)))
            }
            _ => {
                let header = Header::from_reader(&dbcfg.alignment, metadata.len(), &mut f)?;
                f.seek(SeekFrom::Start(header.dir_ofs))?;
                let dir = Directory::from_reader(&header.layout, header.dir_sz, &mut f)?;

                // ensure all bucket offsets are reasonable
                if !dir.validate(header.block_sz as u64, header.next_block, header.block_sz) {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "corruption: bucket offset outside of file",
                    ))?;
                }

                (header, dir, None)
            }
        };

        let bucket_cache = {
            let cache_buckets = {
                let bytes = dbcfg.cachesize.unwrap_or(DEFAULT_CACHESIZE);
                let buckets = bytes / header.bucket_sz as usize;
                buckets.max(1)
            };
            BucketCache::new(cache_buckets, initial_bucket)
        };

        Ok(Gdbm {
            pathname: pathname.to_string(),
            cfg: *dbcfg,
            f,
            header,
            dir,
            bucket_cache,
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

    fn export_ascii_datum(outf: &mut std::fs::File, bindata: &[u8]) -> io::Result<()> {
        const MAX_DUMP_LINE_LEN: usize = 76;

        writeln!(outf, "#:len={}", bindata.len())?;

        let mut b64 = base64::prelude::BASE64_STANDARD.encode(bindata);

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
        self.iter().try_fold(0, |count, kv| {
            kv.and_then(|(key, value)| {
                Self::export_ascii_datum(outf, &key)
                    .and_then(|_| Self::export_ascii_datum(outf, &value))
                    .map(|_| count + 1)
            })
        })
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

    pub fn import_ascii(&mut self, reader: &mut impl Read) -> io::Result<()> {
        ASCIIImportIterator::new(reader)?.try_for_each(|l| {
            let (key, value) = l?;
            self.insert(key, value).map(|_| ())
        })
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
        self.iter().try_for_each(|kv| {
            kv.and_then(|(key, value)| {
                Self::export_bin_datum(outf, alignment, &key)
                    .and_then(|_| Self::export_bin_datum(outf, alignment, &value))
            })
        })
    }

    // API: export database to binary dump file
    pub fn export_bin(&mut self, outf: &mut std::fs::File, mode: ExportBinMode) -> io::Result<()> {
        let alignment = match mode {
            ExportBinMode::ExpNative => self.header.layout.alignment,
            ExportBinMode::Exp32 => Alignment::Align32,
            ExportBinMode::Exp64 => Alignment::Align64,
        };

        self.export_bin_header(outf)?;
        self.export_bin_records(outf, alignment)?;
        Ok(())
    }

    pub fn import_bin(&mut self, reader: &mut impl Read, mode: ExportBinMode) -> io::Result<()> {
        let alignment = match mode {
            ExportBinMode::ExpNative => self.header.layout.alignment,
            ExportBinMode::Exp32 => Alignment::Align32,
            ExportBinMode::Exp64 => Alignment::Align64,
        };

        BinaryImportIterator::new(alignment, reader)?.try_for_each(|l| {
            let (key, value) = l?;
            self.insert(key, value).map(|_| ())
        })
    }

    // read bucket into bucket cache.
    fn cache_load_bucket(&mut self, bucket_dir: usize) -> io::Result<&Bucket> {
        let offset = self.dir.dir[bucket_dir];

        if !self.bucket_cache.contains(offset) {
            self.f.seek(SeekFrom::Start(offset))?;
            let bucket = Bucket::from_reader(&self.header, &self.header.layout, &mut self.f)?;
            if let Some((evicted_offset, evicted_bucket)) = self.bucket_cache.insert(offset, bucket)
            {
                self.f.seek(SeekFrom::Start(evicted_offset))?;
                evicted_bucket.serialize(&self.header.layout, &mut self.f)?;
            }
        }

        self.bucket_cache.set_current(offset);

        Ok(self.bucket_cache.current_bucket().unwrap())
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
            len += self.cache_load_bucket(cur_dir)?.count as usize;
            cur_dir = self.next_bucket_dir(cur_dir);
        }

        Ok(len)
    }

    // API: get an iterator over values
    pub fn values(&mut self) -> impl std::iter::Iterator<Item = io::Result<Vec<u8>>> + '_ {
        GDBMIterator::new(self, KeyOrValue::Value).map(|data| data.map(|(_, value)| value))
    }

    // API: get an iterator over keys
    pub fn keys(&mut self) -> impl std::iter::Iterator<Item = io::Result<Vec<u8>>> + '_ {
        GDBMIterator::new(self, KeyOrValue::Key).map(|data| data.map(|(key, _)| key))
    }

    // API: get an iterator
    pub fn iter(&mut self) -> impl std::iter::Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + '_ {
        GDBMIterator::new(self, KeyOrValue::Both)
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

        let bucket = self.cache_load_bucket(bucket_dir)?;

        let bucket_entries = (0..bucket.tab.len())
            .map(|index| ((index + elem_ofs as usize) % bucket.tab.len()))
            .map(|offset| (offset, bucket.tab[offset]))
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
            let offset = self.allocate_record(block.extent(&self.header.layout))?;
            self.f.seek(SeekFrom::Start(offset))?;
            block.serialize(&self.header.layout, &mut self.f)?;
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
            AvailBlock::from_reader(&self.header.layout, &mut self.f)?
        };

        if let Some(block) = self.header.avail.merge(&next) {
            self.header.avail = block;
            self.header.dirty = true;

            // free the block we just merged
            self.free_record(next_addr, AvailBlock::sizeof(&self.header.layout, next.sz))?;
        }

        Ok(())
    }

    // Add (addr,sz) to db-wide free list
    fn free_record(&mut self, addr: u64, sz: u32) -> io::Result<()> {
        // simply forget elements too small to worry about
        if (sz as usize) <= IGNORE_SMALL {
            return Ok(());
        }

        // smaller items go into bucket avail list
        let bucket = self.bucket_cache.current_bucket().unwrap();
        if sz < self.header.block_sz && (bucket.avail.len() as u32) < Bucket::AVAIL {
            self.bucket_cache
                .current_bucket_mut()
                .unwrap()
                .free(addr, sz);
        } else {
            // larger items go into the header avail list
            // (and also when bucket avail list is full)
            if self.header.avail.elems.len() == self.header.avail.sz as usize {
                self.push_avail_block()?;
            }

            self.header.free(addr, sz);
        }

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_buckets(&mut self) -> io::Result<()> {
        self.bucket_cache
            .dirty_list()
            .iter()
            .try_for_each(|(offset, bucket)| {
                self.f
                    .seek(SeekFrom::Start(*offset))
                    .and_then(|_| bucket.serialize(&self.header.layout, &mut self.f))
            })
            .map(|_| self.bucket_cache.clear_dirty())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_dir(&mut self) -> io::Result<()> {
        if !self.dir.dirty {
            return Ok(());
        }

        self.f.seek(SeekFrom::Start(self.header.dir_ofs))?;
        self.dir.serialize(&self.header.layout, &mut self.f)?;

        self.dir.dirty = false;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_header(&mut self) -> io::Result<()> {
        if !self.header.dirty {
            return Ok(());
        }

        self.f.seek(SeekFrom::Start(0))?;
        self.header.serialize(&self.header.layout, &mut self.f)?;

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

        self.header.increment_numsync();
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

        let elem = self
            .bucket_cache
            .current_bucket_mut()
            .unwrap()
            .remove(elem_ofs);

        // release record bytes to available-space pool
        self.free_record(elem.data_ofs, elem.key_size + elem.data_size)?;

        // flush any dirty pages to OS
        self.write_dirty()?;

        Ok(Some(data))
    }

    fn allocate_record(&mut self, size: u32) -> io::Result<u64> {
        let (offset, length) = match self
            .bucket_cache
            .current_bucket_mut()
            .unwrap()
            .allocate(size)
        {
            Some(block) => block,
            None => {
                if self.header.avail.elems.len() as u32 > self.header.avail.sz / 2 {
                    self.pop_avail_block()?;
                }

                match self.header.allocate(size) {
                    Some(block) => block,
                    None => self.extend(size)?,
                }
            }
        };

        self.free_record(offset + size as u64, length - size)?;

        Ok(offset)
    }

    fn int_insert(&mut self, key: Vec<u8>, data: Vec<u8>) -> io::Result<()> {
        let offset = self.allocate_record((key.len() + data.len()) as u32)?;
        self.f.seek(SeekFrom::Start(offset))?;
        self.f.write_all(&key)?;
        self.f.write_all(&data)?;

        let bucket_elem = BucketElement::new(&key, &data, offset);
        self.cache_load_bucket(bucket_dir(self.header.dir_bits, bucket_elem.hash))?;

        while self.bucket_cache.current_bucket().unwrap().count == self.header.bucket_elems {
            self.split_bucket()?;
            self.cache_load_bucket(bucket_dir(self.header.dir_bits, bucket_elem.hash))?;
        }

        self.bucket_cache
            .current_bucket_mut()
            .unwrap()
            .insert(bucket_elem);

        Ok(())
    }

    pub fn insert(&mut self, key: Vec<u8>, data: Vec<u8>) -> io::Result<Option<Vec<u8>>> {
        self.writeable()
            .and_then(|_| self.remove(&key))
            .and_then(|oldkey| self.int_insert(key, data).map(|_| oldkey))
    }

    pub fn try_insert(
        &mut self,
        key: Vec<u8>,
        data: Vec<u8>,
    ) -> io::Result<(bool, Option<Vec<u8>>)> {
        self.writeable()
            .and_then(|_| self.get(&key))
            .and_then(|olddata| match olddata {
                Some(_) => Ok((false, olddata)),
                _ => self.int_insert(key, data).map(|_| (true, None)),
            })
    }

    fn split_bucket(&mut self) -> io::Result<()> {
        if self.bucket_cache.current_bucket().unwrap().bits == self.header.dir_bits {
            self.extend_directory()?;
        }

        // allocate space for new bucket in an aligned block at the end of file
        let new_bucket_offset = {
            let (offset, size) = self.extend(self.header.bucket_sz)?;
            self.free_record(
                offset + self.header.bucket_sz as u64,
                size - self.header.bucket_sz,
            )?;
            offset
        };

        let bucket = self.bucket_cache.current_bucket().unwrap();
        let cur_bucket_offset = self.bucket_cache.current_bucket_offset().unwrap();
        let (bucket0, bucket1) = bucket.split();
        let bits = bucket0.bits;

        let _ = self.bucket_cache.insert(cur_bucket_offset, bucket0);
        if let Some((evicted_offset, evicted_bucket)) =
            self.bucket_cache.insert(new_bucket_offset, bucket1)
        {
            self.f.seek(SeekFrom::Start(evicted_offset))?;
            evicted_bucket.serialize(&self.header.layout, &mut self.f)?;
        }

        self.dir.update_bucket_split(
            self.header.dir_bits,
            bits,
            cur_bucket_offset,
            new_bucket_offset,
        );

        Ok(())
    }

    // Convenience function to convert readonly flag into an error if we want to write
    fn writeable(&self) -> io::Result<()> {
        (!self.cfg.readonly)
            .then_some(())
            .ok_or_else(|| Error::new(ErrorKind::Other, "write to readonly db"))
    }

    // Extends the directory by duplicating each bucket offset.
    // Old storage is freed and new storage is allocated.
    // The maximum number of hash_bits represented by each element is increased by 1.
    // The header is updated with new offset, size and bits.
    // Both the directory and header are marked dirty, but not written.
    fn extend_directory(&mut self) -> io::Result<()> {
        let directory = self.dir.extend();
        let size = directory.extent(&self.header.layout);
        let offset = self.allocate_record(size)?;

        self.free_record(self.header.dir_ofs, self.header.dir_sz)?;
        self.header.dir_bits += 1;
        self.header.dir_ofs = offset;
        self.header.dir_sz = size;
        self.header.dirty = true;

        self.dir = directory;

        Ok(())
    }

    // API: convert
    pub fn convert(&mut self, options: &ConvertOptions) -> io::Result<()> {
        self.writeable()
            .and_then(|_| self.header.convert_numsync(options.numsync))?
            .into_iter()
            .try_for_each(|(offset, length)| self.free_record(offset, length))
    }
}

struct GDBMIterator<'a> {
    key_or_value: KeyOrValue,
    db: &'a mut Gdbm,
    slot: Option<io::Result<Slot>>,
}

enum KeyOrValue {
    Key,
    Value,
    Both,
}

#[derive(Debug)]
struct Slot {
    bucket: usize,
    element: usize,
}

impl<'a> GDBMIterator<'a> {
    fn next_slot(db: &Gdbm, slot: Slot) -> Option<Slot> {
        match slot {
            Slot { bucket, element } if element + 1 < db.header.bucket_elems as usize => {
                Some(Slot {
                    bucket,
                    element: element + 1,
                })
            }
            Slot { bucket, .. } => {
                let current_bucket_offset = db.dir.dir[bucket];
                (db.dir.dir)
                    .iter()
                    .enumerate()
                    .skip(bucket + 1)
                    .find(|(_, &offset)| offset != current_bucket_offset)
                    .map(|(bucket, _)| Slot { bucket, element: 0 })
            }
        }
    }

    fn next_occupied_slot(db: &mut Gdbm, slot: Slot) -> Option<io::Result<Slot>> {
        let mut next_slot = Self::next_slot(db, slot);
        while let Some(slot) = next_slot {
            let is_occupied = db
                .cache_load_bucket(slot.bucket)
                .map(|bucket| bucket.tab.get(slot.element).unwrap().is_occupied());
            match is_occupied {
                Ok(false) => (),
                Ok(true) => return Some(Ok(slot)),
                Err(e) => return Some(Err(e)),
            }
            next_slot = Self::next_slot(db, slot);
        }

        None
    }

    fn new(db: &'a mut Gdbm, key_or_value: KeyOrValue) -> GDBMIterator<'a> {
        let slot = {
            let slot = Slot {
                bucket: 0,
                element: 0,
            };
            match db.cache_load_bucket(0) {
                Ok(bucket) => {
                    if bucket.tab.first().unwrap().is_occupied() {
                        Some(Ok(slot))
                    } else {
                        Self::next_occupied_slot(db, slot)
                    }
                }
                Err(e) => Some(Err(e)),
            }
        };
        Self {
            db,
            slot,
            key_or_value,
        }
    }
}

impl<'a> Iterator for GDBMIterator<'a> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.slot.take();
        match slot {
            None => None,
            Some(Err(e)) => Some(Err(e)),
            Some(Ok(slot)) => {
                let data = self
                    .db
                    .cache_load_bucket(slot.bucket)
                    .map(|bucket| {
                        bucket
                            .tab
                            .get(slot.element)
                            .map(|e| (e.data_ofs, e.key_size as usize, e.data_size as usize))
                            .unwrap()
                    })
                    .and_then(
                        |(offset, key_length, data_length)| match self.key_or_value {
                            KeyOrValue::Key => read_ofs(&mut self.db.f, offset, key_length)
                                .map(|data| (data.to_vec(), vec![])),
                            KeyOrValue::Value => {
                                read_ofs(&mut self.db.f, offset + key_length as u64, data_length)
                                    .map(|data| (vec![], data.to_vec()))
                            }
                            KeyOrValue::Both => {
                                read_ofs(&mut self.db.f, offset, key_length + data_length).map(
                                    |data| {
                                        let (key, value) = data.split_at(key_length);
                                        (key.to_vec(), value.to_vec())
                                    },
                                )
                            }
                        },
                    );

                match data {
                    Ok(data) => {
                        self.slot = Self::next_occupied_slot(self.db, slot);
                        Some(Ok(data))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        }
    }
}
