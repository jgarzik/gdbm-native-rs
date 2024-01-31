extern crate base64;

use std::{
    fs::OpenOptions,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write},
};

mod avail;
mod bucket;
mod dir;
mod hashutil;
mod header;
mod ser;
use avail::{AvailBlock, AvailElem};
use bucket::{Bucket, BucketCache, BUCKET_AVAIL};
use dir::{dir_reader, dirent_elem_size, Directory};
use hashutil::{key_loc, partial_key_match};
use header::Header;

// todo: convert to enum w/ value
const GDBM_OMAGIC: u32 = 0x13579ace; /* Original magic number. */
const GDBM_MAGIC32: u32 = 0x13579acd; /* New 32bit magic number. */
const GDBM_MAGIC64: u32 = 0x13579acf; /* New 64bit magic number. */

const GDBM_OMAGIC_SWAP: u32 = 0xce9a5713; /* OMAGIC swapped. */
const GDBM_MAGIC32_SWAP: u32 = 0xcd9a5713; /* MAGIC32 swapped. */
const GDBM_MAGIC64_SWAP: u32 = 0xcf9a5713; /* MAGIC64 swapped. */

// Our claimed GDBM lib version compatibility.  Appears in dump files.
const COMPAT_GDBM_VERSION: &'static str = "1.23";

const GDBM_HASH_BITS: u32 = 31;

const GDBM_HDR_SZ: u32 = 72; // todo: all this varies on 32/64bit, LE/BE...
const GDBM_HASH_BUCKET_SZ: u32 = 136;
const GDBM_BUCKET_ELEM_SZ: u32 = 24;
const GDBM_AVAIL_HDR_SZ: u32 = 16;
const GDBM_AVAIL_ELEM_SZ: u32 = 16;
const KEY_SMALL: usize = 4;
const IGNORE_SMALL: usize = 4;
const DEF_IS_LE: bool = true;

pub enum ExportBinMode {
    ExpNative,
    Exp32,
    Exp64,
}

// read and return file data stored at (ofs,total_size)
// todo:  use Read+Seek traits rather than File
fn read_ofs(f: &mut std::fs::File, ofs: u64, total_size: usize) -> io::Result<Vec<u8>> {
    let mut data: Vec<u8> = Vec::with_capacity(total_size);
    data.resize(total_size, 0);

    f.seek(SeekFrom::Start(ofs))?;
    f.read(&mut data)?;

    Ok(data)
}

// write data to storage at (ofs,total_size)
// todo:  use Write+Seek traits rather than File
fn write_ofs(f: &mut std::fs::File, ofs: u64, data: &[u8]) -> io::Result<()> {
    f.seek(SeekFrom::Start(ofs))?;
    f.write_all(data)?;

    Ok(())
}

#[derive(Debug)]
pub struct Gdbm {
    pathname: String,
    f: std::fs::File,
    header: Header,
    dir: Directory,
    dir_dirty: bool,
    bucket_cache: BucketCache,
    cur_bucket_ofs: u64,
    cur_bucket_dir: usize,
}

impl Gdbm {
    // API: open database file, read and validate header
    pub fn open(pathname: &str) -> io::Result<Gdbm> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(pathname)?;
        let metadata = f.metadata()?;

        let header = Header::from_reader(&metadata, f.try_clone()?)?;
        println!("{:?}", header);

        let dir = dir_reader(&mut f, &header)?;
        let cur_bucket_dir: usize = 0;
        let cur_bucket_ofs = dir[cur_bucket_dir];

        Ok(Gdbm {
            pathname: pathname.to_string(),
            f,
            header,
            dir: Directory { dir },
            dir_dirty: false,
            bucket_cache: BucketCache::new(),
            cur_bucket_ofs,
            cur_bucket_dir,
        })
    }

    fn export_ascii_header(&self, outf: &mut std::fs::File) -> io::Result<()> {
        // TODO: add ctime() to "created by" output line
        write!(
            outf,
            "# GDBM dump file created by {}\n",
            COMPAT_GDBM_VERSION
        )?;
        write!(outf, "#:version=1.1\n")?;
        write!(outf, "#:file={}\n", self.pathname)?;
        write!(outf, "#:format={}\n", "standard")?;
        write!(outf, "# End of header\n")?;
        Ok(())
    }

    fn export_ascii_datum(&self, outf: &mut std::fs::File, bindata: &[u8]) -> io::Result<()> {
        const MAX_DUMP_LINE_LEN: usize = 76;

        write!(outf, "#:len={}\n", bindata.len())?;

        let mut b64 = base64::encode(bindata);

        while b64.len() > MAX_DUMP_LINE_LEN {
            let line = &b64[..MAX_DUMP_LINE_LEN];
            let rem = &b64[MAX_DUMP_LINE_LEN..];

            write!(outf, "{}\n", line)?;

            b64 = rem.to_string();
        }
        write!(outf, "{}\n", b64)?;

        Ok(())
    }

    fn export_ascii_records(&mut self, outf: &mut std::fs::File) -> io::Result<usize> {
        let mut n_written: usize = 0;
        let mut key_res = self.first_key()?;
        while key_res != None {
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
        write!(outf, "#:count={}\n", n_written)?;
        write!(outf, "# End of data\n")?;
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
        is_64: bool,
        bindata: &[u8],
    ) -> io::Result<()> {
        // write metadata:  big-endian datum size, 32b or 64b
        let size_bytes = ser::woff_t(is_64, false, bindata.len() as u64);
        outf.write_all(&size_bytes)?;

        // write datum
        outf.write_all(bindata)?;

        Ok(())
    }

    fn export_bin_records(&mut self, outf: &mut std::fs::File, is_64: bool) -> io::Result<()> {
        let mut key_res = self.first_key()?;
        while key_res != None {
            let key = key_res.unwrap();
            let val_res = self.get(&key)?;
            let val = val_res.unwrap();

            self.export_bin_datum(outf, is_64, &key)?;
            self.export_bin_datum(outf, is_64, &val)?;

            key_res = self.next_key(&key)?;
        }
        Ok(())
    }

    // API: export database to binary dump file
    pub fn export_bin(&mut self, outf: &mut std::fs::File, mode: ExportBinMode) -> io::Result<()> {
        let is_64;
        match mode {
            ExportBinMode::ExpNative => is_64 = self.header.is_64,
            ExportBinMode::Exp32 => is_64 = false,
            ExportBinMode::Exp64 => is_64 = true,
        }

        self.export_bin_header(outf)?;
        self.export_bin_records(outf, is_64)?;
        Ok(())
    }

    // validate directory entry index.  currently just a bounds check.
    fn dirent_valid(&self, idx: usize) -> bool {
        idx < self.dir.len() // && self.dir.dir[idx] >= (self.header.block_sz as u64)
    }

    // read bucket into bucket cache.
    fn cache_load_bucket(&mut self, bucket_dir: usize) -> io::Result<bool> {
        if !self.dirent_valid(bucket_dir) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket idx"));
        }

        let bucket_ofs = self.dir.dir[bucket_dir];
        println!("bucket ofs = {}", bucket_ofs);

        // already in cache
        if self.bucket_cache.contains(bucket_ofs) {
            self.cur_bucket_ofs = bucket_ofs;
            self.cur_bucket_dir = bucket_dir;
            return Ok(true);
        }

        // empty bucket
        if bucket_ofs < (self.header.block_sz as u64) {
            self.cur_bucket_ofs = bucket_ofs;
            self.cur_bucket_dir = bucket_dir;
            return Ok(false);
        }

        // seek to bucket and read it
        let pos = self.f.seek(SeekFrom::Start(bucket_ofs))?;
        println!("seek'd to {}", pos);

        let new_bucket = Bucket::from_reader(&self.header, &mut self.f)?;
        // println!("new_bucket={:?}", new_bucket);

        // add to bucket cache
        self.bucket_cache.insert(bucket_ofs, new_bucket);
        self.cur_bucket_ofs = bucket_ofs;
        self.cur_bucket_dir = bucket_dir;

        Ok(true)
    }

    // return a clone of the current bucket
    fn get_current_bucket(&self) -> Bucket {
        // note: assumes will be called following cache_load_bucket() to cache
        // assignment of dir[0] to cur_bucket at Gdbm{} creation not sufficient.
        self.bucket_cache.bucket_map[&self.cur_bucket_ofs].clone()
    }

    fn dir_max_elem(&self) -> usize {
        self.header.dir_sz as usize / dirent_elem_size(self.header.is_64)
    }

    // since one bucket dir entry may duplicate another,
    // this function returns the next non-dup bucket dir
    fn next_bucket_dir(&self, bucket_dir_in: usize) -> usize {
        let dir_max_elem = self.dir_max_elem();
        if bucket_dir_in >= dir_max_elem {
            return dir_max_elem;
        }

        let mut bucket_dir = bucket_dir_in;

        let cur_ofs = self.dir.dir[bucket_dir];
        while bucket_dir < dir_max_elem && cur_ofs == self.dir.dir[bucket_dir] {
            bucket_dir = bucket_dir + 1;
        }

        bucket_dir
    }

    // API: count entries in database
    pub fn len(&mut self) -> io::Result<usize> {
        let mut len: usize = 0;
        let mut cur_dir: usize = 0;
        let dir_max_elem = self.dir_max_elem();
        while cur_dir < dir_max_elem {
            self.cache_load_bucket(cur_dir)?;
            let bucket = self.get_current_bucket();
            len = len + (bucket.count as usize);

            cur_dir = self.next_bucket_dir(cur_dir);
        }

        Ok(len)
    }

    // given a bucket ptr, return the next key
    fn int_next_key(&mut self, elem_opt: Option<usize>) -> io::Result<Option<Vec<u8>>> {
        let mut init_elem_ofs = false;
        let mut elem_ofs: usize = 0;
        let mut bucket = self.get_current_bucket();
        let mut found = false;
        while !found {
            // setup our bucket ptr, if first time into loop
            if !init_elem_ofs {
                init_elem_ofs = true;
                match elem_opt {
                    None => {
                        elem_ofs = 0;
                    }
                    Some(v) => {
                        elem_ofs = v + 1;
                    }
                }
            } else {
                elem_ofs = elem_ofs + 1;
            }

            // finished current bucket. get next bucket.
            if elem_ofs == self.header.bucket_elems as usize {
                elem_ofs = 0;

                // find next bucket.  many dir entries may duplicate
                // the current bucket, so skip dups.
                let cur_dir = self.next_bucket_dir(self.cur_bucket_dir);
                if cur_dir == self.dir.len() {
                    return Ok(None); // reached end of bucket dir - no more keys
                }

                // load new bucket
                self.cache_load_bucket(cur_dir)?;
                bucket = self.get_current_bucket();
            }

            // any valid hash will do
            found = bucket.tab[elem_ofs].hash != 0xffffffff
        }

        // read and return first half of key+value pair
        let elem = &bucket.tab[elem_ofs];
        let data = read_ofs(&mut self.f, elem.data_ofs, elem.key_size as usize)?;

        Ok(Some(data))
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
        if get_opt == None {
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
        let (key_hash, bucket_dir, elem_ofs_32) = key_loc(&self.header, key);
        let mut elem_ofs = elem_ofs_32 as usize;

        let cached = self.cache_load_bucket(bucket_dir)?;
        if !cached {
            return Ok(None);
        } // bucket not found -> key not found

        let bucket = self.get_current_bucket();

        // loop through bucket, starting at elem_ofs position
        let home_ofs = elem_ofs;
        let mut bucket_hash = bucket.tab[elem_ofs].hash;
        while bucket_hash != 0xffffffff {
            let elem = &bucket.tab[elem_ofs];
            // println!("elem={:?}", elem);

            // if quick-match made, ...
            if bucket_hash == key_hash
                && key.len() == elem.key_size as usize
                && partial_key_match(key, &elem.key_start)
            {
                // read full entry to verify full match
                let data = read_ofs(
                    &mut self.f,
                    elem.data_ofs,
                    (elem.key_size + elem.data_size) as usize,
                )?;
                if &data[0..key.len()] == key {
                    return Ok(Some((elem_ofs, (&data[key.len()..]).to_vec())));
                }
            }

            // next bucket slot (possibly warping back to beginning)
            elem_ofs = (elem_ofs + 1) % (self.header.bucket_elems as usize);
            if elem_ofs == home_ofs {
                break;
            }

            bucket_hash = bucket.tab[elem_ofs].hash;
        }

        Ok(None)
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
    fn new_block(&mut self, new_sz: usize) -> AvailElem {
        let mut elem = AvailElem {
            sz: self.header.block_sz,
            addr: self.header.next_block,
        };

        while (elem.sz as usize) < new_sz {
            elem.sz += self.header.block_sz;
        }

        self.header.next_block += elem.sz as u64;

        self.header.dirty = true;

        elem
    }

    // Free list is full.  Split in half, and store 1/2 in new list block.
    fn push_avail_block(&mut self) -> io::Result<()> {
        let new_blk_sz =
            (((self.header.avail.sz * GDBM_AVAIL_ELEM_SZ) / 2) + GDBM_AVAIL_HDR_SZ) as usize;

        // having calculated size of 1st extension block (new_blk_sz),
        // - look in free list for that amount of space
        // - if not, get new space from end of file
        let mut ext_elem = self
            .header
            .avail
            .remove_elem(new_blk_sz)
            .unwrap_or(self.new_block(new_blk_sz));
        let new_blk_ofs = ext_elem.addr;

        let mut hdr_blk = AvailBlock::new(self.header.avail.sz);
        let mut ext_blk = AvailBlock::new(self.header.avail.sz);

        // divide avail into 2 vectors.  elements are sorted by size,
        // so we perform A/B push, rather than simply slice first 1/2 of vec
        let mut index = 0;
        for elem in &self.header.avail.elems {
            if index & 0x1 == 0 {
                hdr_blk.elems.push(elem.clone());
            } else {
                ext_blk.elems.push(elem.clone());
            }
            index += 1;
        }

        // finalize new header avail block.  equates to
        //     head.next = second
        hdr_blk.count = hdr_blk.elems.len() as u32;
        hdr_blk.next_block = new_blk_ofs;

        // finalize new extension avail block, linked-to by header block
        // as with any linked list insertion at-head, our 'next' equates to
        //     second.next = head.next
        ext_blk.count = ext_blk.elems.len() as u32;
        ext_blk.next_block = self.header.avail.next_block;

        // size allocation may have allocated more space than we needed.
        // gdbm calls self.free_record(), which may call
        // self.push_avail_block() recurively.  We choose the alternative,
        // adding the space to the header block as a simplfication.
        ext_elem.sz -= new_blk_sz as u32;
        ext_elem.addr += new_blk_sz as u64;
        if ext_elem.sz > IGNORE_SMALL as u32 {
            // insert, sorted by size
            let pos = hdr_blk.elems.binary_search(&ext_elem).unwrap_or_else(|e| e);
            hdr_blk.elems.insert(pos, ext_elem);
            hdr_blk.count += 1;
        }

        // update avail block in header (deferred write)
        self.header.avail = hdr_blk;
        self.header.dirty = true;

        // write extension block to storage (immediately)
        let ext_bytes = ext_blk.serialize(self.header.is_64, DEF_IS_LE);
        write_ofs(&mut self.f, new_blk_ofs, &ext_bytes)?;

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
        let mut bucket = self.get_current_bucket();
        if sz < self.header.block_sz && bucket.av_count < BUCKET_AVAIL {
            // insort into bucket avail vector, sorted by size
            let pos = bucket.avail.binary_search(&elem).unwrap_or_else(|e| e);
            bucket.avail.insert(pos, elem);
            bucket.av_count += 1;

            // store updated bucket in cache, and mark dirty
            self.bucket_cache.update(self.cur_bucket_ofs, bucket);

            // success (and no I/O performed)
            return Ok(());
        }

        // larger items go into the header avail list
        // (and also when bucket avail list is full)
        if self.header.avail.count == self.header.avail.sz {
            self.push_avail_block()?;
        }
        assert!(self.header.avail.count < self.header.avail.sz);

        // insort into header avail vector, sorted by size
        let pos = self
            .header
            .avail
            .elems
            .binary_search(&elem)
            .unwrap_or_else(|e| e);
        self.header.avail.elems.insert(pos, elem);
        self.header.avail.count += 1;

        // header needs to be written
        self.header.dirty = true;

        Ok(())
    }

    fn write_bucket(&mut self, bucket_ofs: u64, bucket: &Bucket) -> io::Result<()> {
        let bytes = bucket.serialize(self.header.is_64, DEF_IS_LE);
        write_ofs(&mut self.f, bucket_ofs, &bytes)?;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_buckets(&mut self) -> io::Result<()> {
        let dirty_list = self.bucket_cache.dirty_list();

        // write out each dirty bucket
        for bucket_ofs in dirty_list {
            assert_eq!(self.bucket_cache.contains(bucket_ofs), true);
            let bucket = self.bucket_cache.bucket_map[&bucket_ofs].clone();

            self.write_bucket(bucket_ofs, &bucket)?;
        }

        // nothing in cache remains dirty
        self.bucket_cache.clear_dirty();

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_dir(&mut self) -> io::Result<()> {
        if !self.dir_dirty {
            return Ok(());
        }

        let bytes = self.dir.serialize(self.header.is_64, DEF_IS_LE);
        write_ofs(&mut self.f, self.header.dir_ofs, &bytes)?;

        self.dir_dirty = false;

        Ok(())
    }

    // write out any cached, not-yet-written metadata and data to storage
    fn write_header(&mut self) -> io::Result<()> {
        if !self.header.dirty {
            return Ok(());
        }

        let bytes = self.header.serialize(self.header.is_64, DEF_IS_LE);
        write_ofs(&mut self.f, 0, &bytes)?;

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
        // TODO: read-only check

        self.write_dirty()?;
        self.f.sync_data()?;

        Ok(())
    }

    // API: remove a key/value pair from db, given a key
    pub fn remove(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let get_opt = self.int_get(key)?;
        if get_opt == None {
            return Ok(None);
        }
        let (mut elem_ofs, data) = get_opt.unwrap();

        let bucket_elems = self.header.bucket_elems as usize;
        let mut bucket = self.get_current_bucket();

        // remember element to be removed
        let elem = bucket.tab[elem_ofs].clone();

        // remove element from table
        bucket.tab[elem_ofs].hash = 0xffffffff;
        bucket.count -= 1;

        // move other elements to fill gap
        let mut last_ofs = elem_ofs;
        while elem_ofs != last_ofs && bucket.tab[elem_ofs].hash != 0xffffffff {
            let home = (bucket.tab[elem_ofs].hash as usize) % bucket_elems;
            if (last_ofs < elem_ofs && (home <= last_ofs || home > elem_ofs))
                || (last_ofs > elem_ofs && home <= last_ofs && home > elem_ofs)
            {
                bucket.tab[last_ofs] = bucket.tab[elem_ofs].clone();
                bucket.tab[elem_ofs].hash = 0xffffffff;
                last_ofs = elem_ofs;
            }

            elem_ofs = (elem_ofs + 1) % bucket_elems;
        }

        // store updated bucket in cache, and mark dirty
        self.bucket_cache.update(self.cur_bucket_ofs, bucket);

        // release record bytes to available-space pool
        self.free_record(elem.data_ofs, elem.key_size + elem.data_size)?;

        // flush any dirty pages to OS
        self.write_dirty()?;

        Ok(Some(data))
    }

    // API: print bucket directory
    pub fn print_dir(&self) {
        println!(
            "size = {}, bits = {}, buckets = ?",
            self.header.dir_sz, self.header.dir_bits
        );

        for idx in 0..self.dir.len() {
            println!("{}: {}", idx, self.dir.dir[idx]);
        }
    }

    // API: print bucket
    pub fn print_bucket(&mut self, bucket_dir: usize) -> io::Result<()> {
        self.cache_load_bucket(bucket_dir)?;
        let bucket_ofs = self.dir.dir[bucket_dir];
        let bucket = self.bucket_cache.bucket_map[&bucket_ofs].clone();

        println!("bits = {}", bucket.bits);
        println!("count = {}", bucket.count);

        for idx in 0..bucket.tab.len() {
            let elem = &bucket.tab[idx];
            println!(
                "{}   {:x}   {}   {}   {}  {}",
                idx,
                elem.hash,
                elem.key_size,
                elem.data_size,
                elem.data_ofs,
                String::from_utf8_lossy(&elem.key_start)
            );
        }

        println!("avail count = {}", bucket.av_count);
        println!("Address     Size");
        for av_elem in &bucket.avail {
            println!("{}   {}", av_elem.addr, av_elem.sz);
        }

        Ok(())
    }
}

#[cfg(test)]
use std::path::PathBuf;

#[cfg(test)]
mod test_gdbm {
    use super::*;
    use std::collections::HashMap;
    use std::fs;

    struct TestInfo {
        #[allow(dead_code)]
        pub json_path: String,
        pub db_path: String,
        pub is_basic: bool,
        pub n_records: usize,
    }

    struct TestConfig {
        pub tests: Vec<TestInfo>,
    }

    impl TestConfig {
        fn new() -> TestConfig {
            TestConfig { tests: Vec::new() }
        }
    }

    fn push_test(cfg: &mut TestConfig, db_fn: &str, json_fn: &str, is_basic: bool, n_rec: usize) {
        let mut dbp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dbp.push("src/data");
        dbp.push(db_fn);

        let mut jsp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        jsp.push("src/data");
        jsp.push(json_fn);

        cfg.tests.push(TestInfo {
            db_path: dbp.to_str().unwrap().to_string(),
            json_path: jsp.to_str().unwrap().to_string(),
            is_basic,
            n_records: n_rec,
        });
    }

    fn init_tests() -> TestConfig {
        let mut cfg = TestConfig::new();

        // NOTE: Order of push is important.
        // Some tests depend on basic.db being index 1 (2nd item)

        push_test(&mut cfg, "empty.db.le64", "empty.json.le64", false, 0);
        push_test(&mut cfg, "basic.db.le64", "basic.json.le64", true, 10001);

        cfg
    }

    #[test]
    fn api_open_close() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            let _res = Gdbm::open(&testdb.db_path).unwrap();
            // implicit close when scope closes
        }
    }

    #[test]
    fn api_exists_not() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            let mut db = Gdbm::open(&testdb.db_path).unwrap();
            let res = db.contains_key(b"dummy").unwrap();
            assert_eq!(res, false);

            if testdb.is_basic {
                db = Gdbm::open(&testdb.db_path).unwrap();
                let res = db.contains_key(b"key -111").unwrap();
                assert_eq!(res, false);
            }
        }
    }

    #[test]
    fn api_exists() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            if testdb.is_basic {
                let mut db = Gdbm::open(&testdb.db_path).unwrap();

                for n in 0..10001 {
                    let keystr = format!("key {}", n);
                    let res = db.contains_key(keystr.as_bytes()).unwrap();
                    assert_eq!(res, true);
                }
            }
        }
    }

    #[test]
    fn api_first_next_key() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            if testdb.is_basic {
                // build internal map of keys expected to be present in basic.db
                let mut keys_remaining: HashMap<Vec<u8>, bool> = HashMap::new();
                for n in 0..10001 {
                    let keystr = format!("key {}", n);
                    keys_remaining.insert(keystr.as_bytes().to_vec(), true);
                }

                // simple verf of correct map construction
                assert_eq!(keys_remaining.len(), testdb.n_records);

                // open basic.db
                let mut db = Gdbm::open(&testdb.db_path).unwrap();

                // iterate through each key in db
                let mut key_res = db.first_key().unwrap();
                while key_res != None {
                    let key = key_res.unwrap();

                    // remove iteration key from internal map
                    assert_ne!(keys_remaining.remove(&key), None);

                    key_res = db.next_key(&key).unwrap();
                }

                // if internal map is empty, success
                assert_eq!(keys_remaining.len(), 0);
            }
        }
    }

    #[test]
    fn api_len() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            let mut db = Gdbm::open(&testdb.db_path).unwrap();
            let res = db.len().unwrap();
            assert_eq!(res, testdb.n_records);
        }
    }

    #[test]
    fn api_export_bin() {
        const EXPORT_FN: &'static str = "./export.bin";

        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            let mut db = Gdbm::open(&testdb.db_path).unwrap();
            let mut outf = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(EXPORT_FN)
                .unwrap();

            let _iores = db.export_bin(&mut outf, ExportBinMode::ExpNative).unwrap();
            fs::remove_file(EXPORT_FN).unwrap();

            // TODO: once Store is implemented, import the exported data
            // into a new db, and verify that old & new dbs match.
        }
    }

    #[test]
    fn api_export_ascii() {
        const EXPORT_FN: &'static str = "./export.txt";

        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            let mut db = Gdbm::open(&testdb.db_path).unwrap();
            let mut outf = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(EXPORT_FN)
                .unwrap();

            let _iores = db.export_ascii(&mut outf).unwrap();
            fs::remove_file(EXPORT_FN).unwrap();

            // TODO: once Store is implemented, import the exported data
            // into a new db, and verify that old & new dbs match.
        }
    }

    #[test]
    fn api_get_not() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            let keystr = String::from("This key does not exist.");
            let mut db = Gdbm::open(&testdb.db_path).unwrap();
            let res = db.get(keystr.as_bytes()).unwrap();
            assert_eq!(res, None);
        }
    }

    #[test]
    fn api_get() {
        let testcfg = init_tests();

        for testdb in &testcfg.tests {
            if testdb.is_basic {
                let mut db = Gdbm::open(&testdb.db_path).unwrap();

                for n in 0..10001 {
                    let keystr = format!("key {}", n);
                    let res = db.get(keystr.as_bytes());
                    match res {
                        Ok(opt) => match opt {
                            None => {
                                assert!(false);
                            }
                            Some(val) => {
                                let valstr = format!("value {}", n);
                                assert_eq!(val, valstr.as_bytes());
                            }
                        },
                        Err(_e) => {
                            assert!(false);
                        }
                    }
                }
            }
        }
    }
}
