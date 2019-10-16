use byteorder::{LittleEndian, ReadBytesExt}; // 1.2.7
use std::collections::HashMap;
use std::{
    fs::File,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom},
};

// todo: convert to enum w/ value
const GDBM_OMAGIC: u32 = 0x13579ace; /* Original magic number. */
const GDBM_MAGIC32: u32 = 0x13579acd; /* New 32bit magic number. */
const GDBM_MAGIC64: u32 = 0x13579acf; /* New 64bit magic number. */

const GDBM_OMAGIC_SWAP: u32 = 0xce9a5713; /* OMAGIC swapped. */
const GDBM_MAGIC32_SWAP: u32 = 0xcd9a5713; /* MAGIC32 swapped. */
const GDBM_MAGIC64_SWAP: u32 = 0xcf9a5713; /* MAGIC64 swapped. */

const GDBM_HASH_BITS: u32 = 31;

const GDBM_HDR_SZ: u32 = 72; // todo: all this varies on 32/64bit...
const GDBM_HASH_BUCKET_SZ: u32 = 136;
const GDBM_BUCKET_ELEM_SZ: u32 = 24;
const GDBM_AVAIL_ELEM_SZ: u32 = 16;
const BUCKET_AVAIL: u32 = 6;
const KEY_SMALL: usize = 4;
const IGNORE_SMALL: usize = 4;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct AvailElem {
    sz: u32,
    addr: u64,
}

impl AvailElem {
    fn from_reader(is_64: bool, rdr: &mut impl Read) -> io::Result<Self> {
        let elem_sz = rdr.read_u32::<LittleEndian>()?;
        let elem_ofs: u64;
        if is_64 {
            let _padding = rdr.read_u32::<LittleEndian>()?;
            elem_ofs = rdr.read_u64::<LittleEndian>()?;
        } else {
            elem_ofs = rdr.read_u32::<LittleEndian>()? as u64;
        }

        Ok(AvailElem {
            sz: elem_sz,
            addr: elem_ofs,
        })
    }
}

#[derive(Debug)]
pub struct AvailBlock {
    sz: u32,
    count: u32,
    next_block: u64,
    elems: Vec<AvailElem>,
}

#[derive(Debug)]
pub struct Header {
    // on-disk gdbm database file header
    magic: u32,
    block_sz: u32,
    dir_ofs: u64,
    dir_sz: u32,
    dir_bits: u32,
    bucket_sz: u32,
    bucket_elems: u32,
    next_block: u64,

    avail: AvailBlock,

    // following fields are calculated, not stored
    is_64: bool,
    dirty: bool,
}

#[derive(Debug, Clone)]
pub struct BucketElement {
    hash: u32,
    key_start: [u8; 4],
    data_ofs: u64,
    key_size: u32,
    data_size: u32,
}

#[derive(Debug, Clone)]
pub struct Bucket {
    // on-disk gdbm database hash bucket
    av_count: u32,
    avail: Vec<AvailElem>,
    bits: u32,
    count: u32,
    tab: Vec<BucketElement>,
}

#[derive(Debug)]
pub struct BucketCache {
    bucket_map: HashMap<u64, Bucket>,
    dirty: HashMap<u64, bool>,
}

impl BucketCache {
    pub fn new() -> BucketCache {
        BucketCache {
            bucket_map: HashMap::new(),
            dirty: HashMap::new(),
        }
    }

    pub fn dirty(&mut self, bucket_ofs: u64) {
        self.dirty.insert(bucket_ofs, true);
    }

    pub fn dirty_list(&mut self) -> Vec<u64> {
        let mut dl: Vec<u64> = Vec::new();
        for (ofs, _dummy) in &self.dirty {
            dl.push(*ofs);
        }
        dl.sort();

        dl
    }

    pub fn clear_dirty(&mut self) {
        self.dirty.clear();
    }

    pub fn contains(&self, bucket_ofs: u64) -> bool {
        self.bucket_map.contains_key(&bucket_ofs)
    }

    pub fn insert(&mut self, bucket_ofs: u64, bucket: Bucket) {
        self.bucket_map.insert(bucket_ofs, bucket);
    }

    pub fn update(&mut self, bucket_ofs: u64, bucket: Bucket) {
        self.bucket_map.insert(bucket_ofs, bucket);
        self.dirty(bucket_ofs);
    }
}

#[derive(Debug)]
pub struct Gdbm {
    f: std::fs::File,
    header: Header,
    dir: Vec<u64>,
    bucket_cache: BucketCache,
    cur_bucket_ofs: u64,
    cur_bucket_dir: usize,
}

fn build_dir_size(block_sz: u32) -> (u32, u32) {
    let mut dir_size = 8 * 8; // fixme: 8==off_t==vary on is_64
    let mut dir_bits = 3;

    while dir_size < block_sz && dir_bits < GDBM_HASH_BITS - 3 {
        dir_size = dir_size << 1;
        dir_bits = dir_bits + 1;
    }

    (dir_size, dir_bits)
}

fn bucket_count(bucket_sz: u32) -> u32 {
    (bucket_sz - GDBM_HASH_BUCKET_SZ) / GDBM_BUCKET_ELEM_SZ + 1
}

impl Header {
    fn from_reader(metadata: &std::fs::Metadata, mut rdr: impl Read) -> io::Result<Self> {
        let file_sz = metadata.len();

        let magic = rdr.read_u32::<LittleEndian>()?;

        let is_64 = match magic {
            GDBM_MAGIC64 | GDBM_MAGIC64_SWAP => true,
            _ => false,
        };

        // fixme: read u32, not u64, if is_64

        let block_sz = rdr.read_u32::<LittleEndian>()?;
        let dir_ofs = rdr.read_u64::<LittleEndian>()?;
        let dir_sz = rdr.read_u32::<LittleEndian>()?;
        let dir_bits = rdr.read_u32::<LittleEndian>()?;
        let bucket_sz = rdr.read_u32::<LittleEndian>()?;
        let bucket_elems = rdr.read_u32::<LittleEndian>()?;
        let next_block = rdr.read_u64::<LittleEndian>()?;

        let avail_sz = rdr.read_u32::<LittleEndian>()?;
        let avail_count = rdr.read_u32::<LittleEndian>()?;
        let avail_next_block = rdr.read_u64::<LittleEndian>()?;

        if !(block_sz > 0 && block_sz > GDBM_HDR_SZ && block_sz - GDBM_HDR_SZ >= GDBM_AVAIL_ELEM_SZ)
        {
            return Err(Error::new(ErrorKind::Other, "bad header: blksz"));
        }

        if next_block < file_sz {
            return Err(Error::new(ErrorKind::Other, "needs recovery"));
        }

        if !(dir_ofs > 0 && dir_ofs < file_sz && dir_sz > 0 && dir_ofs + (dir_sz as u64) < file_sz)
        {
            return Err(Error::new(ErrorKind::Other, "bad header: dir"));
        }

        let (ck_dir_sz, _ck_dir_bits) = build_dir_size(block_sz);
        if !(dir_sz >= ck_dir_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: dir sz"));
        }

        let (_ck_dir_sz, ck_dir_bits) = build_dir_size(dir_sz);
        if dir_bits != ck_dir_bits {
            return Err(Error::new(ErrorKind::Other, "bad header: dir bits"));
        }

        if !(bucket_sz > GDBM_HASH_BUCKET_SZ) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket sz"));
        }

        if bucket_elems != bucket_count(bucket_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket elem"));
        }

        if ((block_sz - GDBM_HDR_SZ) / GDBM_AVAIL_ELEM_SZ + 1) != avail_sz {
            return Err(Error::new(ErrorKind::Other, "bad header: avail sz"));
        }

        if !(avail_sz > 1 && avail_count <= avail_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: avail sz/ct"));
        }

        let mut elems: Vec<AvailElem> = Vec::new();
        for _idx in 0..avail_count {
            let av_elem = AvailElem::from_reader(is_64, &mut rdr)?;
            elems.push(av_elem);
        }

        // maintain intrinsic: avail is always sorted by size
        elems.sort();

        // todo: check for overlapping segments

        for elem in elems.iter() {
            if !(elem.addr >= bucket_sz.into() && elem.addr + (elem.sz as u64) <= next_block) {
                return Err(Error::new(ErrorKind::Other, "bad header: avail el"));
            }
        }

        let magname = match magic {
            GDBM_OMAGIC => "GDBM_OMAGIC",
            GDBM_MAGIC32 => "GDBM_MAGIC32",
            GDBM_MAGIC64 => "GDBM_MAGIC64",
            GDBM_OMAGIC_SWAP => "GDBM_OMAGIC_SWAP",
            GDBM_MAGIC32_SWAP => "GDBM_MAGIC32_SWAP",
            GDBM_MAGIC64_SWAP => "GDBM_MAGIC64_SWAP",
            _ => "?",
        };
        println!("magname {}", magname);

        Ok(Header {
            magic,
            block_sz,
            dir_ofs,
            dir_sz,
            dir_bits,
            bucket_sz,
            bucket_elems,
            next_block,
            avail: AvailBlock {
                sz: avail_sz,
                count: avail_count,
                next_block: avail_next_block,
                elems,
            },
            is_64,
            dirty: false,
        })
    }
}

fn dir_reader(f: &mut std::fs::File, header: &Header) -> io::Result<Vec<u64>> {
    let is_64 = header.is_64;
    let dirent_sz = match is_64 {
        true => 8,
        false => 4,
    };
    let dirent_count = header.dir_sz / dirent_sz;

    let mut dir = Vec::new();
    dir.reserve_exact(dirent_count as usize);

    let _pos = f.seek(SeekFrom::Start(header.dir_ofs))?;

    for _idx in 0..dirent_count {
        let ofs: u64;
        if is_64 {
            ofs = f.read_u64::<LittleEndian>()?;
        } else {
            ofs = f.read_u32::<LittleEndian>()? as u64;
        }
        dir.push(ofs);
    }

    Ok(dir)
}

fn hash_key(key: &[u8]) -> u32 {
    let mut value: u32 = 0x238F13AF * (key.len() as u32);
    let mut index: u32 = 0;
    for ch in key.iter() {
        value = (value + ((*ch as u32) << (index * 5 % 24))) & 0x7FFFFFFF;
        index = index + 1;
    }
    value = (value.wrapping_mul(1103515243) + 12345) & 0x7FFFFFFF;

    value
}

fn bucket_dir(header: &Header, hash: u32) -> usize {
    (hash as usize) >> (GDBM_HASH_BITS - header.dir_bits)
}

fn key_loc(header: &Header, key: &[u8]) -> (u32, usize, u32) {
    let hash = hash_key(key);
    let bucket = bucket_dir(header, hash);
    let ofs = hash % header.bucket_elems;

    (hash, bucket, ofs)
}

fn partial_key_match(key_a: &[u8], partial_b: &[u8; KEY_SMALL]) -> bool {
    if key_a.len() <= KEY_SMALL {
        key_a == &partial_b[0..key_a.len()]
    } else {
        &key_a[0..KEY_SMALL] == partial_b
    }
}

fn read_ofs(f: &mut std::fs::File, ofs: u64, total_size: usize) -> io::Result<Vec<u8>> {
    let mut data: Vec<u8> = Vec::with_capacity(total_size);
    data.resize(total_size, 0);

    f.seek(SeekFrom::Start(ofs))?;
    f.read(&mut data)?;

    Ok(data)
}

impl Gdbm {
    pub fn open(pathname: &str) -> io::Result<Gdbm> {
        let mut f = File::open(pathname)?;
        let metadata = f.metadata()?;

        let header = Header::from_reader(&metadata, f.try_clone()?)?;
        println!("{:?}", header);

        let dir = dir_reader(&mut f, &header)?;
        let cur_bucket_dir: usize = 0;
        let cur_bucket_ofs = dir[cur_bucket_dir];

        Ok(Gdbm {
            f,
            header,
            dir,
            bucket_cache: BucketCache::new(),
            cur_bucket_ofs,
            cur_bucket_dir,
        })
    }

    fn dirent_valid(&self, idx: usize) -> bool {
        idx < self.dir.len() // && self.dir[idx] >= (self.header.block_sz as u64)
    }

    fn get_bucket(&mut self, bucket_dir: usize) -> io::Result<bool> {
        if !self.dirent_valid(bucket_dir) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket idx"));
        }

        let bucket_ofs = self.dir[bucket_dir];
        println!("bucket ofs = {}", bucket_ofs);

        // already in cache
        if self.bucket_cache.contains(bucket_ofs) {
            return Ok(true);
        }

        // empty bucket
        if bucket_ofs < (self.header.block_sz as u64) {
            return Ok(false);
        }

        // seek to bucket and read it
        let pos = self.f.seek(SeekFrom::Start(bucket_ofs))?;
        println!("seek'd to {}", pos);

        // read avail section
        let av_count = self.f.read_u32::<LittleEndian>()?;
        let _padding = self.f.read_u32::<LittleEndian>()?;
        let mut avail = Vec::new();
        for _idx in 0..BUCKET_AVAIL {
            let av_elem = AvailElem::from_reader(self.header.is_64, &mut self.f)?;
            avail.push(av_elem);
        }

        // todo: validate avail[]

        // read misc. section
        let bits = self.f.read_u32::<LittleEndian>()?;
        let count = self.f.read_u32::<LittleEndian>()?;

        if !(count <= self.header.bucket_elems && bits <= self.header.dir_bits) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket c/b"));
        }

        // read bucket elements section
        let mut tab = Vec::new();
        for _idx in 0..self.header.bucket_elems {
            let hash = self.f.read_u32::<LittleEndian>()?;
            let mut key_start = [0; KEY_SMALL];
            self.f.read(&mut key_start)?;
            let data_ofs: u64;
            if self.header.is_64 {
                data_ofs = self.f.read_u64::<LittleEndian>()?;
            } else {
                data_ofs = self.f.read_u32::<LittleEndian>()? as u64;
            }
            let key_size = self.f.read_u32::<LittleEndian>()?;
            let data_size = self.f.read_u32::<LittleEndian>()?;
            tab.push(BucketElement {
                hash,
                key_start,
                data_ofs,
                key_size,
                data_size,
            });
        }

        let new_bucket = Bucket {
            av_count,
            avail,
            bits,
            count,
            tab,
        };
        // println!("new_bucket={:?}", new_bucket);

        // add to bucket cache
        self.bucket_cache.insert(bucket_ofs, new_bucket);
        self.cur_bucket_ofs = bucket_ofs;
        self.cur_bucket_dir = bucket_dir;

        Ok(true)
    }

    fn get_current_bucket(&self) -> Bucket {
        // note: assumes will be called following get_bucket() to cache
        // assignment of dir[0] to cur_bucket at Gdbm{} creation not sufficient.
        self.bucket_cache.bucket_map[&self.cur_bucket_ofs].clone()
    }

    fn next_bucket_dir(&self, bucket_dir_in: usize) -> usize {
        let dir_sz = self.header.dir_sz as usize;
        if bucket_dir_in >= dir_sz {
            return dir_sz;
        }

        let mut bucket_dir = bucket_dir_in;

        let cur_ofs = self.dir[bucket_dir];
        while bucket_dir < dir_sz && cur_ofs == self.dir[bucket_dir] {
            bucket_dir = bucket_dir + 1;
        }

        bucket_dir
    }

    pub fn len(&mut self) -> io::Result<usize> {
        let mut len: usize = 0;
        let mut cur_dir: usize = 0;
        while cur_dir < (self.header.dir_sz as usize) {
            self.get_bucket(cur_dir)?;
            let bucket = self.get_current_bucket();
            len = len + (bucket.count as usize);

            cur_dir = self.next_bucket_dir(cur_dir);
        }

        Ok(len)
    }

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
                self.get_bucket(cur_dir)?;
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

    pub fn first_key(&mut self) -> io::Result<Option<Vec<u8>>> {
        // get first bucket
        self.get_bucket(0)?;

        // start iteration - return next key
        self.int_next_key(None)
    }

    pub fn next_key(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let get_opt = self.int_get(key)?;
        if get_opt == None {
            return Ok(None);
        }

        let (elem_ofs, _data) = get_opt.unwrap();

        self.int_next_key(Some(elem_ofs))
    }

    pub fn contains_key(&mut self, key: &[u8]) -> io::Result<bool> {
        let get_opt = self.int_get(key)?;
        match get_opt {
            None => Ok(false),
            Some(_v) => Ok(true),
        }
    }

    fn int_get(&mut self, key: &[u8]) -> io::Result<Option<(usize, Vec<u8>)>> {
        let (key_hash, bucket_dir, elem_ofs_32) = key_loc(&self.header, key);
        let mut elem_ofs = elem_ofs_32 as usize;

        let cached = self.get_bucket(bucket_dir)?;
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

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let get_opt = self.int_get(key)?;
        match get_opt {
            None => Ok(None),
            Some(data) => Ok(Some(data.1)),
        }
    }

    fn push_avail_block(&mut self) -> io::Result<()> {
        Err(Error::new(ErrorKind::Other, "not implemented"))
    }

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

    fn write_dirty(&mut self) -> io::Result<()> {
        Err(Error::new(ErrorKind::Other, "not implemented"))
    }

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

    pub fn print_dir(&self) {
        println!(
            "size = {}, bits = {}, buckets = ?",
            self.header.dir_sz, self.header.dir_bits
        );

        for idx in 0..self.dir.len() {
            println!("{}: {}", idx, self.dir[idx]);
        }
    }

    pub fn print_bucket(&mut self, bucket_dir: usize) -> io::Result<()> {
        self.get_bucket(bucket_dir)?;
        let bucket_ofs = self.dir[bucket_dir];
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
mod tests {
    use super::*;

    #[test]
    fn test_hash() {
        assert_eq!(hash_key(b"hello"), 1730502474);
        assert_eq!(hash_key(b"hello\0"), 72084335);
        assert_eq!(hash_key(b""), 12345);
    }

    #[test]
    fn test_partial_key_match() {
        assert!(partial_key_match(b"123", b"123 "));
        assert!(partial_key_match(b"123456", b"1234"));
    }
}
