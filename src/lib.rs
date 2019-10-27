use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::{
    fs::OpenOptions,
    io::{self, Error, ErrorKind, Read, Seek, SeekFrom, Write},
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
const GDBM_AVAIL_HDR_SZ: u32 = 16;
const GDBM_AVAIL_ELEM_SZ: u32 = 16;
const BUCKET_AVAIL: u32 = 6;
const KEY_SMALL: usize = 4;
const IGNORE_SMALL: usize = 4;
const DEF_IS_LE: bool = true;

// serialize u32, with runtime endian selection
fn w32(is_le: bool, val: u32) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(4);
    buf.resize(4, 0);

    match is_le {
        true => LittleEndian::write_u32(&mut buf, val),
        false => BigEndian::write_u32(&mut buf, val),
    }

    buf
}

// serialize u64, with runtime endian selection
fn w64(is_le: bool, val: u64) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(4);
    buf.resize(4, 0);

    match is_le {
        true => LittleEndian::write_u64(&mut buf, val),
        false => BigEndian::write_u64(&mut buf, val),
    }

    buf
}

fn woff_t(is_64: bool, is_le: bool, val: u64) -> Vec<u8> {
    match is_64 {
        true => w64(is_le, val),
        false => w32(is_le, val as u32),
    }
}

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

    fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.sz));
        if is_64 {
            let padding: u32 = 0;
            buf.append(&mut w32(is_le, padding));
        }
        buf.append(&mut woff_t(is_64, is_le, self.addr));

        buf
    }
}

#[derive(Debug)]
pub struct AvailBlock {
    sz: u32,
    count: u32,
    next_block: u64,
    elems: Vec<AvailElem>,
}

impl AvailBlock {
    fn new(sz: u32) -> AvailBlock {
        AvailBlock {
            sz,
            count: 0,
            next_block: 0,
            elems: Vec::new(),
        }
    }

    fn find_elem(&self, sz: usize) -> Option<usize> {
        for i in 0..self.elems.len() {
            if (self.elems[i].sz as usize) >= sz {
                return Some(i);
            }
        }

        None
    }

    fn remove_elem(&mut self, sz: usize) -> Option<AvailElem> {
        assert!((self.count as usize) == self.elems.len());
        match self.find_elem(sz) {
            None => None,
            Some(idx) => {
                self.count -= 1;
                return Some(self.elems.remove(idx));
            }
        }
    }

    fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.sz));
        buf.append(&mut w32(is_le, self.count));
        buf.append(&mut woff_t(is_64, is_le, self.next_block));

        for elem in &self.elems {
            buf.append(&mut elem.serialize(is_64, is_le));
        }

        buf
    }
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

impl BucketElement {
    fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.hash));
        buf.append(&mut self.key_start.to_vec());
        buf.append(&mut woff_t(is_64, is_le, self.data_ofs));
        buf.append(&mut w32(is_le, self.key_size));
        buf.append(&mut w32(is_le, self.data_size));

        buf
    }
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

impl Bucket {
    fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();

        //
        // avail section
        //

        buf.append(&mut w32(is_le, self.av_count));
        if is_64 {
            let padding: u32 = 0;
            buf.append(&mut w32(is_le, padding));
        }

        assert_eq!(self.avail.len(), BUCKET_AVAIL as usize);
        for avail_elem in &self.avail {
            buf.append(&mut avail_elem.serialize(is_64, is_le));
        }

        //
        // misc section
        //
        buf.append(&mut w32(is_le, self.bits));
        buf.append(&mut w32(is_le, self.count));

        //
        // bucket elements section
        //
        for bucket_elem in &self.tab {
            buf.append(&mut bucket_elem.serialize(is_64, is_le));
        }

        buf
    }
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
pub struct Directory {
    dir: Vec<u64>,
}

impl Directory {
    fn len(&self) -> usize {
        self.dir.len()
    }

    fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();

        for ofs in &self.dir {
            buf.append(&mut woff_t(is_64, is_le, *ofs));
        }

        buf
    }
}

#[derive(Debug)]
pub struct Gdbm {
    f: std::fs::File,
    header: Header,
    dir: Directory,
    dir_dirty: bool,
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

    fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.magic)); // fixme: check swap?
        buf.append(&mut w32(is_le, self.block_sz));
        buf.append(&mut woff_t(is_64, is_le, self.dir_ofs));
        buf.append(&mut w32(is_le, self.dir_sz));
        buf.append(&mut w32(is_le, self.dir_bits));
        buf.append(&mut w32(is_le, self.bucket_sz));
        buf.append(&mut w32(is_le, self.bucket_elems));
        buf.append(&mut woff_t(is_64, is_le, self.next_block));
        buf.append(&mut self.avail.serialize(is_64, is_le));

        buf
    }
}

// Read C-struct-based bucket directory (a vector of storage offsets)
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

// core gdbm hashing function
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

// hash-to-bucket lookup
fn bucket_dir(header: &Header, hash: u32) -> usize {
    (hash as usize) >> (GDBM_HASH_BITS - header.dir_bits)
}

// derives hash and bucket metadata from key
fn key_loc(header: &Header, key: &[u8]) -> (u32, usize, u32) {
    let hash = hash_key(key);
    let bucket = bucket_dir(header, hash);
    let ofs = hash % header.bucket_elems;

    (hash, bucket, ofs)
}

// does key match the partial-key field?
fn partial_key_match(key_a: &[u8], partial_b: &[u8; KEY_SMALL]) -> bool {
    if key_a.len() <= KEY_SMALL {
        key_a == &partial_b[0..key_a.len()]
    } else {
        &key_a[0..KEY_SMALL] == partial_b
    }
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
            f,
            header,
            dir: Directory { dir },
            dir_dirty: false,
            bucket_cache: BucketCache::new(),
            cur_bucket_ofs,
            cur_bucket_dir,
        })
    }

    // validate directory entry index.  currently just a bounds check.
    fn dirent_valid(&self, idx: usize) -> bool {
        idx < self.dir.len() // && self.dir.dir[idx] >= (self.header.block_sz as u64)
    }

    // read bucket into bucket cache.
    fn get_bucket(&mut self, bucket_dir: usize) -> io::Result<bool> {
        if !self.dirent_valid(bucket_dir) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket idx"));
        }

        let bucket_ofs = self.dir.dir[bucket_dir];
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

        // todo: validate and assure-sorted avail[]

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

    // return a clone of the current bucket
    fn get_current_bucket(&self) -> Bucket {
        // note: assumes will be called following get_bucket() to cache
        // assignment of dir[0] to cur_bucket at Gdbm{} creation not sufficient.
        self.bucket_cache.bucket_map[&self.cur_bucket_ofs].clone()
    }

    // since one bucket dir entry may duplicate another,
    // this function returns the next non-dup bucket dir
    fn next_bucket_dir(&self, bucket_dir_in: usize) -> usize {
        let dir_sz = self.header.dir_sz as usize;
        if bucket_dir_in >= dir_sz {
            return dir_sz;
        }

        let mut bucket_dir = bucket_dir_in;

        let cur_ofs = self.dir.dir[bucket_dir];
        while bucket_dir < dir_sz && cur_ofs == self.dir.dir[bucket_dir] {
            bucket_dir = bucket_dir + 1;
        }

        bucket_dir
    }

    // API: count entries in database
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

    // API: return first key in database, for sequential iteration start.
    pub fn first_key(&mut self) -> io::Result<Option<Vec<u8>>> {
        // get first bucket
        self.get_bucket(0)?;

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
        self.get_bucket(bucket_dir)?;
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
