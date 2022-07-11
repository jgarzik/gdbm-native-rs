use byteorder::{LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io::{self, Read};

use crate::ser::{w32, woff_t};
use crate::{AvailElem, KEY_SMALL};

pub const BUCKET_AVAIL: u32 = 6;

#[derive(Debug, Clone)]
pub struct BucketElement {
    pub hash: u32,
    pub key_start: [u8; 4],
    pub data_ofs: u64,
    pub key_size: u32,
    pub data_size: u32,
}

impl BucketElement {
    pub fn from_reader(is_64: bool, rdr: &mut impl Read) -> io::Result<Self> {
        let hash = rdr.read_u32::<LittleEndian>()?;

        let mut key_start = [0; KEY_SMALL];
        rdr.read(&mut key_start)?;

        let data_ofs: u64;
        if is_64 {
            data_ofs = rdr.read_u64::<LittleEndian>()?;
        } else {
            data_ofs = rdr.read_u32::<LittleEndian>()? as u64;
        }

        let key_size = rdr.read_u32::<LittleEndian>()?;
        let data_size = rdr.read_u32::<LittleEndian>()?;

        Ok(BucketElement {
            hash,
            key_start,
            data_ofs,
            key_size,
            data_size,
        })
    }

    pub fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
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
    pub av_count: u32,
    pub avail: Vec<AvailElem>,
    pub bits: u32,
    pub count: u32,
    pub tab: Vec<BucketElement>,
}

impl Bucket {
    pub fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
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
    pub bucket_map: HashMap<u64, Bucket>,
    pub dirty: HashMap<u64, bool>,
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
