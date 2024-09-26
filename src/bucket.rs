//
// bucket.rs -- GDBM bucket routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::io::{self, Error, ErrorKind, Read};

use crate::ser::{read32, read64, w32, woff_t, Alignment, Endian};
use crate::{AvailElem, Header, KEY_SMALL};

pub const BUCKET_AVAIL: usize = 6;

#[derive(Debug, Clone)]
pub struct BucketElement {
    pub hash: u32,
    pub key_start: [u8; 4],
    pub data_ofs: u64,
    pub key_size: u32,
    pub data_size: u32,
}

impl BucketElement {
    pub fn from_reader(
        alignment: Alignment,
        endian: Endian,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        let hash = read32(endian, reader)?;

        let mut key_start = [0; KEY_SMALL];
        reader.read_exact(&mut key_start)?;

        let data_ofs = match alignment {
            Alignment::Align32 => (read32(endian, reader)?) as u64,
            Alignment::Align64 => read64(endian, reader)?,
        };

        let key_size = read32(endian, reader)?;
        let data_size = read32(endian, reader)?;

        Ok(BucketElement {
            hash,
            key_start,
            data_ofs,
            key_size,
            data_size,
        })
    }

    pub fn serialize(&self, alignment: Alignment, endian: Endian) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(endian, self.hash));
        buf.append(&mut self.key_start.to_vec());
        buf.append(&mut woff_t(alignment, endian, self.data_ofs));
        buf.append(&mut w32(endian, self.key_size));
        buf.append(&mut w32(endian, self.data_size));

        buf
    }
}

#[derive(Debug, Clone)]
pub struct Bucket {
    // on-disk gdbm database hash bucket
    pub avail: Vec<AvailElem>,
    pub bits: u32,
    pub count: u32,
    pub tab: Vec<BucketElement>,
}

impl Bucket {
    pub fn from_reader(header: &Header, reader: &mut impl Read) -> io::Result<Self> {
        // read avail section
        let av_count = read32(header.endian(), reader)? as usize;

        // always padding here
        read32(header.endian(), reader)?;

        // read av_count entries from bucket_avail[]
        let avail = (0..av_count)
            .map(|_| AvailElem::from_reader(header.alignment(), header.endian(), reader))
            .collect::<io::Result<Vec<_>>>()?;

        // read remaining to-be-ignored entries from bucket_avail[]
        (av_count..BUCKET_AVAIL).try_for_each(|_| {
            AvailElem::from_reader(header.alignment(), header.endian(), reader).map(|_| ())
        })?;

        // todo: validate and assure-sorted avail[]

        // read misc. section
        let bits = read32(header.endian(), reader)?;
        let count = read32(header.endian(), reader)?;

        if !(count <= header.bucket_elems && bits <= header.dir_bits) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket c/b"));
        }

        // read bucket elements section
        let tab = (0..header.bucket_elems)
            .map(|_| BucketElement::from_reader(header.alignment(), header.endian(), reader))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Bucket {
            avail,
            bits,
            count,
            tab,
        })
    }

    pub fn serialize(&self, alignment: Alignment, endian: Endian) -> Vec<u8> {
        let mut buf = Vec::new();

        //
        // avail section
        //

        let av_count: u32 = self.avail.len() as u32;
        buf.append(&mut w32(endian, av_count));
        if alignment == Alignment::Align64 {
            let padding: u32 = 0;
            buf.append(&mut w32(endian, padding));
        }

        // valid avail elements
        for avail_elem in &self.avail {
            buf.append(&mut avail_elem.serialize(alignment, endian));
        }

        // dummy avail elements
        assert!(self.avail.len() <= BUCKET_AVAIL);
        let pad_elems = BUCKET_AVAIL - self.avail.len();
        for _idx in 0..pad_elems {
            let dummy_elem = AvailElem { addr: 0, sz: 0 };
            buf.append(&mut dummy_elem.serialize(alignment, endian));
        }

        //
        // misc section
        //
        buf.append(&mut w32(endian, self.bits));
        buf.append(&mut w32(endian, self.count));

        //
        // bucket elements section
        //
        for bucket_elem in &self.tab {
            buf.append(&mut bucket_elem.serialize(alignment, endian));
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
        for ofs in self.dirty.keys() {
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
