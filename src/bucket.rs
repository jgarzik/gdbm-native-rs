//
// bucket.rs -- GDBM bucket routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::collections::HashMap;
use std::io::{self, Error, ErrorKind, Read};

use crate::ser::{w32, woff_t, Alignment, Endian};
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
        rdr: &mut impl Read,
    ) -> io::Result<Self> {
        let hash = if endian == Endian::Little {
            rdr.read_u32::<LittleEndian>()?
        } else {
            rdr.read_u32::<BigEndian>()?
        };

        let mut key_start = [0; KEY_SMALL];
        rdr.read_exact(&mut key_start)?;

        let data_ofs: u64;
        let (key_size, data_size);

        if endian == Endian::Little {
            if alignment == Alignment::Align64 {
                data_ofs = rdr.read_u64::<LittleEndian>()?;
            } else {
                data_ofs = rdr.read_u32::<LittleEndian>()? as u64;
            }

            key_size = rdr.read_u32::<LittleEndian>()?;
            data_size = rdr.read_u32::<LittleEndian>()?;
        } else {
            if alignment == Alignment::Align64 {
                data_ofs = rdr.read_u64::<BigEndian>()?;
            } else {
                data_ofs = rdr.read_u32::<BigEndian>()? as u64;
            }

            key_size = rdr.read_u32::<BigEndian>()?;
            data_size = rdr.read_u32::<BigEndian>()?;
        }

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
    pub fn from_reader(header: &Header, rdr: &mut impl Read) -> io::Result<Self> {
        // read avail section
        let av_count;
        if header.endian == Endian::Little {
            av_count = rdr.read_u32::<LittleEndian>()?;
            let _padding = rdr.read_u32::<LittleEndian>()?;
        } else {
            av_count = rdr.read_u32::<BigEndian>()?;
            let _padding = rdr.read_u32::<BigEndian>()?;
        }

        // read av_count entries from bucket_avail[]
        let mut avail = Vec::new();
        for _idx in 0..av_count {
            let av_elem = AvailElem::from_reader(header.alignment, header.endian, rdr)?;
            avail.push(av_elem);
        }

        // read remaining to-be-ignored entries from bucket_avail[]
        let pad_elems = BUCKET_AVAIL - avail.len();
        for _idx in 0..pad_elems {
            let _av_elem = AvailElem::from_reader(header.alignment, header.endian, rdr)?;
        }

        // todo: validate and assure-sorted avail[]

        // read misc. section
        let (bits, count);

        if header.endian == Endian::Little {
            bits = rdr.read_u32::<LittleEndian>()?;
            count = rdr.read_u32::<LittleEndian>()?;
        } else {
            bits = rdr.read_u32::<BigEndian>()?;
            count = rdr.read_u32::<BigEndian>()?;
        }

        if !(count <= header.bucket_elems && bits <= header.dir_bits) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket c/b"));
        }

        // read bucket elements section
        let mut tab = Vec::new();
        for _idx in 0..header.bucket_elems {
            let bucket_elem = BucketElement::from_reader(header.alignment, header.endian, rdr)?;
            tab.push(bucket_elem);
        }

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
