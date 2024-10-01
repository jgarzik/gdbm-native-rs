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
use std::io::{self, Error, ErrorKind, Read, Write};

use crate::hashutil::{hash_key, PartialKey};
use crate::ser::{read32, read64, write32, write64, Alignment, Endian};
use crate::{AvailElem, Header};

pub const BUCKET_AVAIL: usize = 6;

#[derive(Debug, Clone)]
pub struct BucketElement {
    pub hash: u32,
    pub key_start: PartialKey,
    pub data_ofs: u64,
    pub key_size: u32,
    pub data_size: u32,
}

impl BucketElement {
    pub fn new(key: &[u8], data: &[u8], offset: u64) -> Self {
        Self {
            hash: hash_key(key),
            key_start: PartialKey::new(key),
            data_ofs: offset,
            key_size: key.len() as u32,
            data_size: data.len() as u32,
        }
    }

    pub fn from_reader(
        alignment: Alignment,
        endian: Endian,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        let hash = read32(endian, reader)?;

        let key_start = PartialKey::from_reader(reader)?;

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

    pub fn serialize(
        &self,
        alignment: Alignment,
        endian: Endian,
        writer: &mut impl Write,
    ) -> io::Result<()> {
        write32(endian, writer, self.hash)?;

        self.key_start.serialize(writer)?;

        match alignment {
            Alignment::Align32 => write32(endian, writer, self.data_ofs as u32)?,
            Alignment::Align64 => write64(endian, writer, self.data_ofs)?,
        }

        write32(endian, writer, self.key_size)?;
        write32(endian, writer, self.data_size)?;

        Ok(())
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

    pub fn serialize(
        &self,
        alignment: Alignment,
        endian: Endian,
        writer: &mut impl Write,
    ) -> io::Result<()> {
        assert!(self.avail.len() <= BUCKET_AVAIL);

        //
        // avail section
        //

        write32(endian, writer, self.avail.len() as u32)?;
        write32(endian, writer, 0)?;

        // valid avail elements
        self.avail
            .iter()
            .try_for_each(|elem| elem.serialize(alignment, endian, writer))?;

        // dummy avail elements
        (self.avail.len()..BUCKET_AVAIL)
            .try_for_each(|_| AvailElem::default().serialize(alignment, endian, writer))?;

        //
        // misc section
        //
        write32(endian, writer, self.bits)?;
        write32(endian, writer, self.count)?;

        //
        // bucket elements section
        //
        self.tab
            .iter()
            .try_for_each(|elem| elem.serialize(alignment, endian, writer))?;

        Ok(())
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
