//
// dir.rs -- GDBM hash directory routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Read, Write};

use crate::hashutil::HASH_BITS;
use crate::ser::{read32, read64, write32, write64, Alignment, Endian};

pub fn build_dir_size(block_sz: u32) -> (u32, u32) {
    let mut dir_size = 8 * 8; // fixme: 8==off_t==vary on is_lfs
    let mut dir_bits = 3;

    while dir_size < block_sz && dir_bits < HASH_BITS - 3 {
        dir_size <<= 1;
        dir_bits += 1;
    }

    (dir_size, dir_bits)
}

#[derive(Debug)]
pub struct Directory {
    pub dir: Vec<u64>,
    pub dirty: bool,
}

impl Directory {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.dir.len()
    }

    pub fn serialize(
        &self,
        alignment: Alignment,
        endian: Endian,
        writer: &mut impl Write,
    ) -> io::Result<()> {
        self.dir.iter().try_for_each(|ofs| match alignment {
            Alignment::Align32 => write32(endian, writer, *ofs as u32),
            Alignment::Align64 => write64(endian, writer, *ofs),
        })
    }

    pub fn from_reader(
        alignment: Alignment,
        endian: Endian,
        extent: u32,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        let count = extent
            / match alignment {
                Alignment::Align32 => 4,
                Alignment::Align64 => 8,
            };
        Ok(Self {
            dirty: false,
            dir: (0..count)
                .map(|_| match alignment {
                    Alignment::Align32 => read32(endian, reader).map(|v| v as u64),
                    Alignment::Align64 => read64(endian, reader),
                })
                .collect::<io::Result<Vec<_>>>()?,
        })
    }

    // serialized size of this instance
    pub fn extent(&self, alignment: Alignment) -> u32 {
        match alignment {
            Alignment::Align32 => self.dir.len() as u32 * 4,
            Alignment::Align64 => self.dir.len() as u32 * 8,
        }
    }

    // validate all buckets are inside file
    pub fn validate(&self, start: u64, end: u64, bucket_size: u32) -> bool {
        self.dir
            .iter()
            .all(|&offset| offset >= start && offset + bucket_size as u64 <= end)
    }

    // update_bucket_split is called after a bucket is split.
    // It finds the range of dir entries matching the one at offset,
    // based on dir_bits and bucket_bits.
    // It then replaces the second half of those offsets with the new bucket offset.
    pub fn update_bucket_split(
        &mut self,
        dir_bits: u32,
        bucket_bits: u32,
        offset: usize,
        bucket_offset: u64,
    ) {
        let num_entries = (1 << dir_bits) >> (bucket_bits - 1);
        let range_start = offset / num_entries * num_entries;

        // replace offsets in second half of the range with the new offset.
        (range_start + (num_entries >> 1)..range_start + num_entries)
            .for_each(|index| self.dir[index] = bucket_offset);

        self.dirty = true;
    }
}
