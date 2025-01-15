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
use crate::ser::{read32, read64, write32, write64, Layout, Offset};

pub fn build_dir_size(offset: Offset, block_sz: u32) -> (u32, u32) {
    let block_sz = block_sz.max(512);

    let mut dir_size = 8 * match offset {
        Offset::Small => 4,
        Offset::LFS => 8,
    };
    let mut dir_bits = 3;

    while dir_size < block_sz && dir_bits < HASH_BITS - 3 {
        dir_size <<= 1;
        dir_bits += 1;
    }

    (dir_size, dir_bits)
}

#[derive(Debug, PartialEq)]
pub struct Directory {
    pub dir: Vec<u64>,
    pub dirty: bool,
}

impl Directory {
    pub fn new(bucket_offsets: Vec<u64>) -> Self {
        Self {
            dir: bucket_offsets,
            dirty: true,
        }
    }

    pub fn serialize(&self, layout: Layout, writer: &mut impl Write) -> io::Result<()> {
        self.dir.iter().try_for_each(|ofs| match layout.offset {
            Offset::Small => write32(layout.endian, writer, *ofs as u32),
            Offset::LFS => write64(layout.endian, writer, *ofs),
        })
    }

    pub fn from_reader(layout: Layout, extent: u32, reader: &mut impl Read) -> io::Result<Self> {
        Ok(Self {
            dirty: false,
            dir: match layout.offset {
                Offset::Small => (0..extent / 4)
                    .map(|_| read32(layout.endian, reader).map(u64::from))
                    .collect::<io::Result<Vec<_>>>(),
                Offset::LFS => (0..extent / 8)
                    .map(|_| read64(layout.endian, reader))
                    .collect::<io::Result<Vec<_>>>(),
            }?,
        })
    }

    // double the dir size by duplicating every element
    pub fn extend(&self) -> Self {
        Self {
            dir: self
                .dir
                .iter()
                .copied()
                .flat_map(|offset| std::iter::repeat(offset).take(2))
                .collect(),
            dirty: true,
        }
    }

    // serialized size of this instance
    pub fn extent(&self, layout: Layout) -> u32 {
        match layout.offset {
            Offset::Small => self.dir.len() as u32 * 4,
            Offset::LFS => self.dir.len() as u32 * 8,
        }
    }

    // validate all buckets are inside file
    pub fn validate(&self, start: u64, end: u64, bucket_size: u32) -> bool {
        self.dir
            .iter()
            .all(|&offset| offset >= start && offset + u64::from(bucket_size) <= end)
    }

    // update_bucket_split is called after a bucket is split.
    // It finds the range of dir entries matching the one at offset,
    // based on dir_bits and bucket_bits.
    // It then replaces the second half of those offsets with the new bucket offset.
    pub fn update_bucket_split(
        &mut self,
        dir_bits: u32,
        bucket_bits: u32,
        old_bucket_offset: u64,
        new_bucket_offset: u64,
    ) {
        let num_entries = (1 << dir_bits) >> (bucket_bits - 1);
        let range_start = self
            .dir
            .iter()
            .position(|&offset| offset == old_bucket_offset)
            .unwrap();

        // replace offsets in second half of the range with the new offset.
        (range_start + (num_entries >> 1)..range_start + num_entries)
            .for_each(|index| self.dir[index] = new_bucket_offset);

        self.dirty = true;
    }
}

#[cfg(test)]
mod test {
    use super::Directory;

    #[test]
    fn test_extend() {
        struct Test<'a> {
            name: &'a str,
            dir: Directory,
            expected: Directory,
        }

        for test in [
            Test {
                name: "empty",
                dir: Directory {
                    dir: vec![],
                    dirty: false,
                },
                expected: Directory {
                    dir: vec![],
                    dirty: true,
                },
            },
            Test {
                name: "one",
                dir: Directory {
                    dir: vec![1],
                    dirty: false,
                },
                expected: Directory {
                    dir: vec![1, 1],
                    dirty: true,
                },
            },
            Test {
                name: "two",
                dir: Directory {
                    dir: vec![1, 2],
                    dirty: false,
                },
                expected: Directory {
                    dir: vec![1, 1, 2, 2],
                    dirty: true,
                },
            },
        ] {
            let got = test.dir.extend();
            assert!(
                got == test.expected,
                "test: {}\nexpected: {:?}\ngot: {got:?}",
                test.name,
                test.expected
            );
        }
    }
}
