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
use std::ops::Range;

use crate::hashutil::HASH_BITS;
use crate::ser::{Format, Offset, read32, read64, write32, write64};

pub fn build_dir_size(offset: Offset, block_sz: u32) -> (u32, u32) {
    let block_sz = block_sz.max(512);

    let mut dir_size = 8 * match offset {
        Offset::Small => 4,
        Offset::Lfs => 8,
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

    pub fn serialize(&self, layout: Format, writer: &mut impl Write) -> io::Result<()> {
        self.dir.iter().try_for_each(|ofs| match layout.offset {
            Offset::Small => write32(layout.endian, writer, *ofs as u32),
            Offset::Lfs => write64(layout.endian, writer, *ofs),
        })
    }

    pub fn from_reader(layout: Format, extent: u32, reader: &mut impl Read) -> io::Result<Self> {
        Ok(Self {
            dirty: false,
            dir: match layout.offset {
                Offset::Small => (0..extent / 4)
                    .map(|_| read32(layout.endian, reader).map(u64::from))
                    .collect::<io::Result<Vec<_>>>(),
                Offset::Lfs => (0..extent / 8)
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
                .flat_map(|offset| std::iter::repeat_n(offset, 2))
                .collect(),
            dirty: true,
        }
    }

    // serialized size of this instance
    pub fn extent(&self, layout: Format) -> u32 {
        match layout.offset {
            Offset::Small => self.dir.len() as u32 * 4,
            Offset::Lfs => self.dir.len() as u32 * 8,
        }
    }

    // validate all buckets are inside file
    pub fn validate(&self, start: u64, end: u64, bucket_size: u32) -> bool {
        self.dir.iter().all(|&offset| {
            offset >= start
                && offset
                    .checked_add(u64::from(bucket_size))
                    .is_some_and(|bucket_end| bucket_end <= end)
        })
    }

    // The directory entries that refer to the bucket at entry index, which has bits bits: the
    // 2^(dir_bits - bits) entries sharing the top bits bits of index (C _gdbm_split_bucket's
    // dir_start0..dir_end). None if that range is out of bounds or any entry in it refers to
    // another bucket (corrupt directory or bucket bits).
    pub fn bucket_range(&self, dir_bits: u32, bits: u32, index: usize) -> Option<Range<usize>> {
        let len = 1usize.checked_shl(dir_bits.checked_sub(bits)?)?;
        let start = index & !(len - 1);
        let range = start..start.checked_add(len)?;
        let offset = *self.dir.get(index)?;
        self.dir
            .get(range.clone())?
            .iter()
            .all(|&entry| entry == offset)
            .then_some(range)
    }

    // update_bucket_split is called after the bucket at entry index, which had old_bits bits, is
    // split: the old bucket keeps the first half of its range of entries and the new bucket gets
    // the second half, as in C _gdbm_split_bucket. None (and the directory is unchanged) if the
    // range is invalid; see bucket_range.
    pub fn update_bucket_split(
        &mut self,
        dir_bits: u32,
        old_bits: u32,
        index: usize,
        new_bucket_offset: u64,
    ) -> Option<()> {
        let range = self.bucket_range(dir_bits, old_bits, index)?;
        if range.len() < 2 {
            return None;
        }

        let half = range.start + range.len() / 2;
        self.dir[half..range.end].fill(new_bucket_offset);
        self.dirty = true;

        Some(())
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

    fn dir(offsets: &[u64]) -> Directory {
        Directory {
            dir: offsets.to_vec(),
            dirty: false,
        }
    }

    #[test]
    fn test_bucket_range() {
        // dir_bits 2: bucket 1 has 1 bit (entries 0-1), buckets 2 and 3 have 2 bits
        let d = dir(&[1, 1, 2, 3]);
        assert_eq!(d.bucket_range(2, 1, 0), Some(0..2));
        assert_eq!(d.bucket_range(2, 1, 1), Some(0..2));
        assert_eq!(d.bucket_range(2, 2, 2), Some(2..3));
        assert_eq!(d.bucket_range(2, 2, 3), Some(3..4));
        // bits too small for the entries referring to the bucket
        assert_eq!(d.bucket_range(2, 0, 0), None);
        assert_eq!(d.bucket_range(2, 1, 2), None);
        // bits beyond dir_bits, index or range beyond the directory
        assert_eq!(d.bucket_range(2, 3, 0), None);
        assert_eq!(d.bucket_range(2, 2, 4), None);
        assert_eq!(d.bucket_range(3, 1, 0), None);
        assert_eq!(dir(&[1, 1, 1]).bucket_range(2, 0, 0), None);
        assert_eq!(d.bucket_range(200, 0, 0), None);
    }

    #[test]
    fn test_update_bucket_split() {
        let mut d = dir(&[1, 1, 1, 1, 2, 2, 3, 4]);
        assert_eq!(d.update_bucket_split(3, 1, 2, 5), Some(()));
        assert_eq!(d.dir, [1, 1, 5, 5, 2, 2, 3, 4]);
        assert!(d.dirty);
        assert_eq!(d.update_bucket_split(3, 2, 5, 6), Some(()));
        assert_eq!(d.dir, [1, 1, 5, 5, 2, 6, 3, 4]);

        // a bucket with as many bits as the directory has one entry: nothing to split into
        let mut d = dir(&[1, 2]);
        assert_eq!(d.update_bucket_split(1, 1, 0, 3), None);
        // the entries do not match the bucket's bits: the directory is left alone
        let mut d = dir(&[1, 1, 2, 1]);
        assert_eq!(d.update_bucket_split(2, 0, 0, 3), None);
        assert_eq!(d, dir(&[1, 1, 2, 1]));
    }
}
