//
// hashutil.rs -- GDBM hash library routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Read, Write};
use std::iter::repeat;

use crate::header::Header;
use crate::{GDBM_HASH_BITS, KEY_SMALL};

#[derive(Clone, Debug, PartialEq)]
pub struct PartialKey([u8; KEY_SMALL]);

impl PartialKey {
    pub fn new(key: &[u8]) -> Self {
        Self(
            key.iter()
                .cloned()
                .chain(repeat(0))
                .take(KEY_SMALL)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
    }

    pub fn from_reader(reader: &mut impl Read) -> io::Result<Self> {
        let mut buf = [0; KEY_SMALL];
        reader.read_exact(&mut buf)?;
        Ok(Self(buf))
    }

    pub fn serialize(&self, writer: &mut impl Write) -> io::Result<()> {
        writer.write_all(&self.0)
    }
}

// core gdbm hashing function
pub fn hash_key(key: &[u8]) -> u32 {
    let mut value: u32 = key.len() as u32;
    value = value.wrapping_mul(0x238F13AF);

    for (index, ch) in key.iter().enumerate() {
        value = (value + ((*ch as u32) << (index * 5 % 24))) & 0x7FFFFFFF;
    }
    value = (value.wrapping_mul(1103515243) + 12345) & 0x7FFFFFFF;

    value
}

// hash-to-bucket lookup
pub fn bucket_dir(header: &Header, hash: u32) -> usize {
    (hash as usize) >> (GDBM_HASH_BITS - header.dir_bits)
}

// derives hash and bucket metadata from key
pub fn key_loc(header: &Header, key: &[u8]) -> (u32, usize, u32) {
    let hash = hash_key(key);
    let bucket = bucket_dir(header, hash);
    let ofs = hash % header.bucket_elems;

    (hash, bucket, ofs)
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
}
