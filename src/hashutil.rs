//
// hashutil.rs -- GDBM hash library routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use crate::header::Header;
use crate::{GDBM_HASH_BITS, KEY_SMALL};

// core gdbm hashing function
pub fn hash_key(key: &[u8]) -> u32 {
    let mut index: u32 = 0;
    let mut value: u32 = key.len() as u32;
    value = value.wrapping_mul(0x238F13AF);

    for ch in key.iter() {
        value = (value + ((*ch as u32) << (index * 5 % 24))) & 0x7FFFFFFF;
        index = index + 1;
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

// does key match the partial-key field?
pub fn partial_key_match(key_a: &[u8], partial_b: &[u8; KEY_SMALL]) -> bool {
    if key_a.len() <= KEY_SMALL {
        key_a == &partial_b[0..key_a.len()]
    } else {
        &key_a[0..KEY_SMALL] == partial_b
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
