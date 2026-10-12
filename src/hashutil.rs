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

pub const HASH_BITS: u32 = 31;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct PartialKey([u8; PartialKey::SIZEOF as usize]);

impl PartialKey {
    pub const SIZEOF: u32 = 4;

    pub fn new(key: &[u8]) -> Self {
        Self(
            key.iter()
                .copied()
                .chain(repeat(0))
                .take(Self::SIZEOF as usize)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
    }

    // whether the stored key prefix has only bytes < 0x80
    pub fn is_ascii(&self) -> bool {
        self.0.is_ascii()
    }

    pub fn from_reader(reader: &mut impl Read) -> io::Result<Self> {
        let mut buf = [0; Self::SIZEOF as usize];
        reader.read_exact(&mut buf)?;
        Ok(Self(buf))
    }

    pub fn serialize(self, writer: &mut impl Write) -> io::Result<()> {
        writer.write_all(&self.0)
    }
}

/// Signedness of the C `char` type on the platform whose GDBM library wrote a database.
///
/// C GDBM hashes keys one `char` at a time, so key bytes >= 0x80 hash differently depending on
/// whether `char` is signed (x86, x86-64, MIPS, macOS on ARM) or unsigned (Linux on ARM,
/// PowerPC, RISC-V and s390x). Database files do not record which was used, so gdbm-native
/// detects it from the stored hashes of existing keys the first time a key with a byte >= 0x80
/// is used, falling back to `Signed` if the database has no such keys. Keys of bytes < 0x80
/// hash the same either way. Use [`OpenOptions::char_sign`](crate::OpenOptions::char_sign) to
/// override detection, e.g. to create a database for C GDBM on an unsigned-`char` platform.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum CharSign {
    /// Signed `char`, as on x86 and x86-64. This is the default.
    #[default]
    Signed,
    /// Unsigned `char`, as on ARM, PowerPC, RISC-V and s390x.
    Unsigned,
}

// core gdbm hashing function, a port of C GDBM's _gdbm_hash()
pub fn hash_key(char_sign: CharSign, key: &[u8]) -> u32 {
    let mut value = (key.len() as u32).wrapping_mul(0x238F_13AF);

    for (index, &ch) in key.iter().enumerate() {
        let ch = match char_sign {
            CharSign::Signed => i32::from(ch as i8) as u32,
            CharSign::Unsigned => u32::from(ch),
        };
        value = value.wrapping_add(ch << (index * 5 % 24)) & 0x7FFF_FFFF;
    }

    value.wrapping_mul(1_103_515_243).wrapping_add(12345) & 0x7FFF_FFFF
}

// hash-to-bucket lookup
pub fn bucket_dir(dir_bits: u32, hash: u32) -> usize {
    (hash as usize) >> (HASH_BITS - dir_bits)
}

// derives hash and bucket metadata from key
// bucket directory index and starting element offset for a key hash
pub fn key_loc(dir_bits: u32, bucket_elems: u32, hash: u32) -> (usize, u32) {
    (bucket_dir(dir_bits, hash), hash % bucket_elems)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Expected values are from C GDBM 1.23's _gdbm_hash() built with -fsigned-char and
    // -funsigned-char respectively.
    const HASHES: &[(&[u8], u32, u32)] = &[
        (b"hello", 1_730_502_474, 1_730_502_474),
        (b"hello\0", 72_084_335, 72_084_335),
        (b"", 12345, 12345),
        (b"caf\xc3\xa9", 1_431_509_427, 986_913_203),
        (b"\xe2\x82\xac", 1_744_248_542, 834_595_294),
        (b"\xff\x80\xaa", 929_500_733, 19_847_485),
        (b"\xe0\xfb\xed\x03", 909_659_277, 6029),
        (b"\x7f\x80", 1_992_785_816, 1_083_498_392),
    ];

    #[test]
    fn test_hash() {
        for (key, signed, unsigned) in HASHES {
            assert_eq!(hash_key(CharSign::Signed, key), *signed, "signed {key:?}");
            assert_eq!(
                hash_key(CharSign::Unsigned, key),
                *unsigned,
                "unsigned {key:?}"
            );
        }
    }
}
