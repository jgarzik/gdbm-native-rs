//
// ser.rs -- GDBM data structure serialization routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use byteorder::{BigEndian, ByteOrder, LittleEndian};

// serialize u32, with runtime endian selection
pub fn w32(is_le: bool, val: u32) -> Vec<u8> {
    let mut buf = vec![0; 4];

    match is_le {
        true => LittleEndian::write_u32(&mut buf, val),
        false => BigEndian::write_u32(&mut buf, val),
    }

    buf
}

// serialize u64, with runtime endian selection
pub fn w64(is_le: bool, val: u64) -> Vec<u8> {
    let mut buf = vec![0; 8];

    match is_le {
        true => LittleEndian::write_u64(&mut buf, val),
        false => BigEndian::write_u64(&mut buf, val),
    }

    buf
}

pub fn woff_t(is_lfs: bool, is_le: bool, val: u64) -> Vec<u8> {
    match is_lfs {
        true => w64(is_le, val),
        false => w32(is_le, val as u32),
    }
}
