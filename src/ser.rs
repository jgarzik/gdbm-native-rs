//
// ser.rs -- GDBM data structure serialization routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Read};

/// Field alignment of DB file
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Alignment {
    /// File offset fields are 32bit
    Align32,
    /// File offset fields are 64bit
    Align64,
}

impl Alignment {
    pub fn is64(&self) -> bool {
        *self == Alignment::Align64
    }
}

/// Endianness of DB file
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Endian {
    Little,
    Big,
}

pub fn read32(endian: Endian, reader: &mut impl Read) -> io::Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(match endian {
        Endian::Little => u32::from_le_bytes(bytes),
        Endian::Big => u32::from_be_bytes(bytes),
    })
}

pub fn read64(endian: Endian, reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(match endian {
        Endian::Little => u64::from_le_bytes(bytes),
        Endian::Big => u64::from_be_bytes(bytes),
    })
}

// serialize u32, with runtime endian selection
pub fn w32(endian: Endian, val: u32) -> Vec<u8> {
    match endian {
        Endian::Little => val.to_le_bytes(),
        Endian::Big => val.to_be_bytes(),
    }
    .to_vec()
}

// serialize u64, with runtime endian selection
pub fn w64(endian: Endian, val: u64) -> Vec<u8> {
    match endian {
        Endian::Little => val.to_le_bytes(),
        Endian::Big => val.to_be_bytes(),
    }
    .to_vec()
}

pub fn woff_t(alignment: Alignment, endian: Endian, val: u64) -> Vec<u8> {
    match alignment {
        Alignment::Align32 => w32(endian, val as u32),
        Alignment::Align64 => w64(endian, val),
    }
}
