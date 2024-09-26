//
// ser.rs -- GDBM data structure serialization routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Read, Write};

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

pub fn write32(endian: Endian, writer: &mut impl Write, value: u32) -> io::Result<()> {
    let bytes = match endian {
        Endian::Little => value.to_le_bytes(),
        Endian::Big => value.to_be_bytes(),
    };
    writer.write_all(&bytes)
}

pub fn write64(endian: Endian, writer: &mut impl Write, value: u64) -> io::Result<()> {
    let bytes = match endian {
        Endian::Little => value.to_le_bytes(),
        Endian::Big => value.to_be_bytes(),
    };
    writer.write_all(&bytes)
}
