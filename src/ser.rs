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

/// Struct field alignment of DB file. Unless specified when opening a database, alignment defaults
/// to `Alignment::Align64` for databases created with [`Offset::LFS`](crate::Offset::LFS),
/// otherwise `Alignemnt::Align32`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Alignment {
    /// All fields aligned on 4byte boundaries.
    /// Struct sizes are multiple of 4.
    Align32,
    /// Fields larger than 4byte aligned on 8byte boundaries.
    /// Struct sizes are multiple of 8.
    Align64,
}

impl Alignment {
    /// `true` if `self` is `Align64`, otherwise `false`.
    #[must_use]
    pub fn is64(&self) -> bool {
        *self == Alignment::Align64
    }
}

/// Endianness of database file. This defaults to `Little` for databases created on all systems.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Endian {
    /// Little endian.
    Little,
    /// Big endian.
    Big,
}

/// Offset types: `LFS` (64bit) or `Small` (32bit). This relates to how many bytes are used
/// internally to store file offsets. `LFS` (8 bytes) is the default for new databases, but if you
/// need to save 4 bytes per entry (both in memory and in the database file) you can use `Small`,
/// bearing in mind that 4 byte offsets limit the database to 2^32 bytes (4GIB).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Offset {
    /// Offset fields are 32bit.
    Small,
    /// Offset fields are 64bit.
    LFS,
}

/// Container for layout possibilities.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct Layout {
    pub alignment: Alignment,
    pub endian: Endian,
    pub offset: Offset,
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
