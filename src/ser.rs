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

/// The C struct layout of a database file, which depends on the platform whose C GDBM
/// created it. GDBM files record their byte order and offset size, but not their alignment;
/// gdbm-native works it out when opening a database.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Layout {
    /// 32-bit file offsets, as written by C GDBM built without large file support
    /// (`off_t` of 4 bytes). Databases are limited to 2 GiB.
    Off32,
    /// 64-bit file offsets aligned on 4 bytes, as written by C GDBM on i386 with large file
    /// support.
    Off64Align4,
    /// 64-bit file offsets aligned on 8 bytes, as written by C GDBM on 64-bit platforms and on
    /// 32-bit ARM and MIPS with large file support. This is the default for new databases.
    #[default]
    Off64Align8,
}

// Struct field alignment of DB file.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Alignment {
    /// All fields aligned on 4byte boundaries.
    /// Struct sizes are multiple of 4.
    Align32,
    /// Fields larger than 4byte aligned on 8byte boundaries.
    /// Struct sizes are multiple of 8.
    Align64,
}

impl Alignment {
    pub fn is64(self) -> bool {
        self == Alignment::Align64
    }
}

/// Byte order of a database file. GDBM files are written in the byte order of the platform that
/// created them; gdbm-native reads and writes either. New databases default to `Little`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Endian {
    /// Little endian.
    #[default]
    Little,
    /// Big endian.
    Big,
}

// Size of file offset fields.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Offset {
    Small,
    Lfs,
}

// The complete on-disk format: byte order plus struct layout, the latter split into the two
// properties the (de)serialization code needs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Format {
    pub alignment: Alignment,
    pub endian: Endian,
    pub offset: Offset,
}

impl Format {
    pub fn new(layout: Layout, endian: Endian) -> Self {
        let (offset, alignment) = match layout {
            Layout::Off32 => (Offset::Small, Alignment::Align32),
            Layout::Off64Align4 => (Offset::Lfs, Alignment::Align32),
            Layout::Off64Align8 => (Offset::Lfs, Alignment::Align64),
        };
        Self {
            alignment,
            endian,
            offset,
        }
    }

    pub fn layout(self) -> Layout {
        match (self.offset, self.alignment) {
            (Offset::Small, _) => Layout::Off32,
            (Offset::Lfs, Alignment::Align32) => Layout::Off64Align4,
            (Offset::Lfs, Alignment::Align64) => Layout::Off64Align8,
        }
    }
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
