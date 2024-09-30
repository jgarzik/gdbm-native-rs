//
// dir.rs -- GDBM hash directory routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Seek, SeekFrom, Write};

use crate::ser::{read32, read64, write32, write64, Alignment, Endian};
use crate::{Header, GDBM_HASH_BITS};

pub fn build_dir_size(block_sz: u32) -> (u32, u32) {
    let mut dir_size = 8 * 8; // fixme: 8==off_t==vary on is_lfs
    let mut dir_bits = 3;

    while dir_size < block_sz && dir_bits < GDBM_HASH_BITS - 3 {
        dir_size <<= 1;
        dir_bits += 1;
    }

    (dir_size, dir_bits)
}

#[derive(Debug)]
pub struct Directory {
    pub dir: Vec<u64>,
}

impl Directory {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.dir.len()
    }

    pub fn serialize(
        &self,
        alignment: Alignment,
        endian: Endian,
        writer: &mut impl Write,
    ) -> io::Result<()> {
        self.dir.iter().try_for_each(|ofs| match alignment {
            Alignment::Align32 => write32(endian, writer, *ofs as u32),
            Alignment::Align64 => write64(endian, writer, *ofs),
        })
    }
}

pub fn dirent_elem_size(alignment: Alignment) -> usize {
    match alignment {
        Alignment::Align32 => 4,
        Alignment::Align64 => 8,
    }
}

// Read C-struct-based bucket directory (a vector of storage offsets)
pub fn dir_reader(f: &mut std::fs::File, header: &Header) -> io::Result<Vec<u64>> {
    f.seek(SeekFrom::Start(header.dir_ofs))?;
    (0..header.dir_sz as usize / dirent_elem_size(header.alignment()))
        .map(|_| match header.alignment() {
            Alignment::Align32 => read32(header.endian(), f).map(|v| v as u64),
            Alignment::Align64 => read64(header.endian(), f),
        })
        .collect::<io::Result<Vec<_>>>()
}
