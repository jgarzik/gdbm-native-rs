//
// dir.rs -- GDBM hash directory routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::{self, Seek, SeekFrom};

use crate::ser::woff_t;
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

    pub fn serialize(&self, is_lfs: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();

        for ofs in &self.dir {
            buf.append(&mut woff_t(is_lfs, is_le, *ofs));
        }

        buf
    }
}

pub fn dirent_elem_size(is_lfs: bool) -> usize {
    match is_lfs {
        true => 8,
        false => 4,
    }
}

fn roff_t(f: &mut std::fs::File, is_lfs: bool, is_le: bool) -> io::Result<u64> {
    let v;

    if is_le {
        if is_lfs {
            v = f.read_u64::<LittleEndian>()?;
        } else {
            v = f.read_u32::<LittleEndian>()? as u64;
        }
    } else if is_lfs {
        v = f.read_u64::<BigEndian>()?;
    } else {
        v = f.read_u32::<BigEndian>()? as u64;
    }

    Ok(v)
}

// Read C-struct-based bucket directory (a vector of storage offsets)
pub fn dir_reader(f: &mut std::fs::File, header: &Header) -> io::Result<Vec<u64>> {
    let is_lfs = header.is_lfs;
    let dirent_count = header.dir_sz as usize / dirent_elem_size(is_lfs);

    let mut dir = Vec::new();
    dir.reserve_exact(dirent_count);

    let _pos = f.seek(SeekFrom::Start(header.dir_ofs))?;

    for _idx in 0..dirent_count {
        let ofs = roff_t(f, header.is_lfs, header.is_le)?;
        dir.push(ofs);
    }

    Ok(dir)
}
