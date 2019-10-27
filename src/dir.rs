use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Seek, SeekFrom};

use crate::ser::woff_t;
use crate::{Header, GDBM_HASH_BITS};

pub fn build_dir_size(block_sz: u32) -> (u32, u32) {
    let mut dir_size = 8 * 8; // fixme: 8==off_t==vary on is_64
    let mut dir_bits = 3;

    while dir_size < block_sz && dir_bits < GDBM_HASH_BITS - 3 {
        dir_size = dir_size << 1;
        dir_bits = dir_bits + 1;
    }

    (dir_size, dir_bits)
}

#[derive(Debug)]
pub struct Directory {
    pub dir: Vec<u64>,
}

impl Directory {
    pub fn len(&self) -> usize {
        self.dir.len()
    }

    pub fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();

        for ofs in &self.dir {
            buf.append(&mut woff_t(is_64, is_le, *ofs));
        }

        buf
    }
}

// Read C-struct-based bucket directory (a vector of storage offsets)
pub fn dir_reader(f: &mut std::fs::File, header: &Header) -> io::Result<Vec<u64>> {
    let is_64 = header.is_64;
    let dirent_sz = match is_64 {
        true => 8,
        false => 4,
    };
    let dirent_count = header.dir_sz / dirent_sz;

    let mut dir = Vec::new();
    dir.reserve_exact(dirent_count as usize);

    let _pos = f.seek(SeekFrom::Start(header.dir_ofs))?;

    for _idx in 0..dirent_count {
        let ofs: u64;
        if is_64 {
            ofs = f.read_u64::<LittleEndian>()?;
        } else {
            ofs = f.read_u32::<LittleEndian>()? as u64;
        }
        dir.push(ofs);
    }

    Ok(dir)
}
