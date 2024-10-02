//
// header.rs -- GDBM global file header routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Error, ErrorKind, Read, Write};

use crate::dir::build_dir_size;
use crate::magic::Magic;
use crate::ser::{read32, read64, write32, write64, Alignment, Endian};
use crate::{
    AvailBlock, GDBM_AVAIL_ELEM_SZ, GDBM_BUCKET_ELEM_SZ, GDBM_HASH_BUCKET_SZ, GDBM_HDR_SZ,
};

#[derive(Debug)]
pub struct Header {
    // on-disk gdbm database file header
    magic: Magic,
    pub block_sz: u32,
    pub dir_ofs: u64,
    pub dir_sz: u32,
    pub dir_bits: u32,
    pub bucket_sz: u32,
    pub bucket_elems: u32,
    pub next_block: u64,

    pub avail: AvailBlock,

    // following fields are calculated, not stored
    pub dirty: bool,
}

pub fn bucket_count(bucket_sz: u32) -> u32 {
    (bucket_sz - GDBM_HASH_BUCKET_SZ) / GDBM_BUCKET_ELEM_SZ + 1
}

impl Header {
    pub fn from_reader(metadata: &std::fs::Metadata, reader: &mut impl Read) -> io::Result<Self> {
        let file_sz = metadata.len();

        // fixme: read u32, not u64, if is_lfs

        let magic = Magic::from_reader(reader)?;
        let block_sz = read32(magic.endian(), reader)?;
        let dir_ofs = read64(magic.endian(), reader)?;
        let dir_sz = read32(magic.endian(), reader)?;
        let dir_bits = read32(magic.endian(), reader)?;
        let bucket_sz = read32(magic.endian(), reader)?;
        let bucket_elems = read32(magic.endian(), reader)?;
        let next_block = read64(magic.endian(), reader)?;
        let avail = AvailBlock::from_reader(magic.alignment(), magic.endian(), reader)?;

        if !(block_sz > GDBM_HDR_SZ && block_sz - GDBM_HDR_SZ >= GDBM_AVAIL_ELEM_SZ) {
            return Err(Error::new(ErrorKind::Other, "bad header: blksz"));
        }

        if next_block < file_sz {
            return Err(Error::new(ErrorKind::Other, "needs recovery"));
        }

        if !(dir_ofs > 0 && dir_ofs < file_sz && dir_sz > 0 && dir_ofs + (dir_sz as u64) < file_sz)
        {
            return Err(Error::new(ErrorKind::Other, "bad header: dir"));
        }

        let (ck_dir_sz, _ck_dir_bits) = build_dir_size(block_sz);
        if dir_sz < ck_dir_sz {
            return Err(Error::new(ErrorKind::Other, "bad header: dir sz"));
        }

        let (_ck_dir_sz, ck_dir_bits) = build_dir_size(dir_sz);
        if dir_bits != ck_dir_bits {
            return Err(Error::new(ErrorKind::Other, "bad header: dir bits"));
        }

        if bucket_sz <= GDBM_HASH_BUCKET_SZ {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket sz"));
        }

        if bucket_elems != bucket_count(bucket_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket elem"));
        }

        if avail
            .elems
            .iter()
            .any(|elem| elem.addr < bucket_sz as u64 || elem.addr + elem.sz as u64 > next_block)
        {
            return Err(Error::new(ErrorKind::Other, "bad header: avail el"));
        }

        if ((block_sz - GDBM_HDR_SZ) / GDBM_AVAIL_ELEM_SZ + 1) != avail.sz {
            return Err(Error::new(ErrorKind::Other, "bad header: avail sz"));
        }

        if !(avail.sz > 1 && avail.elems.len() as u32 <= avail.sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: avail sz/ct"));
        }

        println!("magname {}", magic);

        Ok(Header {
            magic,
            block_sz,
            dir_ofs,
            dir_sz,
            dir_bits,
            bucket_sz,
            bucket_elems,
            next_block,
            avail,
            dirty: false,
        })
    }

    pub fn serialize(&self, writer: &mut impl Write) -> io::Result<()> {
        writer.write_all(self.magic.as_bytes())?;

        write32(self.endian(), writer, self.block_sz)?;

        match self.alignment() {
            Alignment::Align32 => write32(self.endian(), writer, self.dir_ofs as u32)?,
            Alignment::Align64 => write64(self.endian(), writer, self.dir_ofs)?,
        }

        write32(self.endian(), writer, self.dir_sz)?;
        write32(self.endian(), writer, self.dir_bits)?;
        write32(self.endian(), writer, self.bucket_sz)?;
        write32(self.endian(), writer, self.bucket_elems)?;

        match self.alignment() {
            Alignment::Align32 => write32(self.endian(), writer, self.next_block as u32)?,
            Alignment::Align64 => write64(self.endian(), writer, self.next_block)?,
        }

        self.avail
            .serialize(self.alignment(), self.endian(), writer)?;

        Ok(())
    }

    pub fn endian(&self) -> Endian {
        self.magic.endian()
    }

    pub fn alignment(&self) -> Alignment {
        self.magic.alignment()
    }
}
