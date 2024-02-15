//
// header.rs -- GDBM global file header routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use byteorder::{BigEndian, LittleEndian, NativeEndian, ReadBytesExt};
use std::io::{self, Error, ErrorKind, Read};

use crate::dir::build_dir_size;
use crate::ser::{w32, woff_t};
use crate::{
    AvailBlock, AvailElem, GDBM_AVAIL_ELEM_SZ, GDBM_BUCKET_ELEM_SZ, GDBM_HASH_BUCKET_SZ,
    GDBM_HDR_SZ, GDBM_MAGIC32, GDBM_MAGIC32_SWAP, GDBM_MAGIC64, GDBM_MAGIC64_SWAP, GDBM_OMAGIC,
    GDBM_OMAGIC_SWAP,
};

#[derive(Debug)]
pub struct Header {
    // on-disk gdbm database file header
    pub magic: u32,
    pub block_sz: u32,
    pub dir_ofs: u64,
    pub dir_sz: u32,
    pub dir_bits: u32,
    pub bucket_sz: u32,
    pub bucket_elems: u32,
    pub next_block: u64,

    pub avail: AvailBlock,

    // following fields are calculated, not stored
    pub is_lfs: bool, // using 64-bit off_t?
    pub is_le: bool,  // metadata endianness is big (false) or little (true)
    pub dirty: bool,
}

pub fn bucket_count(bucket_sz: u32) -> u32 {
    (bucket_sz - GDBM_HASH_BUCKET_SZ) / GDBM_BUCKET_ELEM_SZ + 1
}

impl Header {
    pub fn from_reader(metadata: &std::fs::Metadata, mut rdr: impl Read) -> io::Result<Self> {
        let file_sz = metadata.len();

        let magic = rdr.read_u32::<NativeEndian>()?;

        // determine db file version, intrinsics from magic number
        let (is_lfs, need_swap) = match magic {
            GDBM_OMAGIC => (false, false),
            GDBM_OMAGIC_SWAP => (false, true),
            GDBM_MAGIC32 => (false, false),
            GDBM_MAGIC32_SWAP => (false, true),
            GDBM_MAGIC64 => (true, false),
            GDBM_MAGIC64_SWAP => (true, true),
            _ => {
                return Err(Error::new(ErrorKind::Other, "Unknown/invalid magic number"));
            }
        };

        // detect db file endianness
        let is_le = match need_swap {
            true => {
                if cfg!(target_endian = "little") {
                    false
                } else {
                    true
                }
            }
            false => {
                if cfg!(target_endian = "little") {
                    true
                } else {
                    false
                }
            }
        };

        // fixme: read u32, not u64, if is_lfs

        let (
            block_sz,
            dir_ofs,
            dir_sz,
            dir_bits,
            bucket_sz,
            bucket_elems,
            next_block,
            avail_sz,
            avail_count,
            avail_next_block,
        );

        if is_le {
            block_sz = rdr.read_u32::<LittleEndian>()?;
            dir_ofs = rdr.read_u64::<LittleEndian>()?;
            dir_sz = rdr.read_u32::<LittleEndian>()?;
            dir_bits = rdr.read_u32::<LittleEndian>()?;
            bucket_sz = rdr.read_u32::<LittleEndian>()?;
            bucket_elems = rdr.read_u32::<LittleEndian>()?;
            next_block = rdr.read_u64::<LittleEndian>()?;

            avail_sz = rdr.read_u32::<LittleEndian>()?;
            avail_count = rdr.read_u32::<LittleEndian>()?;
            avail_next_block = rdr.read_u64::<LittleEndian>()?;
        } else {
            block_sz = rdr.read_u32::<BigEndian>()?;
            dir_ofs = rdr.read_u64::<BigEndian>()?;
            dir_sz = rdr.read_u32::<BigEndian>()?;
            dir_bits = rdr.read_u32::<BigEndian>()?;
            bucket_sz = rdr.read_u32::<BigEndian>()?;
            bucket_elems = rdr.read_u32::<BigEndian>()?;
            next_block = rdr.read_u64::<BigEndian>()?;

            avail_sz = rdr.read_u32::<BigEndian>()?;
            avail_count = rdr.read_u32::<BigEndian>()?;
            avail_next_block = rdr.read_u64::<BigEndian>()?;
        }

        if !(block_sz > 0 && block_sz > GDBM_HDR_SZ && block_sz - GDBM_HDR_SZ >= GDBM_AVAIL_ELEM_SZ)
        {
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
        if !(dir_sz >= ck_dir_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: dir sz"));
        }

        let (_ck_dir_sz, ck_dir_bits) = build_dir_size(dir_sz);
        if dir_bits != ck_dir_bits {
            return Err(Error::new(ErrorKind::Other, "bad header: dir bits"));
        }

        if !(bucket_sz > GDBM_HASH_BUCKET_SZ) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket sz"));
        }

        if bucket_elems != bucket_count(bucket_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket elem"));
        }

        if ((block_sz - GDBM_HDR_SZ) / GDBM_AVAIL_ELEM_SZ + 1) != avail_sz {
            return Err(Error::new(ErrorKind::Other, "bad header: avail sz"));
        }

        if !(avail_sz > 1 && avail_count <= avail_sz) {
            return Err(Error::new(ErrorKind::Other, "bad header: avail sz/ct"));
        }

        let mut elems: Vec<AvailElem> = Vec::new();
        for _idx in 0..avail_count {
            let av_elem = AvailElem::from_reader(is_lfs, is_le, &mut rdr)?;
            elems.push(av_elem);
        }

        // maintain intrinsic: avail is always sorted by size
        elems.sort();

        // todo: check for overlapping segments

        for elem in elems.iter() {
            if !(elem.addr >= bucket_sz.into() && elem.addr + (elem.sz as u64) <= next_block) {
                return Err(Error::new(ErrorKind::Other, "bad header: avail el"));
            }
        }

        let magname = match magic {
            GDBM_OMAGIC => "GDBM_OMAGIC",
            GDBM_MAGIC32 => "GDBM_MAGIC32",
            GDBM_MAGIC64 => "GDBM_MAGIC64",
            GDBM_OMAGIC_SWAP => "GDBM_OMAGIC_SWAP",
            GDBM_MAGIC32_SWAP => "GDBM_MAGIC32_SWAP",
            GDBM_MAGIC64_SWAP => "GDBM_MAGIC64_SWAP",
            _ => "?",
        };
        println!("magname {}", magname);

        Ok(Header {
            magic,
            block_sz,
            dir_ofs,
            dir_sz,
            dir_bits,
            bucket_sz,
            bucket_elems,
            next_block,
            avail: AvailBlock {
                sz: avail_sz,
                count: avail_count,
                next_block: avail_next_block,
                elems,
            },
            is_lfs,
            is_le,
            dirty: false,
        })
    }

    pub fn serialize(&self, is_lfs: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.magic)); // fixme: check swap?
        buf.append(&mut w32(is_le, self.block_sz));
        buf.append(&mut woff_t(is_lfs, is_le, self.dir_ofs));
        buf.append(&mut w32(is_le, self.dir_sz));
        buf.append(&mut w32(is_le, self.dir_bits));
        buf.append(&mut w32(is_le, self.bucket_sz));
        buf.append(&mut w32(is_le, self.bucket_elems));
        buf.append(&mut woff_t(is_lfs, is_le, self.next_block));
        buf.append(&mut self.avail.serialize(is_lfs, is_le));

        buf
    }
}
