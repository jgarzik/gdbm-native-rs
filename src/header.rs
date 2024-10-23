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

use crate::avail::{AvailBlock, AvailElem};
use crate::bucket::{Bucket, BucketElement};
use crate::dir::build_dir_size;
use crate::magic::Magic;
use crate::ser::{read32, read64, write32, write64, Alignment, Endian, Layout, Offset};

#[derive(Debug)]
pub struct Header {
    // on-disk gdbm database file header
    pub magic: Magic,
    pub block_sz: u32,
    pub dir_ofs: u64,
    pub dir_sz: u32,
    pub dir_bits: u32,
    pub bucket_sz: u32,
    pub bucket_elems: u32,
    pub next_block: u64,
    numsync: Option<u32>,

    pub avail: AvailBlock,

    // following fields are calculated, not stored
    pub dirty: bool,
    pub layout: Layout,
}

impl Header {
    pub fn sizeof(layout: &Layout, is_numsync: bool, avail_elems: u32) -> u32 {
        match (layout.offset, is_numsync) {
            (Offset::Small, true) => 32 + 32 + AvailBlock::sizeof(layout, avail_elems),
            (Offset::Small, false) => 32 + AvailBlock::sizeof(layout, avail_elems),
            (Offset::LFS, true) => 40 + 32 + AvailBlock::sizeof(layout, avail_elems),
            (Offset::LFS, false) => 40 + AvailBlock::sizeof(layout, avail_elems),
        }
    }

    pub fn new(block_size: u32, layout: &Layout, dir_bits: u32, numsync: bool) -> Self {
        let bucket_elems = (block_size - Bucket::sizeof(layout)) / BucketElement::sizeof(layout);
        let avail_elems =
            (block_size - Self::sizeof(layout, numsync, 0)) / AvailElem::sizeof(layout);
        Header {
            magic: Magic::new(layout.endian, layout.offset, numsync),
            block_sz: block_size,
            dir_ofs: block_size as u64,
            dir_sz: block_size,
            dir_bits,
            bucket_sz: Bucket::sizeof(layout) + bucket_elems * BucketElement::sizeof(layout),
            bucket_elems,
            next_block: block_size as u64 * 3,
            avail: AvailBlock::new(avail_elems, 0, vec![]),
            dirty: true,
            layout: *layout,
            numsync: None,
        }
    }

    pub fn from_reader(
        alignment: &Option<Alignment>,
        length: u64,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        let file_sz = length;

        let magic = Magic::from_reader(reader)?;
        let block_sz = read32(magic.endian(), reader)?;
        let dir_ofs = match magic.offset() {
            Offset::Small => read32(magic.endian(), reader)? as u64,
            Offset::LFS => read64(magic.endian(), reader)?,
        };
        let dir_sz = read32(magic.endian(), reader)?;
        let dir_bits = read32(magic.endian(), reader)?;
        let bucket_sz = read32(magic.endian(), reader)?;
        let bucket_elems = read32(magic.endian(), reader)?;
        let next_block = match magic.offset() {
            Offset::Small => read32(magic.endian(), reader)? as u64,
            Offset::LFS => read64(magic.endian(), reader)?,
        };
        let numsync = magic
            .is_numsync()
            .then(|| read_numsync(magic.endian(), reader))
            .transpose()?;

        let layout = Layout {
            offset: magic.offset(),
            endian: magic.endian(),
            alignment: alignment.unwrap_or(magic.default_alignment()),
        };

        let avail = AvailBlock::from_reader(&layout, reader)?;

        // Block must be big enough for header and avail table with two elements.
        if block_sz < Self::sizeof(&layout, magic.is_numsync(), 2) {
            return Err(Error::new(ErrorKind::Other, "bad header: blksz"));
        }

        if next_block < file_sz {
            return Err(Error::new(ErrorKind::Other, "needs recovery"));
        }

        if !(dir_ofs > 0 && dir_ofs < file_sz && dir_sz > 0 && dir_ofs + (dir_sz as u64) < file_sz)
        {
            return Err(Error::new(ErrorKind::Other, "bad header: dir"));
        }

        let (ck_dir_sz, _ck_dir_bits) = build_dir_size(layout.offset, block_sz);
        if dir_sz < ck_dir_sz {
            return Err(Error::new(ErrorKind::Other, "bad header: dir sz"));
        }

        let (_ck_dir_sz, ck_dir_bits) = build_dir_size(layout.offset, dir_sz);
        if dir_bits != ck_dir_bits {
            return Err(Error::new(ErrorKind::Other, "bad header: dir bits"));
        }

        if bucket_sz <= Bucket::sizeof(&layout) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket sz"));
        }

        if bucket_elems != (bucket_sz - Bucket::sizeof(&layout)) / BucketElement::sizeof(&layout) {
            return Err(Error::new(ErrorKind::Other, "bad header: bucket elem"));
        }

        if avail
            .elems
            .iter()
            .any(|elem| elem.addr < bucket_sz as u64 || elem.addr + elem.sz as u64 > next_block)
        {
            return Err(Error::new(ErrorKind::Other, "bad header: avail el"));
        }

        if block_sz < Self::sizeof(&layout, magic.is_numsync(), avail.sz) {
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
            layout,
            numsync,
        })
    }

    pub fn serialize(&self, layout: &Layout, writer: &mut impl Write) -> io::Result<()> {
        writer.write_all(self.magic.as_bytes())?;

        write32(layout.endian, writer, self.block_sz)?;

        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.dir_ofs as u32)?,
            Offset::LFS => write64(layout.endian, writer, self.dir_ofs)?,
        }

        write32(layout.endian, writer, self.dir_sz)?;
        write32(layout.endian, writer, self.dir_bits)?;
        write32(layout.endian, writer, self.bucket_sz)?;
        write32(layout.endian, writer, self.bucket_elems)?;

        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.next_block as u32)?,
            Offset::LFS => write64(layout.endian, writer, self.next_block)?,
        }

        if self.magic.is_numsync() {
            write_numsync(layout.endian, writer, self.numsync.unwrap_or(0))?
        }

        self.avail.serialize(layout, writer)?;

        Ok(())
    }

    pub fn increment_numsync(&mut self) {
        if self.magic.is_numsync() {
            self.numsync = match self.numsync {
                Some(n) => Some(n + 1),
                None => Some(0),
            };

            self.dirty = true;
        }
    }

    // convert_numsync converts the header to numsync and retuns a list of
    // offset/length pairs that need to be freed (because avail is shortened).
    pub fn convert_numsync(&mut self, use_numsync: bool) -> io::Result<Vec<(u64, u32)>> {
        let new_avail_sz = (self.block_sz - Self::sizeof(&self.layout, use_numsync, 0))
            / AvailElem::sizeof(&self.layout);

        (new_avail_sz > 1)
            .then(|| {
                self.magic = Magic::new(self.magic.endian(), self.magic.offset(), use_numsync);
                self.numsync = None;
                self.dirty = true;
                self.avail.resize(new_avail_sz)
            })
            .ok_or_else(|| Error::new(ErrorKind::Other, "blocksize too small for numsync"))
    }

    pub fn allocate(&mut self, size: u32) -> Option<(u64, u32)> {
        self.avail.remove_elem(size).inspect(|_| self.dirty = true)
    }

    pub fn free(&mut self, offset: u64, length: u32) {
        self.avail.insert_elem(offset, length);
        self.dirty = true;
    }
}

fn read_numsync(endian: Endian, reader: &mut impl Read) -> io::Result<u32> {
    (0..8)
        .map(|_| read32(endian, reader))
        .collect::<io::Result<Vec<_>>>()
        .and_then(|ext| match ext[0] {
            0 => Ok(ext[1]),
            v => {
                let s = format!("bad header: unsupported extended header version: {}", v);
                Err(Error::new(ErrorKind::Other, s))
            }
        })
}

fn write_numsync(endian: Endian, writer: &mut impl Write, numsync: u32) -> io::Result<()> {
    write32(endian, writer, 0)?;
    write32(endian, writer, numsync)?;
    write64(endian, writer, 0)?;
    write64(endian, writer, 0)?;
    write64(endian, writer, 0)?;

    Ok(())
}
