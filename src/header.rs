//
// header.rs -- GDBM global file header routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Read, Write};

use crate::avail::{AvailBlock, AvailElem};
use crate::bucket::{Bucket, BucketElement};
use crate::dir::build_dir_size;
use crate::magic::Magic;
use crate::ser::{read32, read64, write32, write64, Alignment, Endian, Layout, Offset};
use crate::{Error, Result};

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
    pub numsync: Option<u32>,

    pub avail: AvailBlock,

    // following fields are calculated, not stored
    pub dirty: bool,
    pub layout: Layout,
}

impl Header {
    pub fn sizeof(layout: Layout, is_numsync: bool, avail_elems: u32) -> u32 {
        match (layout.offset, is_numsync) {
            (Offset::Small, true) => 32 + 32 + AvailBlock::sizeof(layout, avail_elems),
            (Offset::Small, false) => 32 + AvailBlock::sizeof(layout, avail_elems),
            (Offset::LFS, true) => 40 + 32 + AvailBlock::sizeof(layout, avail_elems),
            (Offset::LFS, false) => 40 + AvailBlock::sizeof(layout, avail_elems),
        }
    }

    pub fn new(block_size: u32, layout: Layout, dir_bits: u32, numsync: bool) -> Self {
        let bucket_elems = (block_size - Bucket::sizeof(layout)) / BucketElement::sizeof(layout);
        let avail_elems =
            (block_size - Self::sizeof(layout, numsync, 0)) / AvailElem::sizeof(layout);
        Header {
            magic: Magic::new(layout.endian, layout.offset, numsync),
            block_sz: block_size,
            dir_ofs: u64::from(block_size),
            dir_sz: block_size,
            dir_bits,
            bucket_sz: Bucket::sizeof(layout) + bucket_elems * BucketElement::sizeof(layout),
            bucket_elems,
            next_block: u64::from(block_size) * 3,
            avail: AvailBlock::new(avail_elems, 0, vec![]),
            dirty: true,
            layout,
            numsync: None,
        }
    }

    pub fn from_reader(
        alignment: Option<Alignment>,
        file_size: u64,
        reader: &mut impl Read,
    ) -> Result<Self> {
        let magic = Magic::from_reader(reader)?;
        let block_sz = read32(magic.endian(), reader)?;
        let dir_ofs = match magic.offset() {
            Offset::Small => u64::from(read32(magic.endian(), reader)?),
            Offset::LFS => read64(magic.endian(), reader)?,
        };
        let dir_sz = read32(magic.endian(), reader)?;
        let dir_bits = read32(magic.endian(), reader)?;
        let bucket_sz = read32(magic.endian(), reader)?;
        let bucket_elems = read32(magic.endian(), reader)?;
        let next_block = match magic.offset() {
            Offset::Small => u64::from(read32(magic.endian(), reader)?),
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

        let avail = AvailBlock::from_reader(layout, reader)?;

        let header = Header {
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
        };

        header.verify(file_size)?;

        Ok(header)
    }

    fn verify(&self, file_size: u64) -> Result<()> {
        // Block must be big enough for header and avail table with two elements.
        if self.block_sz < Self::sizeof(self.layout, self.magic.is_numsync(), 2) {
            return Err(Error::BadHeaderBlockSize {
                size: self.block_sz,
                minimum: Self::sizeof(self.layout, self.magic.is_numsync(), 2),
            });
        }

        if self.next_block < file_size {
            return Err(Error::BadHeaderNextBlock {
                next_block: self.next_block,
                file_size,
            });
        }

        if self.dir_ofs + u64::from(self.dir_sz) > file_size {
            return Err(Error::BadHeaderDirectoryOffset {
                offset: self.dir_ofs,
                size: self.dir_sz,
                file_size,
            });
        }

        let (minimum_size, _) = build_dir_size(self.layout.offset, self.block_sz);
        let (_, expected_bits) = build_dir_size(self.layout.offset, self.dir_sz);
        if self.dir_sz < minimum_size || self.dir_bits != expected_bits {
            return Err(Error::BadHeaderDirectory {
                size: self.dir_sz,
                bits: self.dir_bits,
                minimum_size,
                expected_bits,
            });
        }

        if self.bucket_sz < Bucket::sizeof(self.layout) + BucketElement::sizeof(self.layout) {
            return Err(Error::BadHeaderBucketSize {
                size: self.bucket_sz,
                minimum: Bucket::sizeof(self.layout) + BucketElement::sizeof(self.layout),
            });
        }

        if self.bucket_elems
            != (self.bucket_sz - Bucket::sizeof(self.layout)) / BucketElement::sizeof(self.layout)
        {
            return Err(Error::BadHeaderBucketElems {
                elems: self.bucket_elems,
                expected: (self.bucket_sz - Bucket::sizeof(self.layout))
                    / BucketElement::sizeof(self.layout),
            });
        }

        self.avail
            .elems
            .iter()
            .enumerate()
            .try_for_each(|(i, elem)| {
                (elem.addr >= u64::from(self.block_sz)
                    && elem.addr + u64::from(elem.sz) <= file_size)
                    .then_some(())
                    .ok_or_else(|| Error::BadAvailElem {
                        block_offset: u64::from(Self::sizeof(
                            self.layout,
                            self.magic.is_numsync(),
                            0,
                        )),
                        elem: i,
                        offset: elem.addr,
                        size: elem.sz,
                        file_size,
                    })
            })?;

        if self.avail.sz == 0
            || self.block_sz < Self::sizeof(self.layout, self.magic.is_numsync(), self.avail.sz)
        {
            return Err(Error::BadHeaderAvail {
                elems: self.avail.sz,
                size: Self::sizeof(self.layout, self.magic.is_numsync(), self.avail.sz),
                block_size: self.block_sz,
            });
        }

        if self.avail.elems.len() as u32 > self.avail.sz {
            return Err(Error::BadHeaderAvailCount {
                elems: self.avail.elems.len() as u32,
                maximum: self.avail.sz,
            });
        }

        Ok(())
    }

    pub fn serialize(&self, writer: &mut impl Write) -> io::Result<()> {
        let layout = self.layout;

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
            write_numsync(layout.endian, writer, self.numsync.unwrap_or(0))?;
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
    pub fn convert_numsync(&mut self, use_numsync: bool) -> Vec<(u64, u32)> {
        let new_avail_sz = (self.block_sz - Self::sizeof(self.layout, use_numsync, 0))
            / AvailElem::sizeof(self.layout);

        self.magic = Magic::new(self.magic.endian(), self.magic.offset(), use_numsync);
        self.numsync = None;
        self.dirty = true;
        self.avail.resize(new_avail_sz)
    }

    pub fn allocate(&mut self, size: u32) -> Option<(u64, u32)> {
        self.avail.remove_elem(size).inspect(|_| self.dirty = true)
    }

    pub fn free(&mut self, offset: u64, length: u32) {
        self.avail.insert_elem(offset, length);
        self.dirty = true;
    }
}

fn read_numsync(endian: Endian, reader: &mut impl Read) -> Result<u32> {
    (0..8)
        .map(|_| read32(endian, reader).map_err(Error::Io))
        .collect::<Result<Vec<_>>>()
        .and_then(|ext| match ext.first().copied().unwrap() {
            0 => Ok(ext.get(1).copied().unwrap()),
            v => Err(Error::BadNumsyncVersion { version: v }),
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
