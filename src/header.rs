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
use crate::ser::{Endian, Format, Layout, Offset, read32, read64, write32, write64};
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
    pub layout: Format,
}

impl Header {
    pub fn sizeof(layout: Format, is_numsync: bool, avail_elems: u32) -> u32 {
        let fixed = match (layout.offset, is_numsync) {
            (Offset::Small, true) => 32 + 32,
            (Offset::Small, false) => 32,
            (Offset::Lfs, true) => 40 + 32,
            (Offset::Lfs, false) => 40,
        };
        AvailBlock::sizeof(layout, avail_elems).saturating_add(fixed)
    }

    pub fn new(block_size: u32, layout: Format, dir_bits: u32, numsync: bool) -> Self {
        let bucket_elems = (block_size - Bucket::sizeof(layout)) / BucketElement::sizeof(layout);
        let avail_elems = avail_capacity(layout, numsync, block_size);
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
            // like C GDBM, which zero-fills a new header
            numsync: numsync.then_some(0),
        }
    }

    pub fn from_reader(file_size: u64, reader: &mut impl Read) -> Result<Self> {
        let magic = Magic::from_reader(reader)?;
        let block_sz = read32(magic.endian(), reader)?;
        let dir_ofs = match magic.offset() {
            Offset::Small => u64::from(read32(magic.endian(), reader)?),
            Offset::Lfs => read64(magic.endian(), reader)?,
        };
        let dir_sz = read32(magic.endian(), reader)?;
        let dir_bits = read32(magic.endian(), reader)?;
        let bucket_sz = read32(magic.endian(), reader)?;
        let bucket_elems = read32(magic.endian(), reader)?;
        let next_block = match magic.offset() {
            Offset::Small => u64::from(read32(magic.endian(), reader)?),
            Offset::Lfs => read64(magic.endian(), reader)?,
        };
        let numsync = magic
            .is_numsync()
            .then(|| read_numsync(magic.endian(), reader))
            .transpose()?;

        // The avail table starts at the same offset whatever the alignment, so its capacity can
        // be read before the alignment is known.
        let avail_sz = read32(magic.endian(), reader)?;
        let layout = detect_format(magic, block_sz, bucket_sz, bucket_elems, avail_sz);

        let mut header = Header {
            magic,
            block_sz,
            dir_ofs,
            dir_sz,
            dir_bits,
            bucket_sz,
            bucket_elems,
            next_block,
            avail: AvailBlock::new(avail_sz, 0, vec![]),
            dirty: false,
            layout,
            numsync,
        };

        // Verify the fixed fields first: the avail table capacity then fits the header block,
        // which bounds the element count accepted below.
        header.verify(file_size)?;

        let avail_offset = header.avail_offset();
        header.avail =
            AvailBlock::from_reader_sized(layout, avail_sz, avail_offset, avail_sz, reader)?;

        // Free space may lie beyond the physical end of file, up to next_block.
        header.avail.validate(
            avail_offset,
            avail_sz,
            u64::from(header.block_sz),
            header.next_block,
        )?;

        Ok(header)
    }

    // Verify the fixed fields; from_reader() reads and validates the avail table afterwards.
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

        if self
            .dir_ofs
            .checked_add(u64::from(self.dir_sz))
            .is_none_or(|end| end > file_size)
        {
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

        // Like C GDBM, require the avail table to fill the rest of the header block exactly. This
        // also rejects a header whose layout detect_format() could not determine.
        if self.avail.sz != avail_capacity(self.layout, self.magic.is_numsync(), self.block_sz) {
            return Err(Error::BadHeaderAvail {
                elems: self.avail.sz,
                size: Self::sizeof(self.layout, self.magic.is_numsync(), self.avail.sz),
                block_size: self.block_sz,
            });
        }

        Ok(())
    }

    // file offset of the avail table within the header
    fn avail_offset(&self) -> u64 {
        u64::from(
            Self::sizeof(self.layout, self.magic.is_numsync(), 0)
                - AvailBlock::sizeof(self.layout, 0),
        )
    }

    pub fn serialize(&self, writer: &mut impl Write) -> io::Result<()> {
        let layout = self.layout;

        writer.write_all(self.magic.as_bytes())?;

        write32(layout.endian, writer, self.block_sz)?;

        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.dir_ofs as u32)?,
            Offset::Lfs => write64(layout.endian, writer, self.dir_ofs)?,
        }

        write32(layout.endian, writer, self.dir_sz)?;
        write32(layout.endian, writer, self.dir_bits)?;
        write32(layout.endian, writer, self.bucket_sz)?;
        write32(layout.endian, writer, self.bucket_elems)?;

        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.next_block as u32)?,
            Offset::Lfs => write64(layout.endian, writer, self.next_block)?,
        }

        if self.magic.is_numsync() {
            write_numsync(layout.endian, writer, self.numsync.unwrap_or(0))?;
        }

        self.avail.serialize(layout, writer)?;

        Ok(())
    }

    pub fn increment_numsync(&mut self) {
        if self.magic.is_numsync() {
            self.numsync = Some(self.numsync.unwrap_or(0).wrapping_add(1));

            self.dirty = true;
        }
    }

    // convert_numsync converts the header to numsync and retuns a list of
    // offset/length pairs that need to be freed (because avail is shortened).
    pub fn convert_numsync(&mut self, use_numsync: bool) -> Vec<(u64, u32)> {
        let new_avail_sz = avail_capacity(self.layout, use_numsync, self.block_sz);

        self.magic = Magic::new(self.magic.endian(), self.magic.offset(), use_numsync);
        self.numsync = use_numsync.then_some(0);
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

// Capacity of the header's avail table: as many elements as fit in the rest of the header block
// (C GDBM's `(block_size - sizeof (gdbm_file_header)) / sizeof (avail_elem) + 1`, its header
// struct already holding one element).
fn avail_capacity(layout: Format, numsync: bool, block_sz: u32) -> u32 {
    block_sz.saturating_sub(Header::sizeof(layout, numsync, 0)) / AvailElem::sizeof(layout)
}

// C GDBM files do not record struct alignment, which differs between platforms only for 64-bit
// offsets (4 bytes on i386, 8 elsewhere). C GDBM requires the header's avail table capacity and
// bucket element count to equal the values computed from the block and bucket sizes, and both
// depend on the alignment, so pick the alignment for which both match. If neither does, assume
// 8-byte alignment; verify() then rejects the header (BadHeaderAvail or BadHeaderBucketElems).
fn detect_format(
    magic: Magic,
    block_sz: u32,
    bucket_sz: u32,
    bucket_elems: u32,
    avail_sz: u32,
) -> Format {
    let candidates = match magic.offset() {
        Offset::Small => vec![Layout::Off32],
        Offset::Lfs => vec![Layout::Off64Align8, Layout::Off64Align4],
    };
    let fits = |format: Format| {
        avail_sz == avail_capacity(format, magic.is_numsync(), block_sz)
            && bucket_elems
                == bucket_sz.saturating_sub(Bucket::sizeof(format)) / BucketElement::sizeof(format)
    };
    let formats = candidates
        .into_iter()
        .map(|layout| Format::new(layout, magic.endian()));
    formats
        .clone()
        .find(|&format| fits(format))
        .unwrap_or_else(|| formats.clone().next().unwrap())
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
