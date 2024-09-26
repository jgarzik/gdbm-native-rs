//
// avail.rs -- GDBM avail list routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::{self, Read};

use crate::ser::{w32, woff_t, Alignment, Endian};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct AvailElem {
    pub sz: u32,
    pub addr: u64,
}

impl AvailElem {
    pub fn from_reader(
        alignment: Alignment,
        endian: Endian,
        rdr: &mut impl Read,
    ) -> io::Result<Self> {
        let elem_sz: u32;
        let elem_ofs: u64;

        if endian == Endian::Little {
            elem_sz = rdr.read_u32::<LittleEndian>()?;
            if alignment == Alignment::Align64 {
                let _padding = rdr.read_u32::<LittleEndian>()?;
                elem_ofs = rdr.read_u64::<LittleEndian>()?;
            } else {
                elem_ofs = rdr.read_u32::<LittleEndian>()? as u64;
            }
        } else {
            elem_sz = rdr.read_u32::<BigEndian>()?;
            if alignment == Alignment::Align64 {
                let _padding = rdr.read_u32::<BigEndian>()?;
                elem_ofs = rdr.read_u64::<BigEndian>()?;
            } else {
                elem_ofs = rdr.read_u32::<BigEndian>()? as u64;
            }
        }

        Ok(AvailElem {
            sz: elem_sz,
            addr: elem_ofs,
        })
    }

    pub fn serialize(&self, alignment: Alignment, endian: Endian) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(endian, self.sz));
        if alignment == Alignment::Align64 {
            let padding: u32 = 0;
            buf.append(&mut w32(endian, padding));
        }
        buf.append(&mut woff_t(alignment, endian, self.addr));

        buf
    }
}

#[derive(Debug)]
pub struct AvailBlock {
    pub sz: u32,
    pub count: u32,
    pub next_block: u64,
    pub elems: Vec<AvailElem>,
}

impl AvailBlock {
    pub fn new(sz: u32) -> AvailBlock {
        AvailBlock {
            sz,
            count: 0,
            next_block: 0,
            elems: Vec::new(),
        }
    }

    fn find_elem(&self, sz: usize) -> Option<usize> {
        self.elems.iter().position(|elem| elem.sz as usize >= sz)
    }

    pub fn remove_elem(&mut self, sz: usize) -> Option<AvailElem> {
        assert!((self.count as usize) == self.elems.len());
        match self.find_elem(sz) {
            None => None,
            Some(idx) => {
                self.count -= 1;
                Some(self.elems.remove(idx))
            }
        }
    }

    pub fn serialize(&self, alignment: Alignment, endian: Endian) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(endian, self.sz));
        buf.append(&mut w32(endian, self.count));
        buf.append(&mut woff_t(alignment, endian, self.next_block));

        for elem in &self.elems {
            buf.append(&mut elem.serialize(alignment, endian));
        }

        buf
    }
}
