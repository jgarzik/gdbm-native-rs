//
// avail.rs -- GDBM avail list routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Read, Write};

use crate::ser::{read32, read64, write32, write64, Alignment, Endian};

#[derive(Default, Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct AvailElem {
    pub sz: u32,
    pub addr: u64,
}

impl AvailElem {
    pub fn from_reader(
        alignment: Alignment,
        endian: Endian,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        let elem_sz = read32(endian, reader)?;

        // skip padding
        if alignment.is64() {
            read32(endian, reader)?;
        }

        let elem_ofs = match alignment {
            Alignment::Align32 => (read32(endian, reader)?) as u64,
            Alignment::Align64 => read64(endian, reader)?,
        };

        Ok(AvailElem {
            sz: elem_sz,
            addr: elem_ofs,
        })
    }

    pub fn serialize(
        &self,
        alignment: Alignment,
        endian: Endian,
        writer: &mut impl Write,
    ) -> io::Result<()> {
        write32(endian, writer, self.sz)?;

        // insert padding
        if alignment.is64() {
            write32(endian, writer, 0)?;
        }

        match alignment {
            Alignment::Align32 => write32(endian, writer, self.addr as u32)?,
            Alignment::Align64 => write64(endian, writer, self.addr)?,
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct AvailBlock {
    pub sz: u32,
    pub next_block: u64,
    pub elems: Vec<AvailElem>,
}

impl AvailBlock {
    pub fn new(sz: u32) -> AvailBlock {
        AvailBlock {
            sz,
            next_block: 0,
            elems: Vec::new(),
        }
    }

    fn find_elem(&self, sz: usize) -> Option<usize> {
        self.elems.iter().position(|elem| elem.sz as usize >= sz)
    }

    pub fn remove_elem(&mut self, sz: usize) -> Option<AvailElem> {
        match self.find_elem(sz) {
            None => None,
            Some(idx) => Some(self.elems.remove(idx)),
        }
    }

    pub fn serialize(
        &self,
        alignment: Alignment,
        endian: Endian,
        writer: &mut impl Write,
    ) -> io::Result<()> {
        write32(endian, writer, self.sz)?;
        write32(endian, writer, self.elems.len() as u32)?;
        match alignment {
            Alignment::Align32 => write32(endian, writer, self.next_block as u32)?,
            Alignment::Align64 => write64(endian, writer, self.next_block)?,
        }

        self.elems
            .iter()
            .try_for_each(|elem| elem.serialize(alignment, endian, writer))?;

        Ok(())
    }
}
