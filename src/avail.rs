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

    pub fn from_reader(
        alignment: Alignment,
        endian: Endian,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        let sz = read32(endian, reader)?;
        let count = read32(endian, reader)?;

        let next_block = match alignment {
            Alignment::Align32 => (read32(endian, reader)?) as u64,
            Alignment::Align64 => read64(endian, reader)?,
        };

        let mut elems = (0..count)
            .map(|_| AvailElem::from_reader(alignment, endian, reader))
            .collect::<io::Result<Vec<_>>>()?;

        // maintain intrinsic: avail is always sorted by size
        elems.sort();

        // todo: check for overlapping segments

        Ok(Self {
            sz,
            next_block,
            elems,
        })
    }

    pub fn remove_elem(&mut self, sz: u32) -> Option<AvailElem> {
        remove_elem(&mut self.elems, sz)
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

pub fn remove_elem(elems: &mut Vec<AvailElem>, size: u32) -> Option<AvailElem> {
    elems
        .iter()
        .position(|elem| elem.sz >= size)
        .map(|index| elems.remove(index))
}

#[cfg(test)]
mod tests {
    use super::{remove_elem, AvailElem};

    #[test]
    fn remove_elem_found() {
        let mut elems = vec![
            AvailElem { addr: 1000, sz: 1 },
            AvailElem { addr: 2000, sz: 2 },
            AvailElem { addr: 3000, sz: 3 },
        ];

        assert_eq!(
            remove_elem(&mut elems, 2),
            Some(AvailElem { addr: 2000, sz: 2 })
        );

        assert_eq!(
            elems,
            vec![
                AvailElem { addr: 1000, sz: 1 },
                AvailElem { addr: 3000, sz: 3 },
            ]
        );
    }

    #[test]
    fn remove_elem_not_found() {
        let mut elems = vec![
            AvailElem { addr: 1000, sz: 1 },
            AvailElem { addr: 2000, sz: 2 },
            AvailElem { addr: 3000, sz: 3 },
        ];

        assert_eq!(remove_elem(&mut elems, 4), None);

        assert_eq!(
            elems,
            vec![
                AvailElem { addr: 1000, sz: 1 },
                AvailElem { addr: 2000, sz: 2 },
                AvailElem { addr: 3000, sz: 3 },
            ]
        );
    }

    #[test]
    fn remove_elem_empty() {
        let mut elems = vec![];

        assert_eq!(remove_elem(&mut elems, 4), None);

        assert_eq!(elems, vec![]);
    }
}
