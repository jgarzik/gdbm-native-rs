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

use crate::ser::{read32, read64, write32, write64, Alignment, Layout, Offset};
use crate::{Error, Result};

#[derive(Default, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct AvailElem {
    pub sz: u32,
    pub addr: u64,
}

impl AvailElem {
    pub fn sizeof(layout: Layout) -> u32 {
        match (layout.alignment, layout.offset) {
            (Alignment::Align32, Offset::LFS) => 12,
            (Alignment::Align64, Offset::LFS) => 16,
            _ => 8,
        }
    }

    pub fn from_reader(layout: Layout, reader: &mut impl Read) -> io::Result<Self> {
        let elem_sz = read32(layout.endian, reader)?;

        // skip padding
        if layout.alignment.is64() && layout.offset == Offset::LFS {
            read32(layout.endian, reader)?;
        }

        let elem_ofs = match layout.offset {
            Offset::Small => u64::from(read32(layout.endian, reader)?),
            Offset::LFS => read64(layout.endian, reader)?,
        };

        Ok(AvailElem {
            sz: elem_sz,
            addr: elem_ofs,
        })
    }

    pub fn serialize(&self, layout: Layout, writer: &mut impl Write) -> io::Result<()> {
        write32(layout.endian, writer, self.sz)?;

        // insert padding
        if layout.alignment.is64() && layout.offset == Offset::LFS {
            write32(layout.endian, writer, 0)?;
        }

        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.addr as u32)?,
            Offset::LFS => write64(layout.endian, writer, self.addr)?,
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct AvailBlock {
    pub sz: u32,
    pub next_block: u64,
    pub elems: Vec<AvailElem>,
}

impl AvailBlock {
    pub fn sizeof(layout: Layout, elems: u32) -> u32 {
        elems * AvailElem::sizeof(layout)
            + match (layout.alignment, layout.offset) {
                (Alignment::Align32, Offset::Small) => 12,
                _ => 16,
            }
    }

    pub fn new(sz: u32, next_block: u64, elems: Vec<AvailElem>) -> Self {
        Self {
            sz,
            next_block,
            elems,
        }
    }

    pub fn from_reader(layout: Layout, reader: &mut impl Read) -> io::Result<Self> {
        let sz = read32(layout.endian, reader)?;
        let count = read32(layout.endian, reader)?;

        let next_block = match layout.offset {
            Offset::Small => u64::from(read32(layout.endian, reader)?),
            Offset::LFS => read64(layout.endian, reader)?,
        };

        if layout.alignment.is64() && layout.offset == Offset::Small {
            read32(layout.endian, reader)?;
        }

        let mut elems = (0..count)
            .map(|_| AvailElem::from_reader(layout, reader))
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

    pub fn remove_elem(&mut self, sz: u32) -> Option<(u64, u32)> {
        remove_elem(&mut self.elems, sz)
    }

    pub fn insert_elem(&mut self, offset: u64, length: u32) {
        insert_elem(&mut self.elems, offset, length);
    }

    pub fn serialize(&self, layout: Layout, writer: &mut impl Write) -> io::Result<()> {
        write32(layout.endian, writer, self.sz)?;
        write32(layout.endian, writer, self.elems.len() as u32)?;
        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.next_block as u32)?,
            Offset::LFS => write64(layout.endian, writer, self.next_block)?,
        }

        if layout.alignment.is64() && layout.offset == Offset::Small {
            write32(layout.endian, writer, 0)?;
        }

        self.elems
            .iter()
            .try_for_each(|elem| elem.serialize(layout, writer))?;

        Ok(())
    }

    // resize Self and return a Vec of elements that can no longer be accommodated.
    pub fn resize(&mut self, size: u32) -> Vec<(u64, u32)> {
        self.sz = size;
        self.elems
            .drain(self.elems.len().min(size as usize)..)
            .map(|elem| (elem.addr, elem.sz))
            .collect()
    }

    // Size in bytes of a block pushed onto the avail stack when the header avail table (of
    // `table_sz` elements) overflows: half the table plus the C struct avail_block, whose sizeof
    // includes one table element. Mirrors push_avail_block() in C GDBM's falloc.c.
    pub fn stack_block_size(layout: Layout, table_sz: u32) -> u32 {
        table_sz * AvailElem::sizeof(layout) / 2 + Self::sizeof(layout, 1)
    }

    // Number of elements that fit in a block of stack_block_size() bytes.
    pub fn stack_block_capacity(layout: Layout, table_sz: u32) -> u32 {
        (Self::stack_block_size(layout, table_sz) - Self::sizeof(layout, 0))
            / AvailElem::sizeof(layout)
    }

    pub fn is_full(&self) -> bool {
        self.elems.len() >= self.sz as usize
    }

    // Insert, first coalescing with any adjacent free extents.
    pub fn insert_elem_coalesce(&mut self, offset: u64, length: u32) {
        let (mut offset, mut length) = (offset, length);
        self.elems.retain(|elem| {
            if elem.addr + u64::from(elem.sz) == offset {
                offset = elem.addr;
                length += elem.sz;
                false
            } else if offset + u64::from(length) == elem.addr {
                length += elem.sz;
                false
            } else {
                true
            }
        });
        self.insert_elem(offset, length);
    }

    // Validate a block read from `block_offset`: at most `capacity` elements, each lying within
    // [min_addr, next_block).
    pub fn validate(
        &self,
        block_offset: u64,
        capacity: u32,
        min_addr: u64,
        next_block: u64,
    ) -> Result<()> {
        if self.elems.len() > capacity as usize || self.elems.len() > self.sz as usize {
            return Err(Error::BadAvailCount {
                block_offset,
                elems: self.elems.len() as u32,
                maximum: capacity.min(self.sz),
            });
        }

        self.elems
            .iter()
            .enumerate()
            .try_for_each(|(elem, AvailElem { sz, addr })| {
                (*addr >= min_addr
                    && addr
                        .checked_add(u64::from(*sz))
                        .is_some_and(|end| end <= next_block))
                .then_some(())
                .ok_or(Error::BadAvailElem {
                    block_offset,
                    elem,
                    offset: *addr,
                    size: *sz,
                    next_block,
                })
            })
    }
}

pub fn remove_elem(elems: &mut Vec<AvailElem>, size: u32) -> Option<(u64, u32)> {
    elems
        .iter()
        .position(|elem| elem.sz >= size)
        .map(|index| elems.remove(index))
        .map(|elem| (elem.addr, elem.sz))
}

pub fn insert_elem(elems: &mut Vec<AvailElem>, offset: u64, length: u32) {
    let elem = AvailElem {
        addr: offset,
        sz: length,
    };
    let pos = elems.binary_search(&elem).unwrap_or_else(|e| e);
    elems.insert(pos, elem);
}

pub fn partition_elems(elems: &[AvailElem]) -> (Vec<AvailElem>, Vec<AvailElem>) {
    let one = elems.iter().step_by(2).copied().collect::<Vec<_>>();
    let two = elems.iter().skip(1).step_by(2).copied().collect::<Vec<_>>();

    (one, two)
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

        assert_eq!(remove_elem(&mut elems, 2), Some((2000, 2)));

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

    #[test]
    fn test_partition_elements() {
        use super::AvailElem;
        struct Test<'a> {
            name: &'a str,
            elements: Vec<AvailElem>,
            expected: (Vec<AvailElem>, Vec<AvailElem>),
        }

        for test in [
            Test {
                name: "empty",
                elements: vec![],
                expected: (vec![], vec![]),
            },
            Test {
                name: "one",
                elements: vec![AvailElem { addr: 0, sz: 0 }],
                expected: (vec![AvailElem { addr: 0, sz: 0 }], vec![]),
            },
            Test {
                name: "two",
                elements: vec![AvailElem { addr: 0, sz: 0 }, AvailElem { addr: 1, sz: 1 }],
                expected: (
                    vec![AvailElem { addr: 0, sz: 0 }],
                    vec![AvailElem { addr: 1, sz: 1 }],
                ),
            },
            Test {
                name: "five",
                elements: vec![
                    AvailElem { addr: 0, sz: 0 },
                    AvailElem { addr: 1, sz: 1 },
                    AvailElem { addr: 2, sz: 2 },
                    AvailElem { addr: 3, sz: 3 },
                    AvailElem { addr: 4, sz: 4 },
                ],
                expected: (
                    vec![
                        AvailElem { addr: 0, sz: 0 },
                        AvailElem { addr: 2, sz: 2 },
                        AvailElem { addr: 4, sz: 4 },
                    ],
                    vec![AvailElem { addr: 1, sz: 1 }, AvailElem { addr: 3, sz: 3 }],
                ),
            },
        ] {
            let partitioned = super::partition_elems(&test.elements);
            assert!(
                partitioned == test.expected,
                "test \"{}\" failed: expected:\n{:?}\ngot:\n{partitioned:?}",
                test.name,
                test.expected
            );
        }
    }
}
