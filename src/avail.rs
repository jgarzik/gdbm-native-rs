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

#[derive(Default, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct AvailElem {
    pub sz: u32,
    pub addr: u64,
}

impl AvailElem {
    pub fn sizeof(layout: &Layout) -> u32 {
        match (layout.alignment, layout.offset) {
            (Alignment::Align32, Offset::LFS) => 12,
            (Alignment::Align64, Offset::LFS) => 16,
            _ => 8,
        }
    }

    pub fn from_reader(layout: &Layout, reader: &mut impl Read) -> io::Result<Self> {
        let elem_sz = read32(layout.endian, reader)?;

        // skip padding
        if layout.alignment.is64() {
            read32(layout.endian, reader)?;
        }

        let elem_ofs = match layout.offset {
            Offset::Small => (read32(layout.endian, reader)?) as u64,
            Offset::LFS => read64(layout.endian, reader)?,
        };

        Ok(AvailElem {
            sz: elem_sz,
            addr: elem_ofs,
        })
    }

    pub fn serialize(&self, layout: &Layout, writer: &mut impl Write) -> io::Result<()> {
        write32(layout.endian, writer, self.sz)?;

        // insert padding
        if layout.alignment.is64() {
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
    pub fn sizeof(layout: &Layout, elems: u32) -> u32 {
        elems * AvailElem::sizeof(layout)
            + match layout.alignment {
                Alignment::Align32 => 12,
                Alignment::Align64 => 16,
            }
    }

    pub fn new(sz: u32, next_block: u64, elems: Vec<AvailElem>) -> Self {
        Self {
            sz,
            next_block,
            elems,
        }
    }

    pub fn from_reader(layout: &Layout, reader: &mut impl Read) -> io::Result<Self> {
        let sz = read32(layout.endian, reader)?;
        let count = read32(layout.endian, reader)?;

        let next_block = match layout.offset {
            Offset::Small => (read32(layout.endian, reader)?) as u64,
            Offset::LFS => read64(layout.endian, reader)?,
        };

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

    pub fn remove_elem(&mut self, sz: u32) -> Option<AvailElem> {
        remove_elem(&mut self.elems, sz)
    }

    pub fn serialize(&self, layout: &Layout, writer: &mut impl Write) -> io::Result<()> {
        write32(layout.endian, writer, self.sz)?;
        write32(layout.endian, writer, self.elems.len() as u32)?;
        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.next_block as u32)?,
            Offset::LFS => write64(layout.endian, writer, self.next_block)?,
        }

        self.elems
            .iter()
            .try_for_each(|elem| elem.serialize(layout, writer))?;

        Ok(())
    }

    // Merge elements from self and other and retuen a new AvailableBlock.
    // Retuns Some(block) if combined elements fit in bolck, otherwise None.
    pub fn merge(&self, other: &Self) -> Option<Self> {
        // gather offsets and length from both blocks
        let mut offsets_and_lengths = self
            .elems
            .iter()
            .chain(other.elems.iter())
            .map(|AvailElem { sz, addr }| (*addr, *sz))
            .collect::<Vec<_>>();

        // sort by offsets
        offsets_and_lengths.sort();

        // fold resulting regions whilst joining adjacent regions
        let mut elems = offsets_and_lengths.into_iter().fold(
            Vec::new(),
            |mut elems: Vec<AvailElem>, (addr, sz)| {
                let last = elems.pop();
                match last {
                    None => vec![AvailElem { addr, sz }],
                    Some(last) if last.addr + last.sz as u64 == addr => {
                        vec![AvailElem {
                            addr: last.addr,
                            sz: last.sz + sz,
                        }]
                    }
                    Some(last) => vec![last, AvailElem { addr, sz }],
                }
                .into_iter()
                .for_each(|elem| elems.push(elem));
                elems
            },
        );

        elems.sort();

        (elems.len() as u32 <= self.sz).then_some(AvailBlock {
            sz: self.sz,
            next_block: other.next_block,
            elems,
        })
    }

    // resize Self and return a Vec of elements that can no longer be accommodated.
    pub fn resize(&mut self, size: u32) -> Vec<(u64, u32)> {
        self.sz = size;
        self.elems
            .drain(self.elems.len().min(size as usize)..)
            .map(|elem| (elem.addr, elem.sz))
            .collect()
    }

    // extent returns the size of this block when serialized
    pub fn extent(&self, layout: &Layout) -> u32 {
        Self::sizeof(layout, self.elems.len() as u32)
    }
}

pub fn remove_elem(elems: &mut Vec<AvailElem>, size: u32) -> Option<AvailElem> {
    elems
        .iter()
        .position(|elem| elem.sz >= size)
        .map(|index| elems.remove(index))
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

    #[test]
    fn test_merge_block() {
        struct Test<'a> {
            name: &'a str,
            first: super::AvailBlock,
            second: super::AvailBlock,
            expected: Option<super::AvailBlock>,
        }

        fn block(elems: &[(u64, u32)], sz: u32, next_block: u64) -> super::AvailBlock {
            super::AvailBlock {
                sz,
                next_block,
                elems: elems
                    .iter()
                    .copied()
                    .map(|(addr, sz)| super::AvailElem { addr, sz })
                    .collect(),
            }
        }

        [
            Test {
                name: "sorts",
                first: block(&[(40, 5), (0, 12)], 12, 0),
                second: block(&[(20, 10)], 10, 42),
                expected: Some(block(&[(40, 5), (20, 10), (0, 12)], 12, 42)),
            },
            Test {
                name: "combines blocks",
                first: block(&[(40, 5), (0, 10)], 2, 0),
                second: block(&[(10, 30)], 10, 42),
                expected: Some(block(&[(0, 45)], 2, 42)),
            },
            Test {
                name: "fails",
                first: block(&[(40, 5), (0, 12)], 2, 0),
                second: block(&[(20, 10)], 10, 42),
                expected: None,
            },
            Test {
                name: "empty self",
                first: block(&[], 10, 0),
                second: block(&[(20, 10)], 10, 42),
                expected: Some(block(&[(20, 10)], 10, 42)),
            },
            Test {
                name: "empty other",
                first: block(&[(20, 10)], 10, 0),
                second: block(&[], 10, 42),
                expected: Some(block(&[(20, 10)], 10, 42)),
            },
        ]
        .into_iter()
        .for_each(|test| {
            let merged = test.first.merge(&test.second);
            if merged != test.expected {
                panic!(
                    "test \"{}\" failed: expected:\n{:?}\ngot:\n{:?}",
                    test.name, test.expected, merged
                );
            }
        });
    }

    #[test]
    fn test_partition_elements() {
        use super::AvailElem;
        struct Test<'a> {
            name: &'a str,
            elements: Vec<AvailElem>,
            expected: (Vec<AvailElem>, Vec<AvailElem>),
        }

        [
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
        ]
        .into_iter()
        .for_each(|test| {
            let partitioned = super::partition_elems(&test.elements);
            if partitioned != test.expected {
                panic!(
                    "test \"{}\" failed: expected:\n{:?}\ngot:\n{:?}",
                    test.name, test.expected, partitioned
                );
            }
        });
    }
}
