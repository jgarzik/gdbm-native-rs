//
// bucket.rs -- GDBM bucket routines
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::io::{self, Error, ErrorKind, Read, Write};

use crate::avail::{self, AvailElem};
use crate::hashutil::{hash_key, PartialKey};
use crate::header::Header;
use crate::ser::{read32, read64, write32, write64, Alignment, Layout, Offset};

#[derive(Debug, Copy, Clone)]
pub struct BucketElement {
    pub hash: u32,
    pub key_start: PartialKey,
    pub data_ofs: u64,
    pub key_size: u32,
    pub data_size: u32,
}

impl Default for BucketElement {
    fn default() -> Self {
        Self {
            hash: 0xffffffff,
            key_start: PartialKey::default(),
            data_ofs: 0,
            key_size: 0,
            data_size: 0,
        }
    }
}

impl BucketElement {
    pub fn sizeof(layout: &Layout) -> u32 {
        match layout.offset {
            Offset::Small => 20,
            Offset::LFS => 24,
        }
    }

    pub fn new(key: &[u8], data: &[u8], offset: u64) -> Self {
        Self {
            hash: hash_key(key),
            key_start: PartialKey::new(key),
            data_ofs: offset,
            key_size: key.len() as u32,
            data_size: data.len() as u32,
        }
    }

    pub fn from_reader(layout: &Layout, reader: &mut impl Read) -> io::Result<Self> {
        let hash = read32(layout.endian, reader)?;

        let key_start = PartialKey::from_reader(reader)?;

        let data_ofs = match layout.offset {
            Offset::Small => (read32(layout.endian, reader)?) as u64,
            Offset::LFS => read64(layout.endian, reader)?,
        };

        let key_size = read32(layout.endian, reader)?;
        let data_size = read32(layout.endian, reader)?;

        Ok(BucketElement {
            hash,
            key_start,
            data_ofs,
            key_size,
            data_size,
        })
    }

    pub fn serialize(&self, layout: &Layout, writer: &mut impl Write) -> io::Result<()> {
        write32(layout.endian, writer, self.hash)?;

        self.key_start.serialize(writer)?;

        match layout.offset {
            Offset::Small => write32(layout.endian, writer, self.data_ofs as u32)?,
            Offset::LFS => write64(layout.endian, writer, self.data_ofs)?,
        }

        write32(layout.endian, writer, self.key_size)?;
        write32(layout.endian, writer, self.data_size)?;

        Ok(())
    }

    pub fn is_occupied(&self) -> bool {
        self.hash != 0xffffffff
    }
}

#[derive(Debug)]
pub struct Bucket {
    dirty: bool,
    // on-disk gdbm database hash bucket
    pub avail: Vec<AvailElem>,
    pub bits: u32,
    pub count: u32,
    pub tab: Vec<BucketElement>,
}

impl Bucket {
    pub const AVAIL: u32 = 6;

    pub fn new(bits: u32, len: usize, avail: Vec<AvailElem>, elements: Vec<BucketElement>) -> Self {
        elements.into_iter().fold(
            Self {
                dirty: true,
                avail,
                bits,
                count: 0,
                tab: vec![BucketElement::default(); len],
            },
            |mut bucket, elem| {
                bucket.insert(elem);
                bucket
            },
        )
    }

    pub fn from_reader(
        header: &Header,
        layout: &Layout,
        reader: &mut impl Read,
    ) -> io::Result<Self> {
        // read avail section
        let av_count = read32(layout.endian, reader)?;

        // paddding
        if layout.alignment.is64() {
            read32(layout.endian, reader)?;
        }

        // read av_count entries from bucket_avail[]
        let avail = (0..av_count)
            .map(|_| AvailElem::from_reader(layout, reader))
            .collect::<io::Result<Vec<_>>>()?;

        // read remaining to-be-ignored entries from bucket_avail[]
        (av_count..Self::AVAIL)
            .try_for_each(|_| AvailElem::from_reader(layout, reader).map(|_| ()))?;

        // todo: validate and assure-sorted avail[]

        // read misc. section
        let bits = read32(layout.endian, reader)?;
        let count = read32(layout.endian, reader)?;

        if !(count <= header.bucket_elems && bits <= header.dir_bits) {
            return Err(Error::new(ErrorKind::Other, "invalid bucket c/b"));
        }

        // read bucket elements section
        let tab = (0..header.bucket_elems)
            .map(|_| BucketElement::from_reader(layout, reader))
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Bucket {
            dirty: false,
            avail,
            bits,
            count,
            tab,
        })
    }

    pub fn serialize(&self, layout: &Layout, writer: &mut impl Write) -> io::Result<()> {
        assert!(self.avail.len() as u32 <= Self::AVAIL);

        //
        // avail section
        //

        write32(layout.endian, writer, self.avail.len() as u32)?;

        // padding
        if layout.alignment.is64() {
            write32(layout.endian, writer, 0)?;
        }

        // valid avail elements
        self.avail
            .iter()
            .try_for_each(|elem| elem.serialize(layout, writer))?;

        // dummy avail elements
        (self.avail.len() as u32..Self::AVAIL)
            .try_for_each(|_| AvailElem::default().serialize(layout, writer))?;

        //
        // misc section
        //
        write32(layout.endian, writer, self.bits)?;
        write32(layout.endian, writer, self.count)?;

        //
        // bucket elements section
        //
        self.tab
            .iter()
            .try_for_each(|elem| elem.serialize(layout, writer))?;

        Ok(())
    }

    pub fn sizeof(layout: &Layout) -> u32 {
        // 4 bytes each for bits, count and av_count + padding
        Self::AVAIL * AvailElem::sizeof(layout)
            + match layout.alignment {
                Alignment::Align32 => 12,
                Alignment::Align64 => 16,
            }
    }

    // insert an element - we assume there's space
    pub fn insert(&mut self, element: BucketElement) {
        self.count += 1;

        let index = (element.hash..)
            .map(|index| index as usize % self.tab.len())
            .find(|&index| !self.tab[index].is_occupied())
            .unwrap();

        self.tab[index] = element;

        self.dirty = true;
    }

    // remove an element - we assume there's an element
    pub fn remove(&mut self, offset: usize) -> BucketElement {
        let elem = self.tab[offset];
        let len = self.tab.len();

        // remove element from table
        self.tab[offset] = BucketElement::default();
        self.count -= 1;

        let mut last_ofs = offset;
        let mut elem_ofs = (offset + 1) % len;
        while elem_ofs != last_ofs && self.tab[elem_ofs].is_occupied() {
            let home = (self.tab[elem_ofs].hash as usize) % len;
            if (last_ofs < elem_ofs && (home <= last_ofs || home > elem_ofs))
                || (last_ofs > elem_ofs && home <= last_ofs && home > elem_ofs)
            {
                self.tab[last_ofs] = self.tab[elem_ofs];
                self.tab[elem_ofs] = BucketElement::default();
                last_ofs = elem_ofs;
            }

            elem_ofs = (elem_ofs + 1) % len;
        }

        self.dirty = true;

        elem
    }

    pub fn split(&self) -> (Bucket, Bucket) {
        let mask = 0x80_00_00_00 >> (self.bits + 1);
        let (elems0, elems1) = self
            .tab
            .iter()
            .copied()
            .partition::<Vec<_>, _>(|elem| elem.hash & mask == 0);

        let (avail0, avail1) = avail::partition_elems(&self.avail);

        (
            Bucket::new(self.bits + 1, self.tab.len(), avail0, elems0),
            Bucket::new(self.bits + 1, self.tab.len(), avail1, elems1),
        )
    }

    pub fn allocate(&mut self, size: u32) -> Option<(u64, u32)> {
        avail::remove_elem(&mut self.avail, size).inspect(|_| self.dirty = true)
    }

    pub fn free(&mut self, offset: u64, length: u32) {
        avail::insert_elem(&mut self.avail, offset, length);
        self.dirty = true;
    }
}

#[derive(Debug)]
pub struct BucketCache {
    cachesize: usize,
    buckets: HashMap<u64, Bucket>,
    // 1st element is MRU
    queue: Vec<u64>,
}

impl BucketCache {
    pub fn new(cachesize: usize, bucket: Option<(u64, Bucket)>) -> BucketCache {
        let buckets = bucket.into_iter().collect::<HashMap<_, _>>();
        let queue = buckets.keys().copied().collect();

        BucketCache {
            cachesize,
            buckets,
            queue,
        }
    }

    pub fn dirty_list(&self) -> Vec<(u64, &Bucket)> {
        let mut dl = self
            .buckets
            .iter()
            .filter_map(|(offset, bucket)| bucket.dirty.then_some(offset))
            .copied()
            .collect::<Vec<_>>();
        dl.sort();
        dl.iter()
            .map(|offset| (*offset, self.buckets.get(offset).unwrap()))
            .collect()
    }

    pub fn clear_dirty(&mut self) {
        self.buckets
            .values_mut()
            .for_each(|bucket| bucket.dirty = false);
    }

    pub fn contains(&self, bucket_ofs: u64) -> bool {
        self.buckets.contains_key(&bucket_ofs)
    }

    /// set_current moves bucket_offset to the front of the MRU queue.
    pub fn set_current(&mut self, bucket_offset: u64) {
        self.queue
            .iter()
            .position(|&o| o == bucket_offset)
            .inspect(|pos| {
                self.queue.copy_within(0..*pos, 1);
                self.queue[0] = bucket_offset;
            });
    }

    #[must_use]
    /// insert inserts the bucket into the cache and returns the evicted bucket if any, and if it
    /// is dirty (needs writing).
    pub fn insert(&mut self, bucket_offset: u64, bucket: Bucket) -> Option<(u64, Bucket)> {
        match self.buckets.insert(bucket_offset, bucket) {
            Some(_) => None, // bucket already in queue, nothing to evict
            None => {
                let evicted = (self.queue.len() >= self.cachesize)
                    .then_some(())
                    .and_then(|_| self.queue.pop())
                    .and_then(|offset| {
                        self.buckets
                            .remove(&offset)
                            .filter(|bucket| bucket.dirty)
                            .map(|bucket| (offset, bucket))
                    });
                self.queue.push(bucket_offset);

                evicted
            }
        }
    }

    pub fn current_bucket(&self) -> Option<&Bucket> {
        self.queue
            .first()
            .map(|offset| self.buckets.get(offset).unwrap())
    }

    pub fn current_bucket_offset(&self) -> Option<u64> {
        self.queue.iter().copied().next()
    }

    pub fn current_bucket_mut(&mut self) -> Option<&mut Bucket> {
        self.queue
            .first()
            .map(|offset| self.buckets.get_mut(offset).unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bucket_remove() {
        struct Test<'a> {
            name: &'a str,
            hashes: [u32; 4],
            offset: usize,
            expected: [u32; 4],
        }

        [
            Test {
                name: "first and only",
                hashes: [0, 0xffffffff, 0xffffffff, 0xffffffff],
                offset: 0,
                expected: [0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff],
            },
            Test {
                name: "last and only",
                hashes: [0xffffffff, 0xffffffff, 0xffffffff, 1],
                offset: 3,
                expected: [0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff],
            },
            Test {
                name: "dup hash",
                hashes: [0, 0, 0xffffffff, 0xffffffff],
                offset: 0,
                expected: [0, 0xffffffff, 0xffffffff, 0xffffffff],
            },
            Test {
                name: "dup hash, non-sequential",
                hashes: [0, 1, 0, 0xffffffff],
                offset: 0,
                expected: [0, 1, 0xffffffff, 0xffffffff],
            },
            Test {
                name: "dup hash, wrapped",
                hashes: [3, 1, 2, 3],
                offset: 3,
                expected: [0xffffffff, 1, 2, 3],
            },
            Test {
                name: "dup hash, wrapped, non-sequential",
                hashes: [2, 2, 2, 3],
                offset: 2,
                expected: [2, 0xffffffff, 2, 3],
            },
        ]
        .into_iter()
        .try_for_each(
            |Test {
                 name,
                 hashes,
                 offset,
                 expected,
             }| {
                let tab = hashes
                    .iter()
                    .map(|&hash| match hash {
                        0xffffffff => BucketElement::default(),
                        hash => BucketElement {
                            hash,
                            ..Default::default()
                        },
                    })
                    .collect::<Vec<_>>();

                let mut bucket = Bucket {
                    dirty: true,
                    avail: vec![],
                    bits: 0, /* unused */
                    count: tab.iter().filter(|elem| elem.is_occupied()).count() as u32,
                    tab,
                };

                bucket.remove(offset);

                let got = bucket.tab.iter().map(|elem| elem.hash).collect::<Vec<_>>();
                (got == expected).then_some(()).ok_or_else(|| {
                    format!(
                        "  failed: {}\nexpected: {:?}\n     got: {:?}",
                        name, expected, got
                    )
                })
            },
        )
        .map_err(|e| println!("{}", e))
        .unwrap()
    }

    #[test]
    fn insert() {
        // Ensure cache eviction mechanism works.
        struct Test {
            name: &'static str,
            bucket: Option<bool>,
            expected: bool,
        }

        [
            Test {
                name: "cache empty",
                bucket: None,
                expected: false,
            },
            Test {
                name: "cache clean add",
                bucket: Some(false),
                expected: false,
            },
            Test {
                name: "cache dirty add",
                bucket: Some(true),
                expected: true,
            },
        ]
        .into_iter()
        .try_for_each(|test| {
            let mut cache = BucketCache::new(
                1,
                test.bucket.map(|dirty| {
                    let mut bucket = Bucket::new(0, 0, vec![], vec![]);
                    bucket.dirty = dirty;

                    (100, bucket)
                }),
            );

            println!("{:?}", cache);
            let evicted = cache.insert(200, Bucket::new(0, 0, vec![], vec![]));

            (evicted.is_some() == test.expected)
                .then_some(())
                .ok_or_else(|| format!("{}: expected {}", test.name, test.expected))
        })
        .unwrap()
    }
}
