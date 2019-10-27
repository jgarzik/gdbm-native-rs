use byteorder::{LittleEndian, ReadBytesExt};
use std::io::{self, Read};

use crate::ser::{w32, woff_t};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct AvailElem {
    pub sz: u32,
    pub addr: u64,
}

impl AvailElem {
    pub fn from_reader(is_64: bool, rdr: &mut impl Read) -> io::Result<Self> {
        let elem_sz = rdr.read_u32::<LittleEndian>()?;
        let elem_ofs: u64;
        if is_64 {
            let _padding = rdr.read_u32::<LittleEndian>()?;
            elem_ofs = rdr.read_u64::<LittleEndian>()?;
        } else {
            elem_ofs = rdr.read_u32::<LittleEndian>()? as u64;
        }

        Ok(AvailElem {
            sz: elem_sz,
            addr: elem_ofs,
        })
    }

    pub fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.sz));
        if is_64 {
            let padding: u32 = 0;
            buf.append(&mut w32(is_le, padding));
        }
        buf.append(&mut woff_t(is_64, is_le, self.addr));

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
        for i in 0..self.elems.len() {
            if (self.elems[i].sz as usize) >= sz {
                return Some(i);
            }
        }

        None
    }

    pub fn remove_elem(&mut self, sz: usize) -> Option<AvailElem> {
        assert!((self.count as usize) == self.elems.len());
        match self.find_elem(sz) {
            None => None,
            Some(idx) => {
                self.count -= 1;
                return Some(self.elems.remove(idx));
            }
        }
    }

    pub fn serialize(&self, is_64: bool, is_le: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.append(&mut w32(is_le, self.sz));
        buf.append(&mut w32(is_le, self.count));
        buf.append(&mut woff_t(is_64, is_le, self.next_block));

        for elem in &self.elems {
            buf.append(&mut elem.serialize(is_64, is_le));
        }

        buf
    }
}
