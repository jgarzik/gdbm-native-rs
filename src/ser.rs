use byteorder::{BigEndian, ByteOrder, LittleEndian};

// serialize u32, with runtime endian selection
pub fn w32(is_le: bool, val: u32) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(4);
    buf.resize(4, 0);

    match is_le {
        true => LittleEndian::write_u32(&mut buf, val),
        false => BigEndian::write_u32(&mut buf, val),
    }

    buf
}

// serialize u64, with runtime endian selection
pub fn w64(is_le: bool, val: u64) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(4);
    buf.resize(4, 0);

    match is_le {
        true => LittleEndian::write_u64(&mut buf, val),
        false => BigEndian::write_u64(&mut buf, val),
    }

    buf
}

pub fn woff_t(is_64: bool, is_le: bool, val: u64) -> Vec<u8> {
    match is_64 {
        true => w64(is_le, val),
        false => w32(is_le, val as u32),
    }
}
