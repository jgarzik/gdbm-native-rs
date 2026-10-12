use std::io::{self, BufRead, BufReader, Chain, Cursor, ErrorKind, Read};

use base64::Engine;

use crate::dump::BinDumpFormat;

pub struct ASCIIImportIterator<'a> {
    buf_reader: BufReader<&'a mut dyn Read>,
    pub header: Vec<String>,
}

impl<'a> ASCIIImportIterator<'a> {
    pub fn new(reader: &'a mut dyn Read) -> io::Result<Self> {
        let mut buf_reader = BufReader::new(reader);
        let header = Self::read_header(&mut buf_reader)?;

        Ok(Self { buf_reader, header })
    }

    fn read_header(buf_reader: &mut BufReader<&'a mut dyn Read>) -> io::Result<Vec<String>> {
        buf_reader
            .lines()
            .map(|line| match line {
                Ok(s) if s.as_str().starts_with('#') => Ok(s),
                Ok(s) => Err(io::Error::other(format!("bad header line: {s}"))),
                Err(e) => Err(e),
            })
            .take_while(|l| !l.as_ref().is_ok_and(|s| s == "# End of header"))
            .collect()
    }

    fn read_line(&mut self) -> io::Result<String> {
        self.buf_reader
            .by_ref()
            .lines()
            .next()
            .unwrap_or(Err(io::Error::other("end of input")))
    }

    fn read_base64(&mut self, length: usize) -> io::Result<Vec<u8>> {
        // C GDBM writes no data line at all for a zero-length datum; gdbm-native writes an
        // empty line (see export_ascii_datum), which read_datum() skips
        if length == 0 {
            return Ok(Vec::new());
        }

        let bytes = self
            .buf_reader
            .by_ref()
            .bytes()
            .filter(|b| b.is_err() || !b.as_ref().unwrap().is_ascii_whitespace())
            .take((length.saturating_mul(4) / 3).saturating_add(3) & !3) // length of base64 representation
            .collect::<io::Result<Vec<_>>>()?;

        // read past line ending
        self.read_line().and_then(|l| {
            l.is_empty()
                .then_some(())
                .ok_or_else(|| io::Error::other("unexpected data"))
        })?;

        base64::prelude::BASE64_STANDARD
            .decode(bytes)
            .map_err(|e| io::Error::other(format!("bad base64: {e}")))
            .and_then(|decoded| {
                (decoded.len() == length)
                    .then_some(decoded)
                    .ok_or_else(|| io::Error::other("length mismatch"))
            })
    }

    fn read_datum(&mut self) -> io::Result<Option<Vec<u8>>> {
        // skip the empty line gdbm-native writes after zero-length data
        let line = std::iter::repeat_with(|| self.read_line())
            .find(|line| !line.as_ref().is_ok_and(String::is_empty))
            .unwrap()?;
        match line.split_once('=') {
            Some(("#:count", _)) => Ok(None),
            Some(("#:len", length)) => length
                .parse::<usize>()
                .map_err(|e| io::Error::other(format!("bad line ({line}): {e}")))
                .and_then(|length| self.read_base64(length))
                .map(Some),
            _ => Err(io::Error::other(format!("bad data ({line})"))),
        }
    }
}

impl Iterator for ASCIIImportIterator<'_> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_datum() {
            Ok(None) => None,
            Ok(Some(key)) => match self.read_datum() {
                Ok(None) => Some(Err(io::Error::other("end of file"))),
                Ok(Some(value)) => Some(Ok((key, value))),
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct BinaryImportIterator<'a> {
    format: BinDumpFormat,
    // bytes read ahead to detect the format, then the rest of the dump
    reader: Chain<Cursor<Vec<u8>>, BufReader<&'a mut dyn Read>>,
}

impl<'a> BinaryImportIterator<'a> {
    // Skip the header, then use format or detect it from the first length field.
    pub fn new(format: Option<BinDumpFormat>, reader: &'a mut dyn Read) -> io::Result<Self> {
        let mut buf_reader = BufReader::new(reader);

        // four header lines, each starting with '!'
        for _ in 0..4 {
            let mut line = Vec::new();
            buf_reader.read_until(b'\n', &mut line)?;
            if line.first() != Some(&b'!') {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "not a GDBM binary dump",
                ));
            }
        }

        let mut prefix = Vec::with_capacity(8);
        buf_reader.by_ref().take(8).read_to_end(&mut prefix)?;
        let format = format
            .or_else(|| BinDumpFormat::detect(&prefix))
            .ok_or_else(|| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    "cannot detect the binary dump format; specify it",
                )
            })?;

        Ok(Self {
            format,
            reader: Cursor::new(prefix).chain(buf_reader),
        })
    }

    fn read_datum(&mut self) -> io::Result<Option<Vec<u8>>> {
        let mut field = Vec::with_capacity(8);
        self.reader
            .by_ref()
            .take(self.format.field_size() as u64)
            .read_to_end(&mut field)?;
        let length = match field.len() {
            0 => return Ok(None),
            n if n == self.format.field_size() => {
                self.format.decode_len(&field).ok_or_else(|| {
                    io::Error::new(
                        ErrorKind::InvalidData,
                        format!("bad length field for {:?}", self.format),
                    )
                })?
            }
            _ => return Err(io::Error::new(ErrorKind::UnexpectedEof, "partial read")),
        };

        // read incrementally rather than trusting length for an allocation
        let mut buf = Vec::new();
        self.reader
            .by_ref()
            .take(u64::from(length))
            .read_to_end(&mut buf)?;
        (buf.len() == length as usize)
            .then_some(Some(buf))
            .ok_or_else(|| io::Error::new(ErrorKind::UnexpectedEof, "partial read"))
    }
}

impl Iterator for BinaryImportIterator<'_> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_datum() {
            Ok(None) => None,
            Ok(Some(key)) => match self.read_datum() {
                Ok(None) => Some(Err(io::Error::other("end of file"))),
                Ok(Some(value)) => Some(Ok((key, value))),
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn iterates() {
        let export = "# GDBM dump file created by 1.23
#:version=1.1
#:file=some_file.gdbm
#:format=standard
# End of header
#:len=7
SGVsb
G8sIA==
#:len=6
d29
ybGQh
#:count=2
# End of data";

        let kv = ASCIIImportIterator::new(&mut export.as_bytes())
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .map(|(k, v)| {
                std::str::from_utf8(&k).unwrap().to_string() + std::str::from_utf8(&v).unwrap()
            })
            .collect::<String>();
        assert_eq!(kv, "Hello, world!");
    }

    // A dump claiming an enormous length must fail cleanly, not attempt the allocation.
    #[test]
    fn binary_huge_length() {
        let mut dump = b"!\r\n! GDBM FLAT FILE DUMP\r\n! version\r\n!\r\n".to_vec();
        dump.extend_from_slice(&[0xff; 4]);
        dump.extend_from_slice(b"short");

        let mut reader = dump.as_slice();
        let mut iter = BinaryImportIterator::new(Some(BinDumpFormat::Long32), &mut reader).unwrap();
        assert!(iter.next().unwrap().is_err());
    }
}
