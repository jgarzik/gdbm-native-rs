use std::io::{self, BufRead, BufReader, ErrorKind, Read};

use base64::Engine;

use crate::ser::Alignment;

pub struct ASCIIImportIterator<'a> {
    buf_reader: BufReader<&'a mut dyn Read>,
}

impl<'a> ASCIIImportIterator<'a> {
    pub fn new(reader: &'a mut dyn Read) -> io::Result<Self> {
        let mut buf_reader = BufReader::new(reader);
        Self::read_header(&mut buf_reader)?;

        Ok(Self { buf_reader })
    }

    fn read_header(buf_reader: &mut BufReader<&'a mut dyn Read>) -> io::Result<Vec<String>> {
        buf_reader
            .lines()
            .map(|line| match line {
                Ok(s) if s.as_str().starts_with('#') => Ok(s),
                Ok(s) => Err(io::Error::new(
                    ErrorKind::Other,
                    format!("bad header line: {s}"),
                )),
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
            .unwrap_or(Err(io::Error::new(ErrorKind::Other, "end of input")))
    }

    fn read_base64(&mut self, length: usize) -> io::Result<Vec<u8>> {
        let bytes = self
            .buf_reader
            .by_ref()
            .bytes()
            .filter(|b| !b.as_ref().map(u8::is_ascii_whitespace).unwrap_or_default())
            .take((4 * length / 3 + 3) & !3) // length of base64 representation
            .collect::<io::Result<Vec<_>>>()?;

        // read past line ending
        self.read_line().and_then(|l| {
            l.is_empty()
                .then_some(())
                .ok_or_else(|| io::Error::new(ErrorKind::Other, "unexpected data"))
        })?;

        base64::prelude::BASE64_STANDARD
            .decode(bytes)
            .map_err(|e| io::Error::new(ErrorKind::Other, format!("bad base64: {e}")))
            .and_then(|decoded| {
                (decoded.len() == length)
                    .then_some(decoded)
                    .ok_or_else(|| io::Error::new(ErrorKind::Other, "length mismatch"))
            })
    }

    fn read_datum(&mut self) -> io::Result<Option<Vec<u8>>> {
        let line = self.read_line()?;
        match line.split_once('=') {
            Some(("#:count", _)) => Ok(None),
            Some(("#:len", length)) => length
                .parse::<usize>()
                .map_err(|e| io::Error::new(ErrorKind::Other, format!("bad line ({line}): {e}")))
                .and_then(|length| self.read_base64(length))
                .map(Some),
            _ => Err(io::Error::new(
                ErrorKind::Other,
                format!("bad data ({line})"),
            )),
        }
    }
}

impl<'a> Iterator for ASCIIImportIterator<'a> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_datum() {
            Ok(None) => None,
            Ok(Some(key)) => match self.read_datum() {
                Ok(None) => Some(Err(io::Error::new(ErrorKind::Other, "end of file"))),
                Ok(Some(value)) => Some(Ok((key, value))),
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct BinaryImportIterator<'a> {
    alignment: Alignment,
    buf_reader: BufReader<&'a mut dyn Read>,
}

impl<'a> BinaryImportIterator<'a> {
    pub fn new(alignment: Alignment, reader: &'a mut dyn Read) -> io::Result<Self> {
        let mut buf_reader = BufReader::new(reader);

        // skip 4 header lines
        let mut line = String::new();
        (0..4).try_for_each(|_| buf_reader.read_line(&mut line).map(|_| ()))?;

        Ok(Self {
            alignment,
            buf_reader,
        })
    }

    fn read_datum(&mut self) -> io::Result<Option<Vec<u8>>> {
        let length = self
            .buf_reader
            .by_ref()
            .bytes()
            .take(match self.alignment {
                Alignment::Align32 => 4,
                Alignment::Align64 => 8,
            })
            .collect::<io::Result<Vec<_>>>()
            .and_then(|buf| match (self.alignment, buf.len()) {
                (_, 0) => Ok(None),
                (Alignment::Align32, 4) => {
                    Ok(Some(u32::from_be_bytes(buf.try_into().unwrap()) as usize))
                }
                (Alignment::Align64, 8) => {
                    Ok(Some(u64::from_be_bytes(buf.try_into().unwrap()) as usize))
                }
                _ => Err(io::Error::new(ErrorKind::UnexpectedEof, "partial read")),
            })?;

        match length {
            Some(n) => {
                let mut buf = vec![0; n];
                self.buf_reader.read_exact(&mut buf)?;
                Ok(Some(buf))
            }
            None => Ok(None),
        }
    }
}

impl<'a> Iterator for BinaryImportIterator<'a> {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_datum() {
            Ok(None) => None,
            Ok(Some(key)) => match self.read_datum() {
                Ok(None) => Some(Err(io::Error::new(ErrorKind::Other, "end of file"))),
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
}
