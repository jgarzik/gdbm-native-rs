pub struct Bytes(Vec<u8>);

impl Bytes {
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<usize> for Bytes {
    fn from(u: usize) -> Self {
        Self(u.to_be_bytes().to_vec())
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<&[u8]> for Bytes {
    fn from(bs: &[u8]) -> Self {
        Self(bs.to_vec())
    }
}

impl From<Bytes> for String {
    fn from(b: Bytes) -> Self {
        std::str::from_utf8(&b.0)
            .map(|s| s.to_string())
            .unwrap_or_else(|_| Self::from_utf8_lossy(&b.0).as_ref().to_string())
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(b: Bytes) -> Self {
        b.0
    }
}

pub enum BytesRef<'a> {
    WithBuffer(Vec<u8>),
    Reference(&'a [u8]),
}

impl<'a> AsRef<[u8]> for BytesRef<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::WithBuffer(b) => b.as_ref(),
            Self::Reference(r) => r,
        }
    }
}

impl<'a> From<&'a [u8]> for BytesRef<'a> {
    fn from(a: &'a [u8]) -> BytesRef<'a> {
        Self::Reference(a)
    }
}

impl<'a> From<&'a str> for BytesRef<'a> {
    fn from(s: &'a str) -> BytesRef<'a> {
        Self::Reference(s.as_ref())
    }
}

impl<'a> From<&'a String> for BytesRef<'a> {
    fn from(s: &'a String) -> BytesRef<'a> {
        Self::Reference(s.as_bytes())
    }
}

impl<'a> From<&'a usize> for BytesRef<'a> {
    fn from(u: &'a usize) -> BytesRef<'a> {
        let buf = u.to_be_bytes().to_vec();
        Self::WithBuffer(buf)
    }
}

impl<'a> From<&'a Vec<u8>> for BytesRef<'a> {
    fn from(v: &'a Vec<u8>) -> BytesRef<'a> {
        Self::Reference(v.as_ref())
    }
}

impl<'a> From<&'a Bytes> for BytesRef<'a> {
    fn from(b: &'a Bytes) -> BytesRef<'a> {
        Self::Reference(b.as_ref())
    }
}
