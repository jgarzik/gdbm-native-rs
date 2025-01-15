use crate::{Error, Result};

pub enum BytesRef<'a> {
    WithBuffer(Vec<u8>),
    Reference(&'a [u8]),
}

impl AsRef<[u8]> for BytesRef<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::WithBuffer(b) => b.as_ref(),
            Self::Reference(r) => r,
        }
    }
}

pub trait ToBytesRef {
    fn to_bytes_ref(&self) -> BytesRef;
}

pub trait FromBytes
where
    Self: Sized,
{
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

impl ToBytesRef for [u8] {
    fn to_bytes_ref(&self) -> BytesRef {
        BytesRef::Reference(self)
    }
}

impl ToBytesRef for Vec<u8> {
    fn to_bytes_ref(&self) -> BytesRef {
        BytesRef::Reference(self.as_ref())
    }
}

impl FromBytes for Vec<u8> {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl ToBytesRef for str {
    fn to_bytes_ref(&self) -> BytesRef {
        BytesRef::Reference(self.as_ref())
    }
}

impl ToBytesRef for String {
    fn to_bytes_ref(&self) -> BytesRef {
        BytesRef::Reference(self.as_ref())
    }
}

impl FromBytes for String {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        std::str::from_utf8(bytes)
            .map(String::from)
            .map_err(|e| Error::BadData(e.to_string()))
    }
}

impl ToBytesRef for bool {
    fn to_bytes_ref(&self) -> BytesRef {
        if *self {
            BytesRef::WithBuffer(vec![1])
        } else {
            BytesRef::WithBuffer(vec![0])
        }
    }
}

impl FromBytes for bool {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 1 {
            Err(Error::BadData(format!(
                "can't convert {} bytes into bool",
                bytes.len(),
            )))
        } else {
            Ok(bytes[0] == 1)
        }
    }
}

macro_rules! numeric_to_from_bytes {
    ($t:ty) => {
        impl ToBytesRef for $t {
            fn to_bytes_ref(&self) -> BytesRef {
                BytesRef::WithBuffer(self.to_le_bytes().to_vec())
            }
        }
        impl FromBytes for $t {
            fn from_bytes(bytes: &[u8]) -> Result<Self> {
                if bytes.len() != std::mem::size_of::<$t>() {
                    Err(Error::BadData(format!(
                        "can't convert {} bytes into {}",
                        bytes.len(),
                        stringify!($t)
                    )))
                } else {
                    let bytes = <&[_; std::mem::size_of::<$t>()]>::try_from(bytes).unwrap();
                    Ok(<$t>::from_le_bytes(*bytes))
                }
            }
        }
    };
}

numeric_to_from_bytes!(usize);
numeric_to_from_bytes!(u8);
numeric_to_from_bytes!(u16);
numeric_to_from_bytes!(u32);
numeric_to_from_bytes!(u64);
numeric_to_from_bytes!(u128);
numeric_to_from_bytes!(isize);
numeric_to_from_bytes!(i8);
numeric_to_from_bytes!(i16);
numeric_to_from_bytes!(i32);
numeric_to_from_bytes!(i64);
numeric_to_from_bytes!(i128);
numeric_to_from_bytes!(f32);
numeric_to_from_bytes!(f64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numerics() {
        fn test_conversion<T>(value: &T)
        where
            T: ToBytesRef,
            T: FromBytes,
            T: PartialEq,
        {
            assert!(*value == T::from_bytes((value).to_bytes_ref().as_ref()).unwrap());
            assert!(T::from_bytes(&[]).is_err());
        }

        test_conversion::<usize>(&42);
        test_conversion::<u8>(&42);
        test_conversion::<u16>(&42);
        test_conversion::<u32>(&42);
        test_conversion::<u64>(&42);
        test_conversion::<u128>(&42);
        test_conversion::<isize>(&42);
        test_conversion::<i8>(&42);
        test_conversion::<i16>(&42);
        test_conversion::<i32>(&42);
        test_conversion::<i64>(&42);
        test_conversion::<i128>(&42);
        test_conversion::<f32>(&4.2);
        test_conversion::<f64>(&4.2);
    }

    #[test]
    fn test_non_numeric() {
        let marvin = String::from("Marvin");
        assert!(marvin == String::from_bytes(marvin.as_str().to_bytes_ref().as_ref()).unwrap());
        assert!(marvin == String::from_bytes((marvin).to_bytes_ref().as_ref()).unwrap());
        assert!(!bool::from_bytes((false).to_bytes_ref().as_ref()).unwrap());
        assert!(bool::from_bytes((true).to_bytes_ref().as_ref()).unwrap());
    }
}
