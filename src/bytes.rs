//
// bytes.rs -- conversion of keys and values to and from bytes
//
// Copyright (c) 2024 Jeff Garzik, John Hedges
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

//! Conversion of keys and values to and from the bytes stored in a database.
//!
//! GDBM stores keys and values as plain byte strings. [`ToBytesRef`] and [`FromBytes`] are
//! implemented for byte containers and strings; implement them for your own types to store
//! those directly. Choose an encoding that suits every program that will read the database:
//! C programs using GDBM commonly store C strings, with or without the terminating NUL.

use std::borrow::Cow;

use crate::{Error, Result};

/// A type that can be stored as a key or value.
pub trait ToBytesRef {
    /// The bytes to store, borrowed if possible.
    fn to_bytes_ref(&self) -> Cow<'_, [u8]>;
}

/// A type that can be read from a stored key or value.
pub trait FromBytes: Sized {
    /// Convert stored bytes to `Self`.
    ///
    /// # Errors
    /// [`Error::BadData`] if the bytes are not a valid `Self`.
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
}

impl ToBytesRef for [u8] {
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self)
    }
}

impl<const N: usize> ToBytesRef for [u8; N] {
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self)
    }
}

impl ToBytesRef for Vec<u8> {
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self)
    }
}

impl ToBytesRef for str {
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
}

impl ToBytesRef for String {
    fn to_bytes_ref(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }
}

impl FromBytes for Vec<u8> {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl<const N: usize> FromBytes for [u8; N] {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bytes
            .try_into()
            .map_err(|_| Error::BadData(format!("{} bytes, expected {N}", bytes.len())))
    }
}

impl FromBytes for String {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        std::str::from_utf8(bytes)
            .map(String::from)
            .map_err(|e| Error::BadData(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions() {
        assert_eq!(b"abc".to_bytes_ref().as_ref(), b"abc");
        assert_eq!(b"abc"[..].to_bytes_ref().as_ref(), b"abc");
        assert_eq!(vec![1u8, 2].to_bytes_ref().as_ref(), [1, 2]);
        assert_eq!("é".to_bytes_ref().as_ref(), "é".as_bytes());
        assert_eq!(String::from("x").to_bytes_ref().as_ref(), b"x");

        assert_eq!(<[u8; 2]>::from_bytes(&[1, 2]).unwrap(), [1, 2]);
        assert!(<[u8; 2]>::from_bytes(&[1]).is_err());
        assert_eq!(String::from_bytes(b"ok").unwrap(), "ok");
        assert!(String::from_bytes(&[0xff]).is_err());
        assert_eq!(Vec::<u8>::from_bytes(&[]).unwrap(), Vec::<u8>::new());
    }
}
