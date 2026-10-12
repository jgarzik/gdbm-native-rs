//
// dump.rs -- GDBM dump file formats
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{self, Write};

/// Layout of the length fields in a binary ("flat file") dump, as written by C GDBM's
/// `gdbm_export` and read by `gdbm_import`.
///
/// C GDBM writes each length as a C `long` holding the length as a big-endian 32-bit value,
/// in the byte order of the machine, so the layout depends on the platform that wrote the
/// dump. The dump does not record which was used. [`Gdbm::import_bin`](crate::Gdbm::import_bin)
/// can detect it.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BinDumpFormat {
    /// 4-byte lengths: C GDBM on 32-bit platforms, and on Windows.
    Long32,
    /// 8-byte lengths, the big-endian 32-bit length followed by 4 zero bytes: C GDBM on 64-bit
    /// little-endian platforms such as x86-64 and arm64.
    Long64Le,
    /// 8-byte lengths, 4 zero bytes followed by the big-endian 32-bit length: C GDBM on 64-bit
    /// big-endian platforms such as s390x.
    Long64Be,
}

impl BinDumpFormat {
    /// The format C GDBM uses on the platform this program was built for, for exchanging dumps
    /// with the local C GDBM tools.
    #[must_use]
    pub fn host() -> Self {
        if cfg!(any(windows, target_pointer_width = "32")) {
            Self::Long32
        } else if cfg!(target_endian = "big") {
            Self::Long64Be
        } else {
            Self::Long64Le
        }
    }

    pub(crate) fn field_size(self) -> usize {
        match self {
            Self::Long32 => 4,
            Self::Long64Le | Self::Long64Be => 8,
        }
    }

    pub(crate) fn write_len(self, writer: &mut impl Write, len: u32) -> io::Result<()> {
        let len = len.to_be_bytes();
        match self {
            Self::Long32 => writer.write_all(&len),
            Self::Long64Le => writer.write_all(&len).and(writer.write_all(&[0; 4])),
            Self::Long64Be => writer.write_all(&[0; 4]).and(writer.write_all(&len)),
        }
    }

    // Decode a length field of field_size() bytes; None if the padding half is not zero.
    pub(crate) fn decode_len(self, field: &[u8]) -> Option<u32> {
        let be32 = |b: &[u8]| u32::from_be_bytes(b.try_into().unwrap());
        match self {
            Self::Long32 => Some(be32(field)),
            Self::Long64Le => (field[4..] == [0; 4]).then(|| be32(&field[..4])),
            Self::Long64Be => (field[..4] == [0; 4]).then(|| be32(&field[4..])),
        }
    }

    // Guess the format from the first 8 bytes after the header, which hold the first key's
    // length field. A key length is non-zero and below 2^31, so an 8-byte field has exactly one
    // zero half. None if undecidable (an all-zero prefix).
    pub(crate) fn detect(prefix: &[u8]) -> Option<Self> {
        if prefix.len() < 8 {
            return Some(Self::Long32);
        }
        match (prefix[..4] == [0; 4], prefix[4..8] == [0; 4]) {
            (false, false) => Some(Self::Long32),
            (false, true) => Some(Self::Long64Le),
            (true, false) => Some(Self::Long64Be),
            (true, true) => None,
        }
    }
}

/// How records being imported are stored when their key is already in the database.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum ImportMode {
    /// Replace the existing value. This is the default.
    #[default]
    Replace,
    /// Fail with [`Error::KeyExists`](crate::Error::KeyExists), like C GDBM's `GDBM_INSERT`.
    Insert,
}

/// Information from the header of an ASCII dump, returned by
/// [`Gdbm::import_ascii`](crate::Gdbm::import_ascii). Fields the dump does not contain are
/// `None`. gdbm-native does not apply the ownership or mode to any file; that is up to the
/// caller.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DumpInfo {
    /// Number of records imported.
    pub count: usize,
    /// Name of the database file that was dumped.
    pub file: Option<String>,
    /// Owner user id of the dumped database file.
    pub uid: Option<u32>,
    /// Owner user name of the dumped database file.
    pub user: Option<String>,
    /// Owner group id of the dumped database file.
    pub gid: Option<u32>,
    /// Owner group name of the dumped database file.
    pub group: Option<String>,
    /// Permission bits of the dumped database file.
    pub mode: Option<u32>,
    /// Whether the dumped database had a numsync header (`#:format=numsync`).
    pub numsync: Option<bool>,
}

impl DumpInfo {
    // Parse "#:name=value[,name=value...]" header lines; others are comments.
    pub(crate) fn from_header(lines: &[String]) -> Self {
        let mut info = Self::default();
        for line in lines {
            let Some(params) = line.strip_prefix("#:") else {
                continue;
            };
            // a file name may contain commas
            if let Some(file) = params.strip_prefix("file=") {
                info.file = Some(file.to_string());
                continue;
            }
            for (name, value) in params.split(',').filter_map(|p| p.split_once('=')) {
                match name {
                    "uid" => info.uid = value.parse().ok(),
                    "user" => info.user = Some(value.to_string()),
                    "gid" => info.gid = value.parse().ok(),
                    "group" => info.group = Some(value.to_string()),
                    "mode" => info.mode = u32::from_str_radix(value, 8).ok(),
                    "format" => info.numsync = Some(value == "numsync"),
                    _ => (),
                }
            }
        }
        info
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_fields() {
        for (format, bytes) in [
            (BinDumpFormat::Long32, vec![0, 0, 1, 2]),
            (BinDumpFormat::Long64Le, vec![0, 0, 1, 2, 0, 0, 0, 0]),
            (BinDumpFormat::Long64Be, vec![0, 0, 0, 0, 0, 0, 1, 2]),
        ] {
            let mut buf = Vec::new();
            format.write_len(&mut buf, 0x102).unwrap();
            assert_eq!(buf, bytes, "{format:?}");
            assert_eq!(format.field_size(), bytes.len());
            assert_eq!(format.decode_len(&bytes), Some(0x102));
        }
        assert_eq!(
            BinDumpFormat::Long64Le.decode_len(&[0, 0, 0, 1, 0, 0, 0, 1]),
            None
        );
        assert_eq!(
            BinDumpFormat::Long64Be.decode_len(&[0, 0, 0, 1, 0, 0, 0, 1]),
            None
        );
    }

    #[test]
    fn detect() {
        assert_eq!(
            BinDumpFormat::detect(&[0, 0, 0, 3, b'k', b'e', b'y', 0]),
            Some(BinDumpFormat::Long32)
        );
        assert_eq!(
            BinDumpFormat::detect(&[0, 0, 0, 3, 0, 0, 0, 0]),
            Some(BinDumpFormat::Long64Le)
        );
        assert_eq!(
            BinDumpFormat::detect(&[0, 0, 0, 0, 0, 0, 0, 3]),
            Some(BinDumpFormat::Long64Be)
        );
        assert_eq!(BinDumpFormat::detect(&[0; 8]), None);
        assert_eq!(BinDumpFormat::detect(&[]), Some(BinDumpFormat::Long32));
    }

    #[test]
    fn header() {
        let lines = [
            "# GDBM dump file created by GDBM version 1.24. 02/07/2024 on Mon Oct 12 2026",
            "#:version=1.1",
            "#:file=/out/a,b.db",
            "#:uid=0,user=root,gid=0,group=root,mode=644",
            "#:format=numsync",
        ]
        .map(String::from);
        let info = DumpInfo::from_header(&lines);
        assert_eq!(info.file.as_deref(), Some("/out/a,b.db"));
        assert_eq!(
            (info.uid, info.gid, info.mode),
            (Some(0), Some(0), Some(0o644))
        );
        assert_eq!(
            (info.user.as_deref(), info.group.as_deref()),
            (Some("root"), Some("root"))
        );
        assert_eq!(info.numsync, Some(true));
    }
}
