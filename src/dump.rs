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
/// dump. The dump does not record which was used.
///
/// [`Gdbm::import_bin`](crate::Gdbm::import_bin) can detect it from the first 64 KiB of
/// records. A format is consistent with them if they parse under it as length fields, each
/// below 2<sup>31</sup> (with a zero padding half for the 8-byte formats) and followed by that
/// many bytes, with at most one empty key; if the dump ends within those 64 KiB, it must end
/// after a whole key/value pair. Of the consistent formats, the one under which whole non-empty
/// data extend furthest into the 64 KiB is chosen, and of those `Long64Le`, then `Long64Be`,
/// then `Long32`. Detection fails if no format is consistent. A dump whose first records are
/// large may be ambiguous; specify the format when it is known.
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

    // Parse `records`, the start of a dump's records, under this format: a sequence of length
    // fields, each below 2^31 (C GDBM writes an int) and followed by that many bytes, at most one
    // of the keys (even-numbered data) empty, as keys are unique. If `complete`, `records` is the
    // rest of the dump and must end after a whole number of key/value pairs; otherwise it may
    // end anywhere. Returns where the last whole non-empty datum ends (an empty one may be the
    // zero half of a 64-bit length field), or None if `records` does not parse.
    fn parse_extent(self, records: &[u8], complete: bool) -> Option<usize> {
        let mut pos = 0;
        let mut extent = 0;
        let mut empty_key = false;
        let mut index = 0usize;
        loop {
            let rest = &records[pos..];
            if rest.is_empty() {
                return (!complete || index.is_multiple_of(2)).then_some(extent);
            }
            let Some((field, data)) = rest.split_at_checked(self.field_size()) else {
                return (!complete).then_some(extent);
            };
            let len = self
                .decode_len(field)
                .filter(|&len| len <= i32::MAX as u32)? as usize;
            if data.len() < len {
                return (!complete).then_some(extent);
            }
            pos += field.len() + len;
            if len > 0 {
                extent = pos;
            } else if index.is_multiple_of(2) {
                if empty_key {
                    return None;
                }
                empty_key = true;
            }
            index += 1;
        }
    }

    // Detect the format from `records`, the start of a dump's records (all of them if
    // `complete`), as the type's documentation describes. None if no format is consistent.
    pub(crate) fn detect(records: &[u8], complete: bool) -> Option<Self> {
        [Self::Long64Le, Self::Long64Be, Self::Long32]
            .into_iter()
            .filter_map(|format| Some((format, format.parse_extent(records, complete)?)))
            // min_by_key keeps the first, most preferred, of equal extents
            .min_by_key(|&(_, extent)| std::cmp::Reverse(extent))
            .map(|(format, _)| format)
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

    // Records (key, value) dumped in `format`.
    fn dump(format: BinDumpFormat, records: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        for datum in records.iter().flat_map(|&(k, v)| [k, v]) {
            format.write_len(&mut buf, datum.len() as u32).unwrap();
            buf.extend_from_slice(datum);
        }
        buf
    }

    const FORMATS: [BinDumpFormat; 3] = [
        BinDumpFormat::Long32,
        BinDumpFormat::Long64Le,
        BinDumpFormat::Long64Be,
    ];

    #[test]
    fn detect() {
        let zero_key = [0, 0, 0, 0, 1, 2, 3, 4];
        for records in [
            &[(&b"key"[..], &b"value"[..]), (b"second", b"x")][..],
            &[(&zero_key[..4], b"value"), (b"second", b"x")],
            &[(&zero_key, b"value"), (b"second", b"x")],
            &[(b"", b"value"), (b"second", b"x")],
            &[(b"key", b"")],
        ] {
            for format in FORMATS {
                let buf = dump(format, records);
                assert_eq!(
                    BinDumpFormat::detect(&buf, true),
                    Some(format),
                    "{format:?} {records:?}"
                );
            }
        }

        // nothing to tell the formats apart
        assert_eq!(
            BinDumpFormat::detect(&[], true),
            Some(BinDumpFormat::Long64Le)
        );
        // an empty key and value; its 64-bit forms are the same 16 zero bytes
        assert_eq!(
            BinDumpFormat::detect(&[0; 8], true),
            Some(BinDumpFormat::Long32)
        );
        assert_eq!(
            BinDumpFormat::detect(&[0; 16], true),
            Some(BinDumpFormat::Long64Le)
        );
        // not a dump in any format: a truncated length field or data, an odd number of fields
        assert_eq!(BinDumpFormat::detect(&[0, 0, 0, 1], true), None);
        assert_eq!(BinDumpFormat::detect(&[0, 0, 0, 1, 0, 0, 0, 0], true), None);
        assert_eq!(BinDumpFormat::detect(&[0, 0, 0, 1, b'k'], true), None);
        assert_eq!(
            BinDumpFormat::detect(&[0x80, 0, 0, 0, 0x80, 0, 0, 0], false),
            None
        );
        // two empty keys
        assert_eq!(BinDumpFormat::detect(&[0; 24], false), None);
    }

    // The read-ahead may end anywhere after the first record; the format under which most of
    // it parses wins.
    #[test]
    fn detect_partial() {
        let records: Vec<(&[u8], &[u8])> = vec![(b"keyAkeyB", b"some value"); 20];
        for format in FORMATS {
            let buf = dump(format, &records);
            let record = 2 * format.field_size() + 18;
            for end in record..buf.len() {
                assert_eq!(
                    BinDumpFormat::detect(&buf[..end], false),
                    Some(format),
                    "{format:?} {end}"
                );
                // a complete dump must end after a whole record
                let complete = (end % record == 0).then_some(format);
                assert_eq!(
                    BinDumpFormat::detect(&buf[..end], true),
                    complete,
                    "{format:?} {end}"
                );
            }
        }

        // Long64Le parses this Long32 dump until the second length, which is past the end
        let mut records: Vec<(&[u8], &[u8])> =
            vec![(&[0, 0, 0, 0, 1, 2, 3, 4], &[1, 0, 0, 0, 0, 0, 0, 0])];
        records.extend(vec![(&b"key"[..], &b"value"[..]); 20]);
        let buf = dump(BinDumpFormat::Long32, &records);
        assert_eq!(
            BinDumpFormat::detect(&buf[..buf.len() - 1], false),
            Some(BinDumpFormat::Long32)
        );
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
