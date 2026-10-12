//
// tests/regressions.rs -- reproductions of reported bugs
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::path::Path;

use gdbm_native::{BlockSize, OpenOptions};
use tempfile::tempdir;

// Databases here are little endian with 64-bit offsets aligned on 8 bytes and 512-byte blocks:
// header fields at fixed offsets, buckets of a 112-byte header and 24-byte elements.
const DIR_OFS: usize = 8;
const BUCKET_COUNT: usize = 108;
const BUCKET_ELEMS: usize = 112;
const ELEM_SIZE: usize = 24;
const ELEM_KEY_START: usize = 4;

fn u64_at(file: &[u8], at: usize) -> usize {
    u64::from_le_bytes(file[at..at + 8].try_into().unwrap()) as usize
}

fn patch(path: &Path, at: usize, bytes: &[u8]) {
    let mut file = std::fs::read(path).unwrap();
    file[at..at + bytes.len()].copy_from_slice(bytes);
    std::fs::write(path, file).unwrap();
}

fn create(path: &Path, numsync: bool) -> gdbm_native::Gdbm {
    OpenOptions::new()
        .create(true)
        .numsync(numsync)
        .block_size(BlockSize::Exactly(512))
        .open(path)
        .unwrap()
}

#[test]
// Bug: with CharSign undetermined, recover() (via CharSign detection) and lookups of non-ASCII
// keys failed outright when any bucket was unreadable, instead of skipping it.
fn unreadable_bucket_does_not_block_char_sign_detection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = create(&path, true);
    for i in 0..300 {
        db.insert(format!("key {i}").as_str(), "v").unwrap();
    }
    for i in 0..300 {
        db.insert(format!("clé {i} €").as_str(), "é").unwrap();
    }
    assert!(db.bucket_count() > 4);
    db.close().unwrap();

    // make the bucket of directory entry 0 unreadable: more elements than a bucket holds
    let file = std::fs::read(&path).unwrap();
    let bucket0 = u64_at(&file, u64_at(&file, DIR_OFS));
    patch(&path, bucket0 + BUCKET_COUNT, &0xffffu32.to_le_bytes());

    // a non-ASCII key in a healthy bucket can still be found
    let db = OpenOptions::new().open(&path).unwrap();
    let found = (0..300)
        .filter(|i| {
            matches!(
                db.get::<_, String>(format!("clé {i} €").as_str()),
                Ok(Some(_))
            )
        })
        .count();
    assert!(found > 0, "no non-ASCII key found");
    drop(db);

    let mut db = OpenOptions::new().write(true).open(&path).unwrap();
    let report = db.recover(false).unwrap();
    assert_eq!(report.failed_buckets, 1);
    assert!(report.recovered_keys > 0);
    db.verify().unwrap();
}

#[test]
// Bug: key prefixes were compared on all 4 stored bytes, but C GDBM stores and compares only
// min(4, key length) bytes, so a short key in a reused C bucket slot (with leftover bytes from a
// deleted longer key) was not found, and inserting it again created a duplicate.
fn short_key_with_stale_prefix_bytes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = create(&path, true);
    db.insert("ab", "value").unwrap();
    db.close().unwrap();

    // what C GDBM leaves behind: bytes 2..4 of key_start from an earlier, longer key
    let file = std::fs::read(&path).unwrap();
    let bucket0 = u64_at(&file, u64_at(&file, DIR_OFS));
    let elem = (0..16)
        .map(|i| bucket0 + BUCKET_ELEMS + i * ELEM_SIZE)
        .find(|&e| file[e..e + 4] != [0xff; 4])
        .unwrap();
    patch(&path, elem + ELEM_KEY_START + 2, b"XY");

    let mut db = OpenOptions::new().write(true).open(&path).unwrap();
    assert_eq!(db.get::<_, String>("ab").unwrap().as_deref(), Some("value"));
    db.insert("ab", "new").unwrap();
    assert_eq!(db.len().unwrap(), 1);
}
