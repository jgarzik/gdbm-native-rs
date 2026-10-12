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

use gdbm_native::{BlockSize, Error, OpenOptions};
use tempfile::tempdir;

// Databases here are little endian with 64-bit offsets aligned on 8 bytes and 512-byte blocks:
// header fields at fixed offsets, buckets of a 112-byte header and 24-byte elements.
const DIR_OFS: usize = 8;
const DIR_SZ: usize = 16;
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

#[test]
// Bug: a directory with fewer entries than its bit count implies (a size that is not a power of
// two, which C GDBM also accepts) made lookups of keys hashing past its end index out of bounds.
// C GDBM reports GDBM_BAD_DIR_ENTRY for them.
fn short_directory_lookup_does_not_panic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = create(&path, false);
    let mut n = 0;
    while db.dir_depth() < 7 {
        db.insert(format!("key {n}").as_str(), "v").unwrap();
        n += 1;
    }
    db.close().unwrap();

    // drop the last directory entry
    let file = std::fs::read(&path).unwrap();
    let dir_sz = u32::from_le_bytes(file[DIR_SZ..DIR_SZ + 4].try_into().unwrap());
    patch(&path, DIR_SZ, &(dir_sz - 8).to_le_bytes());

    let mut db = OpenOptions::new().write(true).open(&path).unwrap();
    let bad_entry = |result: &gdbm_native::Result<_>| {
        matches!(
            result,
            Err(Error::BadDirectoryEntry {
                index: 127,
                entries: 127
            })
        )
    };
    let results = (0..n)
        .map(|i| db.get::<_, String>(format!("key {i}").as_str()))
        .collect::<Vec<_>>();
    assert!(results.iter().any(bad_entry));
    assert!(results.iter().filter(|r| !bad_entry(r)).all(|r| r.is_ok()));
    for i in 0..n {
        let _ = db.insert(format!("key {i}").as_str(), "new");
    }
}

// Insert into a possibly corrupt database: errors are fine, panics are not.
fn insert_some(path: &Path) {
    if let Ok(mut db) = OpenOptions::new().write(true).open(path) {
        for i in 0..20u8 {
            let _ = db.insert(&[i, 0xe9], &vec![i; usize::from(i) * 40]);
        }
    }
}

#[test]
// Bug (found by fuzzing): a bucket whose header count is below capacity while every slot is
// occupied passed validation, and inserting into it panicked in Bucket::insert. A file truncated
// to 1 MiB has such buckets.
fn truncated_database_insert_does_not_panic() {
    let data: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data"]
        .iter()
        .collect();
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let file = std::fs::read(data.join("basic.db.le32.numsync")).unwrap();
    std::fs::write(&path, &file[..1 << 20]).unwrap();
    insert_some(&path);
}

#[test]
// The same with a hand-made bucket of zeroes: count 0, but every slot's hash is 0, i.e. occupied.
fn zeroed_bucket_insert_does_not_panic() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = create(&path, true);
    db.insert("key", "value").unwrap();
    db.close().unwrap();

    let file = std::fs::read(&path).unwrap();
    let bucket0 = u64_at(&file, u64_at(&file, DIR_OFS));
    patch(&path, bucket0, &[0u8; 496]);
    insert_some(&path);
}

#[test]
// Inputs that crashed the `open` fuzz target, kept as fixtures: opening, reading and inserting
// into them must fail cleanly. open-split-dir-range: a directory whose entries for a bucket are
// not the aligned range its bit count implies made a bucket split index past the directory.
fn fuzz_crash_inputs() {
    let fixtures: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data", "fuzz"]
        .iter()
        .collect();
    let dir = tempdir().unwrap();
    for entry in std::fs::read_dir(fixtures).unwrap() {
        let input = entry.unwrap().path();
        let path = dir.path().join("db");
        std::fs::copy(&input, &path).unwrap();
        if let Ok(db) = OpenOptions::new().open(&path) {
            let _ = db.verify();
            let _ = db.len();
            let _ = db.get::<_, Vec<u8>>("key \u{e9}");
        }
        insert_some(&path);
    }
}
