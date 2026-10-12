//
// tests/features.rs -- testing lock waiting, verify, recover and header getters
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::io::{Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

use gdbm_native::{Error, Gdbm, Layout, OpenOptions};
use tempfile::tempdir;

fn fill(path: &std::path::Path, n: usize) -> Gdbm {
    let mut db = OpenOptions::new()
        .create(true)
        .block_size(gdbm_native::BlockSize::Exactly(512))
        .open(path)
        .unwrap();
    for i in 0..n {
        db.insert(format!("key {i}").as_str(), format!("value {i}").as_str())
            .unwrap();
    }
    db
}

#[test]
// With lock_wait, opening retries until the lock is released or the time is up.
fn lock_wait() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let holder = fill(&path, 1);

    let start = Instant::now();
    let result = OpenOptions::new()
        .lock_wait(Duration::from_millis(200))
        .open(&path);
    assert!(matches!(result, Err(Error::Locked)));
    assert!(start.elapsed() >= Duration::from_millis(200));

    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(holder);
    });
    let db = OpenOptions::new()
        .lock_wait(Duration::from_secs(10))
        .open(&path)
        .unwrap();
    assert_eq!(db.len().unwrap(), 1);
    releaser.join().unwrap();
}

#[test]
fn getters() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = fill(&path, 500);
    db.sync().unwrap();
    assert_eq!(db.block_size(), 512);
    // bucket header (112 bytes) plus 16 elements of 24 bytes
    assert_eq!(db.bucket_size(), 496);
    assert!(db.dir_depth() >= 1);
    assert!(db.bucket_count() > 1);
    assert!(db.is_numsync());
    assert!(db.numsync().unwrap() > 0);
    assert_eq!(db.layout(), Layout::Off64Align8);
    assert_eq!(db.path(), Some(path.as_path()));
}

#[test]
fn verify() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = fill(&path, 2000);
    for i in (0..2000).step_by(3) {
        db.remove(format!("key {i}").as_str()).unwrap();
    }
    let report = db.verify().unwrap();
    assert_eq!(report.records, db.len().unwrap());
    assert_eq!(report.buckets, db.bucket_count());
    assert!(report.free_bytes > 0);

    // a record pointing into the file header is an overlap
    db.close().unwrap();
    corrupt_first_record_offset(&path, 0);
    let db = OpenOptions::new().open(&path).unwrap();
    assert!(matches!(db.verify(), Err(Error::Corrupt { .. })));
}

// Overwrite the data offset of the first occupied element of the first bucket, parsing the file
// as written by fill(): little-endian, 64-bit offsets aligned on 8 bytes, 512-byte blocks.
fn corrupt_first_record_offset(path: &std::path::Path, offset: u64) {
    let file = std::fs::read(path).unwrap();
    let u64_at = |at: usize| u64::from_le_bytes(file[at..at + 8].try_into().unwrap()) as usize;
    let dir = u64_at(8);
    let bucket = u64_at(dir);
    // bucket header of 112 bytes, then elements of 24 bytes starting with the hash
    let element = (0..16)
        .map(|i| bucket + 112 + i * 24)
        .find(|&e| file[e..e + 4] != [0xff; 4])
        .unwrap() as u64;
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    // LFS element: hash 4, key_start 4, data_ofs 8
    f.seek(SeekFrom::Start(element + 8)).unwrap();
    f.write_all(&offset.to_le_bytes()).unwrap();
}

#[test]
// recover() rebuilds the database from what can be read, optionally keeping a backup.
fn recover() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    fill(&path, 1000).close().unwrap();
    corrupt_first_record_offset(&path, u64::MAX / 2);
    let original = std::fs::read(&path).unwrap();

    let mut db = OpenOptions::new().write(true).open(&path).unwrap();
    assert!(db.verify().is_err());
    let report = db.recover(true).unwrap();
    assert_eq!(report.recovered_keys, 999);
    assert_eq!(report.failed_keys, 1);
    assert_eq!(report.failed_buckets, 0);
    let backup = report.backup.unwrap();
    assert_eq!(std::fs::read(&backup).unwrap(), original);

    db.verify().unwrap();
    assert_eq!(db.len().unwrap(), 999);
    db.insert("new", "value").unwrap();
    db.close().unwrap();
    let db = OpenOptions::new().open(&path).unwrap();
    assert_eq!(db.len().unwrap(), 1000);
    db.verify().unwrap();
}

#[cfg(unix)]
#[test]
// A backup must not be more accessible than the database it copies.
fn recover_backup_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    fill(&path, 10).close().unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let mut db = OpenOptions::new().write(true).open(&path).unwrap();
    let backup = db.recover(true).unwrap().backup.unwrap();
    let mode = std::fs::metadata(&backup).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
}
