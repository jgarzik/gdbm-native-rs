//
// tests/hash.rs -- testing hash compatibility with C GDBM for non-ASCII keys
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

extern crate gdbm_native;

mod common;

use common::TestMetadata;
use gdbm_native::{CharSign, OpenOptions};
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "tests/data", name]
        .iter()
        .collect()
}

fn metadata() -> TestMetadata {
    serde_json::from_str(&std::fs::read_to_string(data_path("hash.json.le64")).unwrap()).unwrap()
}

// hash.db.* was written by C GDBM on x86_64, where char is signed.
#[test]
fn read_c_nonascii_keys() {
    let metadata = metadata();

    for db_name in ["hash.db.le64", "hash.db.le64.numsync"] {
        let db = OpenOptions::new().open(data_path(db_name)).unwrap();

        assert_eq!(db.len().unwrap(), metadata.data_records);
        for kv in &metadata.data {
            assert_eq!(
                db.get::<_, String>(kv[0].as_str()).unwrap().as_deref(),
                Some(kv[1].as_str()),
                "{db_name}: key {:?}",
                kv[0]
            );
        }
    }
}

// Keys inserted by gdbm-native land in the same bucket as C GDBM would put them.
#[test]
fn insert_nonascii_keys() {
    let metadata = metadata();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(data_path("hash.db.le64"), tmp.path()).unwrap();

    let mut db = OpenOptions::new().write(true).open(tmp.path()).unwrap();
    for kv in &metadata.data {
        // replacing an existing key must find (not duplicate) the C-written record
        db.insert(kv[0].as_str(), "replaced").unwrap();
    }
    assert_eq!(db.len().unwrap(), metadata.data_records);
}

// Databases written with CharSign::Unsigned keep it across compact(), which rebuilds the
// database; later opens detect it, unless told otherwise.
#[test]
fn unsigned_char_sign_roundtrip() {
    let metadata = metadata();
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::remove_file(tmp.path()).unwrap();

    {
        let mut db = OpenOptions::new()
            .char_sign(CharSign::Unsigned)
            .write(true)
            .create(true)
            .open(tmp.path())
            .unwrap();
        for kv in &metadata.data {
            db.insert(kv[0].as_str(), kv[1].as_str()).unwrap();
        }
        db.compact().unwrap();
    }

    let db = OpenOptions::new()
        .char_sign(CharSign::Unsigned)
        .open(tmp.path())
        .unwrap();
    for kv in &metadata.data {
        assert_eq!(
            db.get::<_, String>(kv[0].as_str()).unwrap().as_deref(),
            Some(kv[1].as_str())
        );
    }

    let found = |db: gdbm_native::Gdbm| {
        metadata
            .data
            .iter()
            .filter(|kv| db.contains_key(kv[0].as_str()).unwrap())
            .count()
    };

    // detected
    assert_eq!(
        found(OpenOptions::new().open(tmp.path()).unwrap()),
        metadata.data_records
    );

    // overridden with the wrong one
    let n = found(
        OpenOptions::new()
            .char_sign(CharSign::Signed)
            .open(tmp.path())
            .unwrap(),
    );
    assert!(
        n < metadata.data_records / 2,
        "found {n} keys with wrong CharSign"
    );
}
