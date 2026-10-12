//
// tests/api.rs -- testing API conventions: errors, traits, close
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use gdbm_native::{Error, Gdbm, OpenOptions};
use tempfile::tempdir;

#[test]
// Error works with `?` into Box<dyn Error>, displays a message rather than its Debug form, and
// exposes I/O causes.
fn errors() {
    fn open(path: &std::path::Path) -> std::result::Result<Gdbm, Box<dyn std::error::Error>> {
        Ok(OpenOptions::new().open(path)?)
    }

    let dir = tempdir().unwrap();
    let missing = open(&dir.path().join("missing")).unwrap_err();
    let missing = missing.downcast_ref::<Error>().unwrap();
    assert!(std::error::Error::source(missing).is_some());

    let bad = dir.path().join("bad");
    std::fs::write(&bad, [0u8; 512]).unwrap();
    let err = open(&bad).unwrap_err().to_string();
    assert!(err.contains("not a GDBM database"), "{err}");
}

#[test]
fn close_persists() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut db = OpenOptions::new().create(true).open(&path).unwrap();
    assert!(db.is_empty().unwrap());
    db.insert("key", "value").unwrap();
    assert!(!db.is_empty().unwrap());
    db.close().unwrap();

    let db = OpenOptions::new().open(&path).unwrap();
    assert_eq!(
        db.get::<_, String>("key").unwrap().as_deref(),
        Some("value")
    );
    db.close().unwrap();
}

#[test]
fn debug_and_send() {
    fn send<T: Send>(_: &T) {}

    let db = OpenOptions::new().tempfile().unwrap();
    send(&db);
    let debug = format!("{db:?}");
    assert!(
        debug.contains("Gdbm") && debug.contains("Off64Align8"),
        "{debug}"
    );
}

#[test]
fn byte_array_keys_and_values() {
    let mut db = OpenOptions::new().tempfile().unwrap();
    db.insert(&[1u8, 2, 3], &42u32.to_be_bytes()).unwrap();
    let value: Option<[u8; 4]> = db.get(&[1u8, 2, 3]).unwrap();
    assert_eq!(value.map(u32::from_be_bytes), Some(42));
    assert!(db.get::<_, [u8; 2]>(&[1u8, 2, 3]).is_err());
}
