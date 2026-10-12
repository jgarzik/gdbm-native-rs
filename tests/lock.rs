//
// tests/lock.rs -- testing database file locking
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

extern crate gdbm_native;

use gdbm_native::{Error, OpenOptions};
use tempfile::tempdir;

fn is_locked<T>(result: gdbm_native::Result<T>) -> bool {
    matches!(result, Err(Error::Locked))
}

#[test]
fn writer_excludes_writers_and_readers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    let db = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    assert!(is_locked(OpenOptions::new().write(true).open(&path)));
    assert!(is_locked(
        OpenOptions::new().write(true).create(true).open(&path)
    ));
    assert!(is_locked(OpenOptions::new().open(&path)));

    drop(db);
    assert!(OpenOptions::new().write(true).open(&path).is_ok());
}

#[test]
fn readers_share() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    drop(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap(),
    );

    let r1 = OpenOptions::new().open(&path).unwrap();
    let r2 = OpenOptions::new().open(&path).unwrap();
    assert!(is_locked(OpenOptions::new().write(true).open(&path)));

    drop((r1, r2));
    assert!(OpenOptions::new().write(true).open(&path).is_ok());
}

#[test]
fn lock_disabled() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    let mut w = OpenOptions::new()
        .lock(false)
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    w.sync().unwrap();
    assert!(OpenOptions::new().lock(false).open(&path).is_ok());
    assert!(
        OpenOptions::new()
            .lock(false)
            .write(true)
            .open(&path)
            .is_ok()
    );
}
