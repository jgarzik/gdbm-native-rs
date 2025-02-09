//
// tests/update.rs -- testing GDBM read-write APIs
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

extern crate gdbm_native;

mod common;

use common::init_tests;
use gdbm_native::OpenOptions;
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn api_remove() {
    let tests = init_tests();

    for testdb in tests {
        if testdb.is_basic {
            // Create temporary filename for writable db
            let newdb_fn = String::from(&testdb.db_path) + ".rmtest";

            // Copy existing test db to temp filepath
            fs::copy(&testdb.db_path, &newdb_fn).expect("DB File copy failed");

            // Open database for testing
            let mut db = OpenOptions::new()
                .alignment(testdb.alignment)
                .write()
                .open(&newdb_fn)
                .expect("GDBM open failed");

            // Test: remove non-existent key
            let res = db
                .remove("This key does not exist.")
                .expect("GDBM remove failed");
            assert_eq!(res, None);

            // Test: remove existing key
            let key = "key 1";
            let res = db.remove(key).expect("GDBM remove failed");
            let removed_val = res.expect("Expected some value data");
            let val1 = String::from("value 1");
            assert_eq!(removed_val, val1.as_bytes());

            // Test: validate that just-removed key is not in db anymore
            let res = db
                .contains_key(key.as_bytes())
                .expect("GDBM contains-key failed");
            assert!(!res);

            // Cleanup
            fs::remove_file(newdb_fn).expect("Test file remove failed");
        }
    }
}

#[test]
fn api_insert() {
    let tests = init_tests();

    tests
        .iter()
        .filter_map(|test| (!test.is_basic).then_some((test.db_path.clone(), test.alignment)))
        .try_for_each(|(filename, alignment)| {
            let file = NamedTempFile::new().unwrap();
            let test_filename = file.path();
            fs::copy(filename, test_filename).unwrap();

            let mut db = OpenOptions::new()
                .alignment(alignment)
                .write()
                .open(test_filename.to_str().unwrap())
                .map_err(|e| e.to_string())?;

            // insert items
            (0..10000).try_for_each(|n| {
                let key = format!("key {n}");
                let value = format!("value {n}");

                db.insert(&key, &value)
                    .map_err(|e| format!("inserting key \"{key}\" with value \"{value}\": {e}"))
                    .and_then(|_| {
                        db.try_insert(&key, &value)
                            .map_err(|e| {
                                format!("inserting key \"{key}\" with value \"{value}\": {e}")
                            })
                            .and_then(|old| old.ok_or_else(|| "try_insert should fail".to_string()))
                    })
                    .map(|_| ())
            })?;

            // try_insert again (all should fail)
            (0..10000).try_for_each(|n| {
                let key = format!("key {n}");
                let value = format!("value {n}");

                db.try_insert(&key, &value)
                    .map_err(|e| format!("inserting key \"{key}\" with value \"{value}\": {e}"))
                    .and_then(|old| {
                        old.ok_or_else(|| "try_insert should fail".to_string())
                            .map(|_| ())
                    })
            })?;

            // make sure we can get them all
            (0..10000).try_for_each(|n| {
                let key = format!("key {n}");
                let value = format!("value {n}");

                db.get::<_, String>(&key)
                    .map_err(|e| format!("getting key \"{key}\": {e}"))
                    .and_then(|v| match v {
                        None => Err(format!("no value for key \"{key}\"")),
                        Some(v) if v != value => Err(format!("wrong value for key \"{key}\"")),
                        _ => Ok(()),
                    })
            })?;

            Ok(())
        })
        .map_err(|e: String| println!("{e}"))
        .unwrap();
}
