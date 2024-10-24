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
use gdbm_native::Gdbm;
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
            let mut db = Gdbm::open(&newdb_fn, &testdb.rw_cfg()).expect("GDBM open failed");

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
        .filter_map(|test| (!test.is_basic).then_some((test.db_path.clone(), test.rw_cfg())))
        .try_for_each(|(filename, cfg)| {
            let file = NamedTempFile::new().unwrap();
            let test_filename = file.path();
            fs::copy(filename, test_filename).unwrap();

            let mut db =
                Gdbm::open(test_filename.to_str().unwrap(), &cfg).map_err(|e| e.to_string())?;

            // insert items
            (0..10000).try_for_each(|n| {
                let key = format!("key {}", n);
                let value = format!("value {}", n);

                db.insert(key.clone(), value.clone())
                    .map_err(|e| {
                        format!("inserting key \"{}\" with value \"{}\": {}", key, value, e)
                    })
                    .map(|_| ())
            })?;

            // reopen the database
            db.sync().map_err(|e| e.to_string())?;
            drop(db);
            let mut db =
                Gdbm::open(test_filename.to_str().unwrap(), &cfg).map_err(|e| e.to_string())?;

            // try_insert again (all should fail)
            (0..10000).try_for_each(|n| {
                let key = format!("key {}", n);
                let value = format!("value {}", n);

                db.try_insert(key.as_bytes().to_vec(), value.as_bytes().to_vec())
                    .map_err(|e| {
                        format!("inserting key \"{}\" with value \"{}\": {}", key, value, e)
                    })
                    .and_then(|(success, _)| {
                        (!success)
                            .then_some(())
                            .ok_or_else(|| format!("overwriting key \"{}\"", key))
                    })
            })?;

            // make sure we can get them all
            (0..10000).try_for_each(|n| {
                let key = format!("key {}", n);
                let value = format!("value {}", n);

                db.get::<_, String>(&key)
                    .map_err(|e| format!("getting key \"{}\": {}", key, e))
                    .and_then(|v| match v {
                        None => Err(format!("no value for key \"{}\"", key)),
                        Some(v) if v != value => Err(format!("wrong value for key \"{}\"", key)),
                        _ => Ok(()),
                    })
            })?;

            Ok(())
        })
        .map_err(|e: String| println!("{}", e))
        .unwrap()
}
