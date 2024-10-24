//
// tests/read.rs -- testing GDBM read-only APIs
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

#[test]
fn api_exists_not() {
    let tests = init_tests();

    for testdb in tests {
        let mut db = Gdbm::open(&testdb.db_path, &testdb.ro_cfg()).unwrap();
        assert!(!db.contains_key(b"dummy").unwrap());

        if testdb.is_basic {
            assert!(!db.contains_key(b"key -111").unwrap());
        }
    }
}

#[test]
fn api_exists() {
    let tests = init_tests();

    for testdb in tests {
        if testdb.is_basic {
            let mut db = Gdbm::open(&testdb.db_path, &testdb.ro_cfg()).unwrap();

            for n in 0..10001 {
                let keystr = format!("key {}", n);
                assert!(db.contains_key(keystr.as_bytes()).unwrap());
            }
        }
    }
}

#[test]
fn api_get_not() {
    let tests = init_tests();

    for testdb in tests {
        let mut db = Gdbm::open(&testdb.db_path, &testdb.ro_cfg()).unwrap();
        let res = db.get::<&str, String>("This key does not exist").unwrap();
        assert_eq!(res, None);
    }
}

#[test]
fn api_get() {
    let tests = init_tests();

    for testdb in tests {
        if testdb.is_basic {
            let mut db = Gdbm::open(&testdb.db_path, &testdb.ro_cfg()).unwrap();

            for n in 0..10001 {
                let keystr = format!("key {}", n);
                let valstr = format!("value {}", n);
                assert_eq!(db.get(&keystr).unwrap(), Some(valstr));
            }
        }
    }
}

#[test]
fn api_open_close() {
    let tests = init_tests();

    for testdb in tests {
        let _res = Gdbm::open(&testdb.db_path, &testdb.ro_cfg()).unwrap();
        // implicit close when scope closes
    }
}

#[test]
fn api_len() {
    let tests = init_tests();

    for testdb in tests {
        let mut db = Gdbm::open(&testdb.db_path, &testdb.ro_cfg()).unwrap();
        let res = db.len().unwrap();
        assert_eq!(res, testdb.n_records);
    }
}
