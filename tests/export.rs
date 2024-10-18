//
// tests/export.rs -- testing GDBM export APIs
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

extern crate gdbm_native;

mod common;

use std::fs::OpenOptions;
use tempfile::NamedTempFile;

use common::{creat_cfg, default_cfg, init_tests};
use gdbm_native::{ExportBinMode, Gdbm};

#[test]
fn api_export_bin() {
    init_tests()
        .into_iter()
        .try_for_each(|test| {
            [ExportBinMode::Exp32, ExportBinMode::Exp64]
                .into_iter()
                .try_for_each(|mode| {
                    let mut dumpfile = NamedTempFile::new().unwrap();

                    // make an ascii dump
                    OpenOptions::new()
                        .write(true)
                        .open(dumpfile.path())
                        .and_then(|mut f| {
                            Gdbm::open(&test.db_path, &test.ro_cfg())
                                .and_then(|mut db| db.export_bin(&mut f, mode))
                        })
                        .unwrap();

                    // import into a fresh database
                    let importdb = NamedTempFile::new().unwrap();
                    Gdbm::open(importdb.path().to_str().unwrap(), &creat_cfg())
                        .and_then(|mut db| {
                            db.import_bin(&mut dumpfile, mode).and_then(|_| db.sync())
                        })
                        .unwrap();

                    // compare the databases
                    Gdbm::open(importdb.path().to_str().unwrap(), &default_cfg())
                        .map_err(|e| e.to_string())
                        .and_then(|mut db| {
                            test.metadata.data.iter().try_for_each(|kv| {
                                let key = kv[0].as_ref();
                                db.get(key).map_err(|e| e.to_string()).and_then(|v| {
                                    let expected = Some(kv[1].as_bytes().to_vec());
                                    (v == expected).then_some(()).ok_or_else(|| {
                                        format!("expected: {:?}, got: {:?}", v, expected)
                                    })
                                })
                            })
                        })
                })
        })
        .unwrap_or_else(|e| panic!("{}", e));
}

#[test]
fn api_export_ascii() {
    let tests = init_tests();

    for testdb in tests {
        let mut dumpfile = NamedTempFile::new().unwrap();

        // make an ascii dump
        OpenOptions::new()
            .write(true)
            .open(dumpfile.path())
            .and_then(|mut f| {
                Gdbm::open(&testdb.db_path, &testdb.ro_cfg())
                    .and_then(|mut db| db.export_ascii(&mut f))
            })
            .unwrap();

        // import into a fresh database
        let importdb = NamedTempFile::new().unwrap();
        Gdbm::open(importdb.path().to_str().unwrap(), &creat_cfg())
            .and_then(|mut db| db.import_ascii(&mut dumpfile).and_then(|_| db.sync()))
            .unwrap();

        // compare the databases
        Gdbm::open(importdb.path().to_str().unwrap(), &default_cfg())
            .map_err(|e| e.to_string())
            .and_then(|mut db| {
                testdb.metadata.data.iter().try_for_each(|kv| {
                    let key = kv[0].as_ref();
                    db.get(key).map_err(|e| e.to_string()).and_then(|got| {
                        let expected = Some(kv[1].as_bytes().to_vec());
                        (got == expected).then_some(()).ok_or_else(|| {
                            format!(
                                "expected: {:?}, got: {:?} ({:?})",
                                expected.as_ref().map(|v| std::str::from_utf8(v.as_ref())),
                                got.as_ref().map(|v| std::str::from_utf8(v)),
                                got
                            )
                        })
                    })
                })
            })
            .unwrap_or_else(|e| panic!("{}", e));
    }
}
