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
                                db.get(&kv[0]).map_err(|e| e.to_string()).and_then(|v| {
                                    (v == Some(kv[1].clone())).then_some(()).ok_or_else(|| {
                                        format!("expected: {:?}, got: {:?}", v, kv[1])
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
                    db.get(&kv[0]).map_err(|e| e.to_string()).and_then(|got| {
                        (got == Some(kv[1].clone()))
                            .then_some(())
                            .ok_or_else(|| format!("expected: {:?}, got: {:?}", kv[1], got))
                    })
                })
            })
            .unwrap_or_else(|e| panic!("{}", e));
    }
}
