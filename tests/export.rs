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

use std::{fs, fs::OpenOptions};

use common::init_tests;
use gdbm_native::{ExportBinMode, Gdbm};

#[test]
fn api_export_bin() {
    const EXPORT_FN: &str = "./export.bin";

    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let mut outf = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(EXPORT_FN)
            .unwrap();

        db.export_bin(&mut outf, ExportBinMode::ExpNative).unwrap();
        fs::remove_file(EXPORT_FN).unwrap();

        // TODO: once Store is implemented, import the exported data
        // into a new db, and verify that old & new dbs match.
    }
}

#[test]
fn api_export_ascii() {
    const EXPORT_FN: &str = "./export.txt";

    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let mut outf = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(EXPORT_FN)
            .unwrap();

        db.export_ascii(&mut outf).unwrap();
        fs::remove_file(EXPORT_FN).unwrap();

        // TODO: once Store is implemented, import the exported data
        // into a new db, and verify that old & new dbs match.
    }
}
