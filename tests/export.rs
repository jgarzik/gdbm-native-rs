
extern crate gdbm_native;

mod common;

use std::{fs, fs::OpenOptions};

use common::init_tests;
use gdbm_native::{ExportBinMode, Gdbm};

#[test]
fn api_export_bin() {
    const EXPORT_FN: &'static str = "./export.bin";

    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let mut outf = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(EXPORT_FN)
            .unwrap();

        let _iores = db.export_bin(&mut outf, ExportBinMode::ExpNative).unwrap();
        fs::remove_file(EXPORT_FN).unwrap();

        // TODO: once Store is implemented, import the exported data
        // into a new db, and verify that old & new dbs match.
    }
}

#[test]
fn api_export_ascii() {
    const EXPORT_FN: &'static str = "./export.txt";

    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let mut outf = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(EXPORT_FN)
            .unwrap();

        let _iores = db.export_ascii(&mut outf).unwrap();
        fs::remove_file(EXPORT_FN).unwrap();

        // TODO: once Store is implemented, import the exported data
        // into a new db, and verify that old & new dbs match.
    }
}
