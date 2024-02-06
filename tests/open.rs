extern crate rs_gdbm;

mod common;

use common::init_tests;
use rs_gdbm::Gdbm;

#[test]
fn api_open_close() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let _res = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        // implicit close when scope closes
    }
}

