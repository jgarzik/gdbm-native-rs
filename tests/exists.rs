extern crate rs_gdbm;

mod common;

use common::init_tests;
use rs_gdbm::Gdbm;

#[test]
fn api_exists_not() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let res = db.contains_key(b"dummy").unwrap();
        assert_eq!(res, false);

        if testdb.is_basic {
            db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
            let res = db.contains_key(b"key -111").unwrap();
            assert_eq!(res, false);
        }
    }
}

#[test]
fn api_exists() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        if testdb.is_basic {
            let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();

            for n in 0..10001 {
                let keystr = format!("key {}", n);
                let res = db.contains_key(keystr.as_bytes()).unwrap();
                assert_eq!(res, true);
            }
        }
    }
}
