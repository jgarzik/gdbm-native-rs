extern crate rs_gdbm;

mod common;

use std::fs;
use common::init_tests;
use rs_gdbm::Gdbm;

#[test]
fn api_remove() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        if testdb.is_basic {
            // Create temporary filename for writable db
            let newdb_fn = String::from(&testdb.db_path) + ".rmtest";

            // Copy existing test db to temp filepath
            fs::copy(&testdb.db_path, &newdb_fn).expect("DB File copy failed");

            // Open database for testing
            let mut db = Gdbm::open(&newdb_fn, &testcfg.def_rw_cfg).expect("GDBM open failed");

            // Test: remove non-existent key
            let keystr = String::from("This key does not exist.");
            let res = db.remove(keystr.as_bytes()).expect("GDBM remove failed");
            assert_eq!(res, None);

            // Test: remove existing key
            let key1 = String::from("key 1");
            let res = db.remove(key1.as_bytes()).expect("GDBM remove failed");
            let removed_val = res.expect("Expected some value data");
            let val1 = String::from("value 1");
            assert_eq!(removed_val, val1.as_bytes());

            // Cleanup
            fs::remove_file(newdb_fn).expect("Test file remove failed");
        }
    }
}
