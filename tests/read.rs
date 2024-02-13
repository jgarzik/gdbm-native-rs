
extern crate gdbm_native;

mod common;

use common::init_tests;
use gdbm_native::Gdbm;
use std::collections::HashMap;

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

#[test]
fn api_get_not() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let keystr = String::from("This key does not exist.");
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let res = db.get(keystr.as_bytes()).unwrap();
        assert_eq!(res, None);
    }
}

#[test]
fn api_get() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        if testdb.is_basic {
            let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();

            for n in 0..10001 {
                let keystr = format!("key {}", n);
                let res = db.get(keystr.as_bytes());
                match res {
                    Ok(opt) => match opt {
                        None => {
                            assert!(false);
                        }
                        Some(val) => {
                            let valstr = format!("value {}", n);
                            assert_eq!(val, valstr.as_bytes());
                        }
                    },
                    Err(_e) => {
                        assert!(false);
                    }
                }
            }
        }
    }
}

#[test]
fn api_first_next_key() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        if testdb.is_basic {
            // build internal map of keys expected to be present in basic.db
            let mut keys_remaining: HashMap<Vec<u8>, bool> = HashMap::new();
            for n in 0..10001 {
                let keystr = format!("key {}", n);
                keys_remaining.insert(keystr.as_bytes().to_vec(), true);
            }

            // simple verf of correct map construction
            assert_eq!(keys_remaining.len(), testdb.n_records);

            // open basic.db
            let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();

            // iterate through each key in db
            let mut key_res = db.first_key().unwrap();
            while key_res != None {
                let key = key_res.unwrap();

                // remove iteration key from internal map
                assert_ne!(keys_remaining.remove(&key), None);

                key_res = db.next_key(&key).unwrap();
            }

            // if internal map is empty, success
            assert_eq!(keys_remaining.len(), 0);
        }
    }
}

#[test]
fn api_open_close() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let _res = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        // implicit close when scope closes
    }
}

#[test]
fn api_len() {
    let testcfg = init_tests();

    for testdb in &testcfg.tests {
        let mut db = Gdbm::open(&testdb.db_path, &testcfg.def_ro_cfg).unwrap();
        let res = db.len().unwrap();
        assert_eq!(res, testdb.n_records);
    }
}
