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

use tempfile::tempdir;

use common::init_tests;
use gdbm_native::{BinDumpFormat, Error, ImportMode, OpenOptions};

#[test]
fn api_export_bin() {
    init_tests()
        .into_iter()
        .try_for_each(|test| {
            [
                BinDumpFormat::Long32,
                BinDumpFormat::Long64Le,
                BinDumpFormat::Long64Be,
            ]
            .into_iter()
            .try_for_each(|mode| {
                let dir = tempdir().unwrap();

                // make a binary dump
                let dumpfile = dir.path().join("dumpfile");
                std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&dumpfile)
                    .map_err(|e| e.to_string())
                    .and_then(|mut f| {
                        OpenOptions::new()
                            .open(&test.db_path)
                            .and_then(|db| db.export_bin(&mut f, mode))
                            .map_err(|e| e.to_string())
                    })
                    .unwrap();

                // import into a fresh database
                let importdb = dir.path().join("importdb");
                std::fs::OpenOptions::new()
                    .read(true)
                    .open(&dumpfile)
                    .map_err(|e| e.to_string())
                    .and_then(|mut f| {
                        OpenOptions::new()
                            .write(true)
                            .create(true)
                            .open(&importdb)
                            // the format is detected
                            .and_then(|mut db| db.import_bin(&mut f, None, ImportMode::Replace))
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    })
                    .unwrap();

                // compare the databases
                OpenOptions::new()
                    .open(&importdb)
                    .map_err(|e| e.to_string())
                    .and_then(|db| {
                        test.metadata.data.iter().try_for_each(|kv| {
                            db.get(&kv[0]).map_err(|e| e.to_string()).and_then(|v| {
                                (v == Some(kv[1].clone()))
                                    .then_some(())
                                    .ok_or_else(|| format!("expected: {v:?}, got: {:?}", kv[1]))
                            })
                        })
                    })
            })
        })
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn api_export_ascii() {
    let tests = init_tests();

    for testdb in tests {
        let dir = tempdir().unwrap();

        // make an ascii dump
        let dumpfile = dir.path().join("dumpfile");
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&dumpfile)
            .map_err(|e| e.to_string())
            .and_then(|mut f| {
                OpenOptions::new()
                    .open(&testdb.db_path)
                    .and_then(|db| db.export_ascii(&mut f, None::<&str>))
                    .map_err(|e| e.to_string())
            })
            .unwrap();

        // import into a fresh database
        let importdb = dir.path().join("importdb");
        std::fs::OpenOptions::new()
            .read(true)
            .open(&dumpfile)
            .map_err(|e| e.to_string())
            .and_then(|mut f| {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&importdb)
                    .and_then(|mut db| db.import_ascii(&mut f, ImportMode::Replace))
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
            .unwrap();

        // compare the databases
        OpenOptions::new()
            .open(importdb)
            .map_err(|e| e.to_string())
            .and_then(|db| {
                testdb.metadata.data.iter().try_for_each(|kv| {
                    db.get(&kv[0]).map_err(|e| e.to_string()).and_then(|got| {
                        (got == Some(kv[1].clone()))
                            .then_some(())
                            .ok_or_else(|| format!("expected: {:?}, got: {:?}", kv[1], got))
                    })
                })
            })
            .unwrap_or_else(|e| panic!("{e}"));
    }
}

fn dump_ascii(db: &mut gdbm_native::Gdbm) -> Vec<u8> {
    let mut dump = Vec::new();
    db.export_ascii(&mut dump, None::<&str>).unwrap();
    dump
}

#[test]
// Empty values round trip, written with an empty data line (which, unlike C GDBM's own
// encoding, C GDBM's gdbm_load accepts), and the header records numsync.
fn ascii_empty_values_and_format() {
    let dir = tempdir().unwrap();
    for numsync in [false, true] {
        let mut db = OpenOptions::new()
            .write(true)
            .create(true)
            .numsync(numsync)
            .open(dir.path().join(format!("src{numsync}")))
            .unwrap();
        db.insert("empty", "").unwrap();
        db.insert("full", "value").unwrap();
        let dump = dump_ascii(&mut db);
        let text = String::from_utf8(dump.clone()).unwrap();
        assert!(text.contains("#:len=0\n\n#:len="), "{text}");
        let format = if numsync { "numsync" } else { "standard" };
        assert!(text.contains(&format!("\n#:format={format}\n")), "{text}");

        let mut copy = OpenOptions::new()
            .write(true)
            .create(true)
            .open(dir.path().join(format!("dst{numsync}")))
            .unwrap();
        let info = copy
            .import_ascii(&mut dump.as_slice(), ImportMode::Replace)
            .unwrap();
        assert_eq!((info.count, info.numsync), (2, Some(numsync)));
        assert_eq!(copy.get::<_, String>("empty").unwrap().as_deref(), Some(""));
        assert_eq!(
            copy.get::<_, String>("full").unwrap().as_deref(),
            Some("value")
        );
    }
}

#[test]
// ImportMode::Insert refuses to replace existing keys; Replace overwrites them.
fn import_modes() {
    let dir = tempdir().unwrap();
    let mut src = OpenOptions::new()
        .write(true)
        .create(true)
        .open(dir.path().join("src"))
        .unwrap();
    src.insert("key", "new").unwrap();
    let dump = dump_ascii(&mut src);
    let mut bin = Vec::new();
    src.export_bin(&mut bin, BinDumpFormat::host()).unwrap();

    let mut db = OpenOptions::new()
        .write(true)
        .create(true)
        .open(dir.path().join("dst"))
        .unwrap();
    db.insert("key", "old").unwrap();

    assert!(matches!(
        db.import_ascii(&mut dump.as_slice(), ImportMode::Insert),
        Err(Error::KeyExists { key }) if key == b"key"
    ));
    assert!(matches!(
        db.import_bin(&mut bin.as_slice(), None, ImportMode::Insert),
        Err(Error::KeyExists { .. })
    ));
    assert_eq!(db.get::<_, String>("key").unwrap().as_deref(), Some("old"));

    db.import_bin(&mut bin.as_slice(), None, ImportMode::Replace)
        .unwrap();
    assert_eq!(db.get::<_, String>("key").unwrap().as_deref(), Some("new"));
}

#[test]
// Bug: binary dump format detection looked only at the first 8 bytes after the header, so a
// Long32 dump whose first key begins with 4 zero bytes was taken for Long64Le, and one whose
// first key is empty for Long64Be.
fn detect_bin_format_zero_prefixed_or_empty_first_key() {
    for key in [&[0u8, 0, 0, 0, 1, 2, 3, 4][..], &[0, 0, 0, 0], &[]] {
        for format in [
            BinDumpFormat::Long32,
            BinDumpFormat::Long64Le,
            BinDumpFormat::Long64Be,
        ] {
            let mut src = OpenOptions::new().tempfile().unwrap();
            src.insert(key, "value").unwrap();
            src.insert("second", "x").unwrap();
            let mut dump = Vec::new();
            src.export_bin(&mut dump, format).unwrap();

            let mut dst = OpenOptions::new().tempfile().unwrap();
            let count = dst
                .import_bin(&mut dump.as_slice(), None, ImportMode::Replace)
                .unwrap_or_else(|e| panic!("{format:?} key {key:?}: {e}"));
            assert_eq!(count, 2, "{format:?} key {key:?}");
            assert_eq!(
                dst.get::<_, String>(key).unwrap().as_deref(),
                Some("value"),
                "{format:?} key {key:?}"
            );
        }
    }
}
