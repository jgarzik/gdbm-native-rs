extern crate gdbm_native;

use gdbm_native::{
    magic::Magic,
    ser::{
        Alignment::{Align32, Align64},
        Endian::{Big, Little},
        Offset::{Small, LFS},
    },
    Gdbm, GdbmOptions,
};
use tempfile::NamedTempFile;

#[test]
// Test for conflicting newdb, creat and readonly flags.
fn api_open_conflicting() {
    let old_db = NamedTempFile::new().expect("creating a temporary file");

    [
        (false, false, false, false),
        (false, false, true, false),
        (false, true, false, false),
        (false, true, true, false),
        (true, false, false, false),
        (true, false, true, true),
        (true, true, false, true),
        (true, true, true, true),
    ]
    .into_iter()
    .try_for_each(|(readonly, creat, newdb, expected_arg_conflict)| {
        std::fs::write(old_db.path(), "not a remotely valid database")
            .expect("creating a bad database");

        Gdbm::open(
            old_db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly,
                creat,
                newdb,
                alignment: None,
                offset: None,
                endian: None,
                block_size: None,
                bsexact: false,
                numsync: false,
                cachesize: None,
            },
        )
        .map_err(|e| e.to_string())
        .and_then(|_| Err("success".to_string()))
        .or_else(|e| match e == "readonly conflicts with newdb or creat" {
            true if expected_arg_conflict => Ok(()),
            false if !expected_arg_conflict => Ok(()),
            _ => Err(format!(
                "readonly: {}, creat: {}, newdb: {}, expecting_confilict: {}",
                readonly, creat, newdb, expected_arg_conflict
            )),
        })
    })
    .unwrap_or_else(|e: String| panic!("{}", e));
}

#[test]
// Non-empty, but invalid, DB causes creat to fail (bad format)
// Empty DB allows creat to succeed.
// Regardless of content, newdb always succeeds.
fn api_open_creat_newdb() {
    let old_db = NamedTempFile::new().expect("creating a temporary file");

    let baddb_content = b"bad DB content".to_vec();
    let empty_content = vec![];
    [
        (false, false, &baddb_content, Err(())),
        (false, false, &empty_content, Err(())),
        (false, true, &baddb_content, Err(())),
        (false, true, &empty_content, Ok(())),
        (true, false, &baddb_content, Ok(())),
        (true, false, &empty_content, Ok(())),
        (true, true, &baddb_content, Ok(())),
        (true, true, &empty_content, Ok(())),
    ]
    .into_iter()
    .try_for_each(|(newdb, creat, content, expected)| {
        std::fs::write(old_db.path(), content).expect("creating a DB file");

        match Gdbm::open(
            old_db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly: false,
                creat,
                newdb,
                alignment: None,
                offset: None,
                endian: None,
                block_size: None,
                bsexact: false,
                numsync: false,
                cachesize: None,
            },
        ) {
            Ok(_) if expected.is_ok() => Ok(()),
            Err(_) if expected.is_err() => Ok(()),
            _ => Err(format!(
                "newdb: {}, creat: {}, empty content: {}, expected: {:?}",
                newdb,
                creat,
                content.is_empty(),
                expected
            )),
        }
    })
    .unwrap_or_else(|e: String| panic!("{}", e));
}

#[test]
// Test for correct magic for new databases.
fn api_open_newdb_magic() {
    let old_db = NamedTempFile::new().expect("creating a temporary file");

    [
        (Align32, LFS, Big, false, Magic::BE64),
        (Align32, LFS, Little, false, Magic::LE64),
        (Align32, Small, Big, false, Magic::BE32),
        (Align32, Small, Little, false, Magic::LE32),
        (Align64, LFS, Big, false, Magic::BE64),
        (Align64, LFS, Little, false, Magic::LE64),
        (Align64, Small, Big, false, Magic::BE32),
        (Align64, Small, Little, false, Magic::LE32),
        (Align32, LFS, Big, true, Magic::BE64NS),
        (Align32, LFS, Little, true, Magic::LE64NS),
        (Align32, Small, Big, true, Magic::BE32NS),
        (Align32, Small, Little, true, Magic::LE32NS),
        (Align64, LFS, Big, true, Magic::BE64NS),
        (Align64, LFS, Little, true, Magic::LE64NS),
        (Align64, Small, Big, true, Magic::BE32NS),
        (Align64, Small, Little, true, Magic::LE32NS),
    ]
    .into_iter()
    .try_for_each(|(alignment, offset, endian, numsync, expected_magic)| {
        Gdbm::open(
            old_db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly: false,
                creat: false,
                newdb: true,
                alignment: Some(alignment),
                offset: Some(offset),
                endian: Some(endian),
                block_size: None,
                bsexact: false,
                numsync,
                cachesize: None,
            },
        )
        .and_then(|mut db| {
            println!("magic: {:?}", db.header.magic);
            db.sync()})
        .map_err(|e| format!(
            "alignment: {:?}, offset: {:?}, endian: {:?}, numsync: {}, expected: {:?}, newdb error: {}",
            alignment, offset, endian, numsync, expected_magic, e
        ))?;

        Gdbm::open(
            old_db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly: true,
                creat: false,
                newdb: false,
                alignment: Some(alignment),
                offset: None,
                endian: None,
                block_size: None,
                bsexact: false,
                numsync: false,
                cachesize: None,
            },
        )
        .map_err(|e| format!(
            "alignment: {:?}, offset: {:?}, endian: {:?}, numsync: {}, expected: {:?}, open error: {}",
            alignment, offset, endian, numsync, expected_magic, e
        ))
        .and_then(|db| {
            (db.header.magic == expected_magic)
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "alignment: {:?}, offset: {:?}, endian: {:?}, numsync: {}, expected: {:?}, got: {:?}",
                        alignment, offset, endian, numsync, expected_magic, db.header.magic
                    )
                })
        })
    })
    .unwrap_or_else(|e: String| panic!("{}", e));
}

#[test]
// Test for valid blocksizes.
fn api_open_bsexact() {
    let old_db = NamedTempFile::new().expect("creating a temporary file");

    [
        (256, Err(())), // aligned, but too small
        (511, Err(())), // not aligned and too small
        (512, Ok(())),  // minimum
        (768, Err(())), // not aligned
        (1024, Ok(())), // aligned
    ]
    .into_iter()
    .try_for_each(|(block_size, expected)| {
        match Gdbm::open(
            old_db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly: false,
                creat: false,
                newdb: true,
                alignment: None,
                offset: None,
                endian: None,
                block_size: Some(block_size),
                bsexact: true,
                numsync: false,
                cachesize: None,
            },
        ) {
            Ok(_) if expected.is_ok() => Ok(()),
            Err(_) if expected.is_err() => Ok(()),
            Ok(_) => Err(format!("blocksize: {}, newdb opened", block_size)),
            Err(e) => Err(format!("blocksize: {}, newdb error: {}", block_size, e)),
        }
    })
    .unwrap_or_else(|e: String| panic!("bsexact unexpected: {}", e));
}

#[test]
fn api_open_cachesize() {
    const RECORD_COUNT: usize = 1000; // buckets will occupy around 20k

    fn the_test(cachesize: Option<usize>) -> Result<(), String> {
        let db = NamedTempFile::new().unwrap();

        // Create a database using configured cachesize.
        Gdbm::open(
            db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly: false,
                creat: false,
                newdb: true,
                alignment: None,
                offset: None,
                endian: None,
                block_size: None,
                bsexact: false,
                numsync: true,
                cachesize,
            },
        )
        .map_err(|e| format!("write open failed: {}", e))
        .and_then(|mut db| {
            (0..RECORD_COUNT)
                .try_for_each(|n| {
                    db.insert(n, vec![])
                        .map(|_| ())
                        .map_err(|e| format!("insert failed: {}", e))
                })
                .and_then(|()| db.sync().map_err(|e| format!("sync failed: {}", e)))
        })?;

        // Read a database using configured cachesize.
        Gdbm::open(
            db.path().to_str().unwrap(),
            &GdbmOptions {
                readonly: true,
                creat: false,
                newdb: false,
                alignment: None,
                offset: None,
                endian: None,
                block_size: None,
                bsexact: false,
                numsync: true,
                cachesize,
            },
        )
        .map_err(|e| format!("read open failed: {}", e))
        .and_then(|mut db| {
            (0..RECORD_COUNT).try_for_each(|n| {
                db.get(&n)
                    .map_err(|e| e.to_string())
                    .and_then(|v| {
                        (v == Some(vec![]))
                            .then_some(())
                            .ok_or_else(|| "wrong value".to_string())
                    })
                    .map_err(|e| format!("get failed: {}", e))
            })
        })
    }

    [Some(0), Some(100000)]
        .into_iter()
        .try_for_each(|cachesize| {
            the_test(cachesize).map_err(|e| format!("cachesize: {:?}]: {}", cachesize, e))
        })
        .unwrap_or_else(|e| panic!("{}", e));
}
