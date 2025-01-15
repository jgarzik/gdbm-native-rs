extern crate gdbm_native;

use gdbm_native::{
    Alignment::{Align32, Align64},
    BlockSize,
    Endian::{Big, Little},
    Magic,
    Offset::{Small, LFS},
    OpenOptions,
};
use tempfile::tempdir;

#[test]
// Create fails if the file exists, but isn't a db.
fn api_open_create() {
    let dir = tempdir().unwrap();

    let no_db = dir.path().join("no");
    let bad_db = dir.path().join("bad");
    std::fs::write(&bad_db, "stuff").expect("creating a DB file");

    assert!(OpenOptions::new().write().create().open(no_db).is_ok());
    assert!(OpenOptions::new().write().create().open(bad_db).is_err());
}

#[test]
fn tempfile() {
    assert!(OpenOptions::new().write().create().tempfile().is_ok());
}

#[test]
// Test for correct magic for new databases.
fn api_open_newdb_magic() {
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
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

        OpenOptions::new().write().create().alignment(Some(alignment)).offset(Some(offset)).endian(Some(endian)).numsync(numsync).open(
            &path
        )
        .map_err(|e| format!(
            "creating: alignment: {alignment:?}, offset: {offset:?}, endian: {endian:?}, numsync: {numsync}, expected: {expected_magic:?}, newdb error: {e}",
        ))?;

        OpenOptions::new().alignment(Some(alignment)).open(
            &path,
        )
        .map_err(|e| format!(
            "opening: alignment: {alignment:?}, offset: {offset:?}, endian: {endian:?}, numsync: {numsync}, expected: {expected_magic:?}, open error: {e}",
        ))
        .and_then(|db| {
            (db.magic() == expected_magic)
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "wrong magic: alignment: {alignment:?}, offset: {offset:?}, endian: {endian:?}, numsync: {numsync}, expected: {expected_magic:?}, got: {:?}",
                        db.magic()
                    )
                })
        })
    })
    .unwrap_or_else(|e: String| panic!("{e}"));
}

#[test]
// Test for valid blocksizes.
fn api_open_bsexact() {
    [
        (256, Err(())), // aligned, but too small
        (511, Err(())), // not aligned and too small
        (512, Ok(())),  // minimum
        (768, Err(())), // not aligned
        (1024, Ok(())), // aligned
    ]
    .into_iter()
    .try_for_each(|(block_size, expected)| {
        let dir = tempdir().unwrap();
        let db = dir.path().join("db");
        match OpenOptions::new()
            .write()
            .create()
            .block_size(BlockSize::Exactly(block_size))
            .open(&db)
        {
            Ok(_) if expected.is_ok() => Ok(()),
            Err(_) if expected.is_err() => Ok(()),
            Ok(_) => Err(format!("blocksize: {block_size}, newdb opened")),
            Err(e) => Err(format!("blocksize: {block_size}, newdb error: {e}")),
        }
    })
    .unwrap_or_else(|e: String| panic!("bsexact unexpected: {e}"));
}

#[test]
fn api_open_cachesize() {
    const RECORD_COUNT: usize = 1000; // buckets will occupy around 20k

    fn the_test(cachesize: Option<usize>) -> Result<(), String> {
        let dir = tempdir().unwrap();
        let db = dir.path().join("testdb");

        // Create a database using configured cachesize.
        OpenOptions::new()
            .cachesize(cachesize)
            .write()
            .create()
            .open(&db)
            .map_err(|e| format!("write open failed: {e}"))
            .and_then(|mut db| {
                (0..RECORD_COUNT).try_for_each(|n| {
                    db.insert(&n, &vec![])
                        .map(|_| ())
                        .map_err(|e| format!("insert failed: {e}"))
                })
            })?;

        // Read a database using configured cachesize.
        OpenOptions::new()
            .cachesize(cachesize)
            .open(&db)
            .map_err(|e| format!("read open failed: {e}"))
            .and_then(|mut db| {
                (0..RECORD_COUNT).try_for_each(|n| {
                    db.get(&n)
                        .map_err(|e| e.to_string())
                        .and_then(|v| {
                            (v == Some(vec![]))
                                .then_some(())
                                .ok_or_else(|| "wrong value".to_string())
                        })
                        .map_err(|e| format!("get failed: {e}"))
                })
            })
    }

    [Some(0), Some(100000)]
        .into_iter()
        .try_for_each(|cachesize| {
            the_test(cachesize).map_err(|e| format!("cachesize: {cachesize:?}]: {e}"))
        })
        .unwrap_or_else(|e| panic!("{e}"));
}
