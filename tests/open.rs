extern crate gdbm_native;

use gdbm_native::{
    Alignment::{Align32, Align64},
    BlockSize,
    Endian::{Big, Little},
    Magic,
    Offset::{Small, LFS},
    OpenOptions,
};
use tempfile::NamedTempFile;

#[test]
// Non-empty, but invalid, DB causes creat to fail (bad format)
// Empty DB allows creat to succeed.
// Regardless of content, newdb always succeeds.
fn api_open_creat_newdb() {
    let old_db = NamedTempFile::new().expect("creating a temporary file");

    let baddb_content = b"bad DB content".to_vec();
    let empty_content = vec![];
    [
        (false, &baddb_content, Err(())),
        (false, &empty_content, Ok(())),
        (true, &baddb_content, Ok(())),
        (true, &empty_content, Ok(())),
    ]
    .into_iter()
    .try_for_each(|(newdb, content, expected)| {
        std::fs::write(old_db.path(), content).expect("creating a DB file");

        match OpenOptions::new()
            .write()
            .create()
            .newdb(newdb)
            .open(old_db.path())
        {
            Ok(_) if expected.is_ok() => Ok(()),
            Err(_) if expected.is_err() => Ok(()),
            _ => Err(format!(
                "newdb: {}, empty content: {}, expected: {:?}",
                newdb,
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
        OpenOptions::new().write().create().newdb(true).alignment(Some(alignment)).offset(Some(offset)).endian(Some(endian)).numsync(numsync).open(
            old_db.path().to_str().unwrap()
        )
        .and_then(|mut db| {
            println!("magic: {:?}", db.header.magic);
            db.sync()})
        .map_err(|e| format!(
            "creating: alignment: {:?}, offset: {:?}, endian: {:?}, numsync: {}, expected: {:?}, newdb error: {}",
            alignment, offset, endian, numsync, expected_magic, e
        ))?;

        OpenOptions::new().alignment(Some(alignment)).open(
            old_db.path().to_str().unwrap(),
        )
        .map_err(|e| format!(
            "opening: alignment: {:?}, offset: {:?}, endian: {:?}, numsync: {}, expected: {:?}, open error: {}",
            alignment, offset, endian, numsync, expected_magic, e
        ))
        .and_then(|db| {
            (db.header.magic == expected_magic)
                .then_some(())
                .ok_or_else(|| {
                    format!(
                        "wrong magic: alignment: {:?}, offset: {:?}, endian: {:?}, numsync: {}, expected: {:?}, got: {:?}",
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
        match OpenOptions::new()
            .write()
            .create()
            .newdb(true)
            .block_size(BlockSize::Exactly(block_size))
            .open(old_db.path().to_str().unwrap())
        {
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
        OpenOptions::new()
            .cachesize(cachesize)
            .write()
            .create()
            .newdb(true)
            .open(db.path().to_str().unwrap())
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
        OpenOptions::new()
            .cachesize(cachesize)
            .open(db.path().to_str().unwrap())
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
