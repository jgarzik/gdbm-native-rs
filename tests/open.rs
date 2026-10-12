extern crate gdbm_native;

use gdbm_native::{
    BlockSize,
    Endian::{Big, Little},
    Layout, Magic, OpenOptions,
};
use tempfile::tempdir;

#[test]
// Create fails if the file exists, but isn't a db.
fn api_open_create() {
    let dir = tempdir().unwrap();

    let no_db = dir.path().join("no");
    let bad_db = dir.path().join("bad");
    std::fs::write(&bad_db, "stuff").expect("creating a DB file");

    assert!(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(no_db)
            .is_ok()
    );
    assert!(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(bad_db)
            .is_err()
    );
}

#[test]
// Like C GDBM's GDBM_WRCREAT, create initializes an existing empty file as a new database.
fn api_open_create_empty_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("empty");
    std::fs::write(&path, "").unwrap();

    let mut db = OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .unwrap();
    db.insert("key", "value").unwrap();
    drop(db);

    let db = OpenOptions::new().open(&path).unwrap();
    assert_eq!(
        db.get::<_, String>("key").unwrap().as_deref(),
        Some("value")
    );
}

#[test]
// Create on an existing non-database reports why it isn't a database, and leaves it untouched.
fn api_open_create_bad_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("bad");
    std::fs::write(&path, [0x55u8; 4096]).unwrap();

    let Err(err) = OpenOptions::new().write(true).create(true).open(&path) else {
        panic!("opened a non-database");
    };
    assert!(
        matches!(err, gdbm_native::Error::BadMagic { magic: 0x5555_5555 }),
        "unexpected error {err:?}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), [0x55u8; 4096]);
}

#[test]
fn tempfile() {
    assert!(
        OpenOptions::new()
            .write(true)
            .create(true)
            .tempfile()
            .is_ok()
    );
}

#[test]
// New databases get the magic for their layout, and reopen with that layout detected.
fn api_open_newdb_magic() {
    for layout in [Layout::Off32, Layout::Off64Align4, Layout::Off64Align8] {
        for endian in [Little, Big] {
            for numsync in [false, true] {
                let expected = match (layout, endian, numsync) {
                    (Layout::Off32, Little, false) => Magic::LE32,
                    (Layout::Off32, Big, false) => Magic::BE32,
                    (Layout::Off32, Little, true) => Magic::LE32NS,
                    (Layout::Off32, Big, true) => Magic::BE32NS,
                    (_, Little, false) => Magic::LE64,
                    (_, Big, false) => Magic::BE64,
                    (_, Little, true) => Magic::LE64NS,
                    (_, Big, true) => Magic::BE64NS,
                };
                let what = format!("{layout:?} {endian:?} numsync {numsync}");
                let dir = tempdir().unwrap();
                let path = dir.path().join("db");

                let mut db = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .layout(layout)
                    .endian(endian)
                    .numsync(numsync)
                    .open(&path)
                    .unwrap_or_else(|e| panic!("creating {what}: {e}"));
                db.sync().unwrap();
                drop(db);

                let db = OpenOptions::new()
                    .open(&path)
                    .unwrap_or_else(|e| panic!("opening {what}: {e}"));
                assert_eq!(db.magic(), expected, "{what}");
                assert_eq!(db.layout(), layout, "{what}");
                assert_eq!(db.endian(), endian, "{what}");
            }
        }
    }
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
            .write(true)
            .create(true)
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

        let mut options = OpenOptions::new();
        if let Some(cachesize) = cachesize {
            options.cachesize(cachesize);
        }

        // Create a database using configured cachesize.
        options
            .clone()
            .create(true)
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
        options
            .open(&db)
            .map_err(|e| format!("read open failed: {e}"))
            .and_then(|db| {
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

    [Some(0), Some(100_000)]
        .into_iter()
        .try_for_each(|cachesize| {
            the_test(cachesize).map_err(|e| format!("cachesize: {cachesize:?}]: {e}"))
        })
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
// A database opened read-only refuses writes; create implies write.
fn read_only_mode() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    let mut db = OpenOptions::new().create(true).open(&path).unwrap();
    assert!(db.is_writable());
    db.insert("key", "value").unwrap();
    drop(db);

    let mut db = OpenOptions::new().open(&path).unwrap();
    assert!(!db.is_writable());
    assert!(matches!(
        db.insert("key", "x"),
        Err(gdbm_native::Error::ReadOnly)
    ));
    assert!(matches!(
        db.try_insert("new", "x"),
        Err(gdbm_native::Error::ReadOnly)
    ));
    assert!(matches!(
        db.remove("key"),
        Err(gdbm_native::Error::ReadOnly)
    ));
    assert!(matches!(db.compact(), Err(gdbm_native::Error::ReadOnly)));
    assert!(matches!(
        db.set_numsync(false),
        Err(gdbm_native::Error::ReadOnly)
    ));
    // nothing to sync
    assert!(db.sync().is_ok());
    assert_eq!(
        db.get::<_, String>("key").unwrap().as_deref(),
        Some("value")
    );
}
