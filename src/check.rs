//
// check.rs -- file space consistency checks (test only)
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

use std::collections::{BTreeSet, HashMap};
use std::io::{Seek, SeekFrom};

use crate::avail::AvailBlock;
use crate::bucket::Bucket;
use crate::{Gdbm, OpenOptions, ReadWrite, Result};

impl Gdbm<ReadWrite> {
    // Sync, then verify from on-disk state that every in-use extent (header, directory, buckets,
    // records, avail stack blocks) and every free extent lies below next_block, and that no two
    // extents overlap.
    pub(crate) fn check_space(&mut self) -> Result<()> {
        self.sync()?;

        // like C GDBM, the file is extended to next_block on sync
        assert_eq!(
            self.f.metadata()?.len(),
            self.header.next_block,
            "file size"
        );

        let header = &self.header;
        let mut extents: Vec<(u64, u64, String)> = vec![
            (0, u64::from(header.block_sz), "header".into()),
            (header.dir_ofs, u64::from(header.dir_sz), "directory".into()),
        ];
        header
            .avail
            .elems
            .iter()
            .for_each(|e| extents.push((e.addr, e.sz.into(), "header avail".into())));

        let bucket_offsets = self.dir.dir.iter().copied().collect::<BTreeSet<_>>();
        for offset in bucket_offsets {
            self.f.seek(SeekFrom::Start(offset))?;
            let bucket = Bucket::from_reader(header.bucket_elems, header.layout, &mut self.f)?;
            extents.push((offset, header.bucket_sz.into(), format!("bucket@{offset}")));
            bucket.tab.iter().filter(|e| e.is_occupied()).for_each(|e| {
                extents.push((
                    e.data_ofs,
                    u64::from(e.key_size) + u64::from(e.data_size),
                    format!("record in bucket@{offset}"),
                ));
            });
            bucket.avail.iter().for_each(|e| {
                extents.push((e.addr, e.sz.into(), format!("avail in bucket@{offset}")));
            });
        }

        let stack_block_size = AvailBlock::stack_block_size(header.layout, header.avail.sz);
        let mut next = header.avail.next_block;
        while next != 0 {
            self.f.seek(SeekFrom::Start(next))?;
            let block = AvailBlock::from_reader(header.layout, &mut self.f)?;
            extents.push((next, stack_block_size.into(), format!("avail block@{next}")));
            block.elems.iter().for_each(|e| {
                extents.push((e.addr, e.sz.into(), format!("avail in block@{next}")));
            });
            next = block.next_block;
        }

        extents.sort();
        for pair in extents.windows(2) {
            let ((a_ofs, a_len, a_name), (b_ofs, _, b_name)) = (&pair[0], &pair[1]);
            assert!(
                a_ofs + a_len <= *b_ofs,
                "{a_name} ({a_ofs}+{a_len}) overlaps {b_name} ({b_ofs})"
            );
        }
        let (ofs, len, name) = extents.last().unwrap();
        assert!(
            ofs + len <= header.next_block,
            "{name} ({ofs}+{len}) beyond next_block {}",
            header.next_block
        );

        Ok(())
    }
}

// deterministic pseudo-random sequence for repeatable tests
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

fn value(n: u64, len: u64) -> Vec<u8> {
    format!("{n}:").bytes().cycle().take(len as usize).collect()
}

fn create(path: &std::path::Path) -> Gdbm<ReadWrite> {
    OpenOptions::new()
        .write()
        .create()
        .block_size(crate::BlockSize::Exactly(512))
        .open(path)
        .unwrap()
}

fn assert_contents(db: &mut Gdbm<ReadWrite>, model: &HashMap<String, Vec<u8>>) {
    assert_eq!(db.len().unwrap(), model.len());
    for (k, v) in model {
        assert_eq!(
            db.get::<_, Vec<u8>>(k).unwrap().as_ref(),
            Some(v),
            "key {k}"
        );
    }
}

// Random inserts/removes (remove_pct% of operations) of mixed sizes. With a small block size,
// remove-heavy phases overflow the header avail table onto the avail stack and insert-heavy
// phases pop it back. Space must never be double-used.
fn churn(
    db: &mut Gdbm<ReadWrite>,
    rng: &mut Lcg,
    model: &mut HashMap<String, Vec<u8>>,
    ops: usize,
    remove_pct: u64,
) {
    for op in 0..ops {
        let n = rng.next(1000);
        let key = n.to_string();
        if rng.next(100) < remove_pct {
            assert_eq!(db.remove(&key).unwrap(), model.remove(&key), "remove {key}");
        } else {
            let len = match rng.next(4) {
                0 => 1 + rng.next(30),
                1 => 1 + rng.next(400),
                2 => 400 + rng.next(800),
                _ => 1000 + rng.next(3000),
            };
            let v = value(n, len);
            db.insert(&key, &v).unwrap();
            model.insert(key, v);
        }

        if op % 100 == 0 {
            db.check_space().unwrap();
        }
    }
    db.check_space().unwrap();
}

#[test]
fn churn_space_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut rng = Lcg(1);
    let mut model = HashMap::new();
    let mut db = create(&path);

    let mut stack_used = false;
    for phase in 0..30 {
        churn(&mut db, &mut rng, &mut model, 500, [10, 80][phase % 2]);
        stack_used |= db.header.avail.next_block != 0;
        assert_contents(&mut db, &model);
    }
    assert!(stack_used, "test never exercised the avail stack");

    drop(db);
    let mut db = OpenOptions::new().write().open(&path).unwrap();
    assert_contents(&mut db, &model);
    churn(&mut db, &mut rng, &mut model, 2000, 30);
    assert_contents(&mut db, &model);
}

// A database whose free space extends past the physical end of file must reopen.
#[test]
fn reopen_after_churn() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut model = HashMap::new();
    {
        let mut db = create(&path);
        for n in 0..200 {
            let v = value(n, 700);
            db.insert(&n.to_string(), &v).unwrap();
            model.insert(n.to_string(), v);
        }
        for n in (0..200).step_by(2) {
            db.remove(&n.to_string()).unwrap();
        }
        for n in (0..200).step_by(2) {
            let v = value(n, 100 + n * 7);
            db.insert(&n.to_string(), &v).unwrap();
            model.insert(n.to_string(), v);
        }
        db.check_space().unwrap();
    }

    let mut db = OpenOptions::new().write().open(&path).unwrap();
    assert_contents(&mut db, &model);
}

// free_record() must not assume a bucket is cached; set_numsync() may free space first thing.
#[test]
fn set_numsync_on_fresh_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    {
        let mut db = OpenOptions::new()
            .write()
            .create()
            .numsync(false)
            .block_size(crate::BlockSize::Exactly(512))
            .open(&path)
            .unwrap();
        db.insert("k", "v").unwrap();
    }

    let mut db = OpenOptions::new().write().open(&path).unwrap();
    // fill the header avail table with blocks freshly allocated from the end of file
    while db.header.avail.elems.len() < db.header.avail.sz as usize {
        let (offset, length) = db.extend(1024).unwrap();
        db.header.free(offset, length);
    }
    db.read_write.state = crate::WriteState::Dirty;

    db.set_numsync(true).unwrap();
    db.check_space().unwrap();
    assert_eq!(db.get::<_, String>("k").unwrap().as_deref(), Some("v"));
}

// avail.db.le64 was written by C GDBM and has a deep avail stack. Popping C-written stack
// blocks must free exactly the space C allocated for them.
#[test]
fn pop_c_avail_stack() {
    let data = |name: &str| -> std::path::PathBuf {
        [env!("CARGO_MANIFEST_DIR"), "tests/data", name]
            .iter()
            .collect()
    };
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(data("avail.json.le64")).unwrap()).unwrap();
    let mut model = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|kv| {
            (
                kv[0].as_str().unwrap().to_string(),
                kv[1].as_str().unwrap().into(),
            )
        })
        .collect::<HashMap<String, Vec<u8>>>();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    std::fs::copy(data("avail.db.le64"), &path).unwrap();

    let mut db = OpenOptions::new().write().open(&path).unwrap();
    assert_ne!(db.header.avail.next_block, 0, "fixture has no avail stack");
    db.check_space().unwrap();
    assert_contents(&mut db, &model);

    let mut rng = Lcg(2);
    churn(&mut db, &mut rng, &mut model, 3000, 5);
    assert_contents(&mut db, &model);
}

// set_numsync(true) on a numsync database must not reset the sync counter.
#[test]
fn set_numsync_keeps_counter() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = OpenOptions::new()
        .write()
        .create()
        .numsync(true)
        .open(&path)
        .unwrap();
    for n in 0..5 {
        db.insert(&n.to_string(), "v").unwrap();
        db.sync().unwrap();
    }
    let numsync = db.header.numsync;
    assert!(numsync.unwrap() > 0);

    db.set_numsync(true).unwrap();
    db.sync().unwrap();
    assert_eq!(db.header.numsync, numsync);
}

// compact() rebuilds the database; it must keep the block size and numsync counter.
#[test]
fn compact_keeps_block_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = OpenOptions::new()
        .write()
        .create()
        .block_size(crate::BlockSize::Exactly(1024))
        .open(&path)
        .unwrap();
    for n in 0..500 {
        db.insert(&n.to_string(), &value(n, 300)).unwrap();
    }
    db.sync().unwrap();
    let numsync = db.header.numsync;

    db.compact().unwrap();
    assert_eq!(db.header.block_sz, 1024);
    assert!(db.header.numsync >= numsync);
    db.check_space().unwrap();
    for n in 0..500 {
        assert_eq!(
            db.get::<_, Vec<u8>>(&n.to_string()).unwrap(),
            Some(value(n, 300))
        );
    }
}

// Create a one-record database, then overwrite it with `bytes` at `offset`.
fn corrupt(
    offset: impl Fn(&mut Gdbm<ReadWrite>) -> u64,
    bytes: &[u8],
) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::io::Write;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let offset = {
        let mut db = create(&path);
        db.insert("key", "value").unwrap();
        db.sync().unwrap();
        offset(&mut db)
    };

    let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(bytes).unwrap();

    (dir, path)
}

// Corrupt a database, then reopen it and read the record. Must be an error, never a panic or
// huge allocation.
fn corrupt_and_get(offset: impl Fn(&mut Gdbm<ReadWrite>) -> u64, bytes: &[u8]) -> Result<()> {
    let (_dir, path) = corrupt(offset, bytes);
    let mut db = OpenOptions::new().open(&path)?;
    db.get::<_, Vec<u8>>("key")?;

    db.iter::<Vec<u8>, Vec<u8>>()
        .try_for_each(|kv| kv.map(|_| ()))
}

// Corrupt a database, then reopen it and insert records. Must be an error, never a panic or
// overwriting of unrelated parts of the file.
fn corrupt_and_insert(offset: impl Fn(&mut Gdbm<ReadWrite>) -> u64, bytes: &[u8]) -> Result<()> {
    let (_dir, path) = corrupt(offset, bytes);
    let mut db = OpenOptions::new().write().open(&path)?;
    for n in 0..10 {
        db.insert(&n.to_string(), "v")?;
    }
    db.sync()?;
    drop(db);
    OpenOptions::new().open(&path).map(|_| ())
}

// file offset of field `field_ofs` within the occupied element of the first bucket
fn element_field(db: &mut Gdbm<ReadWrite>, field_ofs: u64) -> u64 {
    let bucket_ofs = db.dir.dir[0];
    db.f.seek(SeekFrom::Start(bucket_ofs)).unwrap();
    let bucket = Bucket::from_reader(db.header.bucket_elems, db.header.layout, &mut db.f).unwrap();
    let index = bucket
        .tab
        .iter()
        .position(super::bucket::BucketElement::is_occupied)
        .unwrap() as u64;
    bucket_ofs
        + u64::from(Bucket::sizeof(db.header.layout))
        + index * u64::from(crate::bucket::BucketElement::sizeof(db.header.layout))
        + field_ofs
}

#[test]
fn corrupt_element_data_size() {
    // LFS element: hash 4, key_start 4, data_ofs 8, key_size 4, data_size 4
    assert!(corrupt_and_get(|db| element_field(db, 20), &[0xff; 4]).is_err());
}

#[test]
fn corrupt_element_data_ofs() {
    assert!(corrupt_and_get(|db| element_field(db, 8), &[0xff; 8]).is_err());
}

#[test]
fn corrupt_header_dir_ofs() {
    // LFS header: magic 4, block_size 4, dir_ofs 8
    assert!(corrupt_and_get(|_| 8, &[0xff; 8]).is_err());
}

#[test]
fn corrupt_directory_entry() {
    assert!(corrupt_and_get(|db| db.header.dir_ofs, &[0xff; 8]).is_err());
}

#[test]
fn corrupt_bucket_avail_count() {
    // first field of a bucket is the avail count, of at most Bucket::AVAIL
    assert!(corrupt_and_get(|db| db.dir.dir[0], &1000u32.to_le_bytes()).is_err());
}

#[test]
fn corrupt_bucket_avail_elem() {
    // Align64/LFS bucket: av_count 4, pad 4, then avail elements of sz 4, pad 4, addr 8.
    // One free extent covering the file header.
    let mut bytes = 1u32.to_le_bytes().to_vec();
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(&512u32.to_le_bytes());
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(&0u64.to_le_bytes());
    assert!(corrupt_and_insert(|db| db.dir.dir[0], &bytes).is_err());
}

#[test]
fn corrupt_header_avail_size() {
    let avail_ofs = |db: &mut Gdbm<ReadWrite>| {
        u64::from(
            crate::header::Header::sizeof(db.header.layout, db.header.magic.is_numsync(), 0)
                - AvailBlock::sizeof(db.header.layout, 0),
        )
    };
    assert!(corrupt_and_get(avail_ofs, &[0xff; 4]).is_err());
}

// Databases with 32-bit offsets cannot grow past what C GDBM's 32-bit (signed) off_t can address;
// offsets beyond that were silently truncated when written.
#[test]
fn small_offset_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let mut db = OpenOptions::new()
        .write()
        .create()
        .offset(Some(crate::Offset::Small))
        .block_size(crate::BlockSize::Exactly(512))
        .open(&path)
        .unwrap();
    db.insert("small", "fits").unwrap();

    // pretend the file has grown to just under the limit
    db.header.next_block = u64::from(i32::MAX as u32) - 1024;
    assert!(matches!(
        db.insert("big", &vec![0u8; 4096]),
        Err(crate::Error::FileTooLarge { .. })
    ));
}
