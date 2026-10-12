// Any file contents: opening, reading, verifying and writing must fail cleanly, never panic,
// hang or allocate unboundedly. Seed with fuzz/seed-corpus.sh (see README.md).
#![no_main]

use std::io::Write;
use std::path::Path;

use gdbm_native::OpenOptions;
use libfuzzer_sys::fuzz_target;

// Larger inputs are skipped (run with -max_len=262144); fits the C GDBM seeds (mixed.db < 248 KiB).
const MAX_LEN: usize = 256 * 1024;
// Verifying after writes and recover() (which rebuilds the whole database) are slow on large
// databases, so they run only on inputs up to this size.
const SMALL_LEN: usize = 16 * 1024;
// Reads allocate no more than the file holds, so before any write memory is bounded by the input.
// But C GDBM (and so this crate) allows records, buckets and blocks of up to i32::MAX bytes, and
// writes at a corrupt header's huge next_block grow the file sparsely, making such sizes real:
// reading them back is valid, bounded, and over the fuzzer's memory limit. So skip writes when
// the bucket or block size alone exceeds this (a write may read or pad one after growing the
// file), and stop once the file has grown beyond it.
const MAX_GROWN_LEN: u64 = 64 * 1024 * 1024;

fn grown_too_large(path: &Path) -> bool {
    std::fs::metadata(path).map_or(true, |m| m.len() > MAX_GROWN_LEN)
}

fuzz_target!(
    init: {
        // Temporary files on tmpfs: sync() fsyncs, which would dominate small inputs on disk.
        if std::env::var_os("TMPDIR").is_none() && Path::new("/dev/shm").is_dir() {
            let _ = tempfile::env::override_temp_dir(Path::new("/dev/shm"));
        }
    },
    |data: &[u8]| {
        if data.len() > MAX_LEN {
            return;
        }
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(data).unwrap();

        if let Ok(db) = OpenOptions::new().open(file.path()) {
            let _ = db.verify();
            let _ = db.len();
            let keys = db
                .keys::<Vec<u8>>()
                .take(50)
                .filter_map(Result::ok)
                .collect::<Vec<_>>();
            for key in &keys {
                let _ = db.get::<_, Vec<u8>>(key.as_slice());
            }
            let _ = db.get::<_, Vec<u8>>("key \u{e9}");
            let sizes_too_large = u64::from(db.block_size().max(db.bucket_size())) > MAX_GROWN_LEN;
            drop(db);
            if sizes_too_large {
                return;
            }

            if let Ok(mut db) = OpenOptions::new().write(true).open(file.path()) {
                for i in 0..10u8 {
                    let _ = db.insert(&[i, 0xe9], &vec![i; usize::from(i) * 60]);
                    if grown_too_large(file.path()) {
                        return;
                    }
                }
                for key in keys.iter().take(5) {
                    let _ = db.remove(key.as_slice());
                    if grown_too_large(file.path()) {
                        return;
                    }
                }
                let _ = db.sync();
                if grown_too_large(file.path()) {
                    return;
                }
                if data.len() <= SMALL_LEN {
                    let _ = db.verify();
                    let _ = db.recover(false);
                    let _ = db.verify();
                }
            }
        }
    }
);
