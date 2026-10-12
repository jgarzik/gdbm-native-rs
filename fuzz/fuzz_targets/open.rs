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
            drop(db);

            if let Ok(mut db) = OpenOptions::new().write(true).open(file.path()) {
                for i in 0..10u8 {
                    let _ = db.insert(&[i, 0xe9], &vec![i; usize::from(i) * 60]);
                }
                for key in keys.iter().take(5) {
                    let _ = db.remove(key.as_slice());
                }
                let _ = db.sync();
                if data.len() <= SMALL_LEN {
                    let _ = db.verify();
                    let _ = db.recover(false);
                    let _ = db.verify();
                }
            }
        }
    }
);
