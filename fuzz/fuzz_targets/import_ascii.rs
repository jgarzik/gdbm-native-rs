// Any dump contents: importing must fail cleanly, never panic, hang or allocate unboundedly.
// Seed with fuzz/seed-corpus.sh (tests/data/*/mixed.txt; see README.md).
#![no_main]

use std::path::Path;

use gdbm_native::{ImportMode, OpenOptions};
use libfuzzer_sys::fuzz_target;

// Larger inputs are skipped (run with -max_len=262144); fits the C GDBM seeds (mixed.txt and
// mixed.bin < 196 KiB), which must not be truncated: truncation would break format detection.
const MAX_LEN: usize = 256 * 1024;

fuzz_target!(
    init: {
        // Temporary files on tmpfs: dropping the database fsyncs, which would dominate on disk.
        if std::env::var_os("TMPDIR").is_none() && Path::new("/dev/shm").is_dir() {
            let _ = tempfile::env::override_temp_dir(Path::new("/dev/shm"));
        }
    },
    |data: &[u8]| {
        if data.len() > MAX_LEN {
            return;
        }
        let mut db = OpenOptions::new().tempfile().unwrap();
        let mut reader = data;
        let _ = db.import_ascii(&mut reader, ImportMode::Replace);
        let _ = db.verify();
    }
);
