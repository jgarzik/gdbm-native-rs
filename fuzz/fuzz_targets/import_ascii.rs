// Any dump contents: importing must fail cleanly, never panic, hang or allocate unboundedly.
// Seed with tests/data/*/mixed.txt (see README.md).
#![no_main]

use gdbm_native::{ImportMode, OpenOptions};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut db = OpenOptions::new().tempfile().unwrap();
    let mut reader = data;
    let _ = db.import_ascii(&mut reader, ImportMode::Replace);
    let _ = db.verify();
});
