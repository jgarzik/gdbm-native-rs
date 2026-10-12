// Any file contents: opening, reading, verifying and writing must fail cleanly, never panic,
// hang or allocate unboundedly. Seed with tests/data databases (see README.md).
#![no_main]

use std::io::Write;

use gdbm_native::OpenOptions;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(data).unwrap();

    if let Ok(db) = OpenOptions::new().open(file.path()) {
        let _ = db.verify();
        let _ = db.len();
        let keys = db
            .keys::<Vec<u8>>()
            .take(100)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        for key in &keys {
            let _ = db.get::<_, Vec<u8>>(key.as_slice());
        }
        let _ = db.get::<_, Vec<u8>>("key \u{e9}");
        drop(db);

        if let Ok(mut db) = OpenOptions::new().write(true).open(file.path()) {
            for i in 0..20u8 {
                let _ = db.insert(&[i, 0xe9], &vec![i; usize::from(i) * 40]);
            }
            for key in keys.iter().take(10) {
                let _ = db.remove(key.as_slice());
            }
            let _ = db.sync();
            let _ = db.verify();
            let _ = db.recover(false);
        }
    }
});
