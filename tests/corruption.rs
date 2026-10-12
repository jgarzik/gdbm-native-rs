//
// tests/corruption.rs -- corrupted databases must fail cleanly
//
// Copyright (c) 2026 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

// A deterministic, always-run counterpart of fuzz/fuzz_targets/open.rs: C GDBM databases from
// several platforms with random bytes overwritten must give errors, never panics.

use gdbm_native::OpenOptions;

// deterministic pseudo-random sequence for repeatable tests
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % n
    }
}

// The operations of the fuzz target; errors are fine.
fn exercise(path: &std::path::Path) {
    if let Ok(db) = OpenOptions::new().open(path) {
        let _ = db.verify();
        let _ = db.len();
        let _ = db.is_empty();
        let keys = db
            .keys::<Vec<u8>>()
            .take(50)
            .filter_map(Result::ok)
            .collect::<Vec<_>>();
        for key in &keys {
            let _ = db.get::<_, Vec<u8>>(key.as_slice());
        }
        let _ = db.get::<_, Vec<u8>>("cl\u{e9} 7 \u{20ac}");
        drop(db);

        if let Ok(mut db) = OpenOptions::new().write(true).open(path) {
            for i in 0..10u8 {
                let _ = db.insert(&[i, 0xe9], &vec![i; usize::from(i) * 60]);
            }
            for key in keys.iter().take(5) {
                let _ = db.remove(key.as_slice());
            }
            let _ = db.sync();
            let _ = db.recover(false);
            let _ = db.verify();
        }
    }
}

#[test]
fn corrupted_databases() {
    let data: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "data"]
        .iter()
        .collect();
    let mut rng = Lcg(7);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");

    for platform in ["amd64", "i386-off32", "mips-off32", "s390x"] {
        let original = std::fs::read(data.join(platform).join("mixed.db")).unwrap();
        for round in 0..150 {
            let mut bytes = original.clone();
            // most rounds hit the metadata-dense start of the file
            let span = if round % 3 == 0 { bytes.len() } else { 8192 };
            for _ in 0..1 + rng.next(8) {
                let at = rng.next(span.min(bytes.len()));
                bytes[at] = rng.next(256) as u8;
            }
            std::fs::write(&path, &bytes).unwrap();
            exercise(&path);
        }
    }
}
