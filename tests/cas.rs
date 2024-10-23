use tempfile::NamedTempFile;

extern crate gdbm_native;
use gdbm_native::{CompareAndSwapSummary, Gdbm};

mod common;
use common::default_cfg;

#[test]
fn api_compare_and_swap() {
    #[derive(Debug)]
    struct Test {
        current: Option<Vec<u8>>,
        old: Option<&'static [u8]>,
        new: Option<Vec<u8>>,
        expected: CompareAndSwapSummary,
    }

    fn the_test(
        db: &mut Gdbm,
        Test {
            current,
            old,
            new,
            expected,
        }: Test,
    ) -> Result<(), String> {
        let key = b"key".to_vec();
        db.remove(&key).map_err(|e| format!("remove: {}", e))?;

        if let Some(current) = current {
            db.insert(key.clone(), current.clone())
                .map_err(|e| format!("insert: {}", e))?;
        }

        let result = db
            .compare_and_swap(key, old, new)
            .map_err(|e| format!("compare_and_swap: {}", e))?;

        (result == expected)
            .then_some(())
            .ok_or_else(|| format!("expected: {:?}, got: {:?}", expected, result))
    }

    let db = NamedTempFile::new().unwrap();
    let mut cfg = default_cfg();
    cfg.readonly = false;
    cfg.creat = true;
    Gdbm::open(db.path().to_str().unwrap(), &cfg)
        .map_err(|e| format!("open: {}", e))
        .and_then(|mut db| {
            const A: &[u8] = b"one";
            const B: &[u8] = b"two";
            [
                // Delete if old didn't exist.
                (None, None, None, (None, None)),
                // Insert if old didn't exist.
                (None, None, Some(A), (None, Some(A))),
                // Update if old is "one" (but old is none).
                (None, Some(A), Some(A), (None, None)),
                // Update if old is "one" (but old is none).
                (None, Some(A), None, (None, None)),
                // Delete if old is none (but old is some)
                (Some(A), None, None, (Some(A), Some(A))),
                // Replace if old is some.
                (Some(A), Some(A), Some(B), (Some(A), Some(B))),
                // Delete if old is matching Some.
                (Some(A), Some(A), None, (Some(A), None)),
                // Delete is old is someting else (fail)
                (Some(A), Some(B), None, (Some(A), Some(A))),
            ]
            .into_iter()
            .enumerate()
            .try_for_each(
                |(test, (current, old, new, (expected_was, expected_is)))| {
                    println!("testing: current {:?}, old: {:?}", current, old);
                    the_test(
                        &mut db,
                        Test {
                            current: current.map(|bs| bs.to_vec()),
                            old,
                            new: new.map(|bs| bs.to_vec()),
                            expected: CompareAndSwapSummary {
                                was: expected_was.map(|bs| bs.to_vec()),
                                is: expected_is.map(|bs| bs.to_vec()),
                            },
                        },
                    )
                    .map_err(|e| format!("test: #{}: {}", test, e))
                },
            )
        })
        .inspect_err(|e| println!("{}", e))
        .unwrap();
}
