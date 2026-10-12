//
// interop.rs -- gdbm-native side of the C GDBM interoperability tests
//
// See tests/interop/run.sh. The C side is tests/interop/cgdbm.c; both implement the same
// commands. Set GDBM_CHAR_SIGN=unsigned to match a C GDBM built with unsigned char.
// Rust-only command: fill DB N [empty] (N records, numsync format, one value empty if asked).
//
// SPDX-License-Identifier: MIT

use gdbm_native::{BinDumpFormat, BlockSize, CharSign, Error, Gdbm, ImportMode, OpenOptions};
use std::process::exit;

// same LCG as cgdbm.c
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

fn unhex(h: &str) -> Vec<u8> {
    (0..h.len() / 2)
        .map(|i| u8::from_str_radix(&h[2 * i..2 * i + 2], 16).unwrap())
        .collect()
}

fn fail(what: &str, e: Error) -> ! {
    println!("R {what}: {e}");
    exit(1)
}

fn char_sign() -> CharSign {
    match std::env::var("GDBM_CHAR_SIGN").as_deref() {
        Ok("unsigned") => CharSign::Unsigned,
        _ => CharSign::Signed,
    }
}

// every record must be readable, and agree with len()
fn verify(db: &mut Gdbm) {
    let n = db
        .iter::<Vec<u8>, Vec<u8>>()
        .try_fold(0, |n, kv| kv.map(|_| n + 1))
        .unwrap_or_else(|e| fail("iterate", e));
    let len = db.len().unwrap_or_else(|e| fail("len", e));
    if n != len {
        println!("R iterated {n} of {len} records");
        exit(1);
    }
}

fn churn(path: &str, create: bool, seed: u64, phases: usize) {
    let mut db = if create {
        OpenOptions::new()
            .char_sign(char_sign())
            .write(true)
            .create(true)
            .block_size(BlockSize::Exactly(512))
            .open(path)
    } else {
        OpenOptions::new()
            .char_sign(char_sign())
            .write(true)
            .open(path)
    }
    .unwrap_or_else(|e| fail("open", e));
    verify(&mut db);

    let mut rng = Lcg(seed);
    for phase in 0..phases {
        let remove_pct = [10, 80][phase % 2];
        for _ in 0..500 {
            let n = rng.next(1000);
            let key = n.to_string();
            if rng.next(100) < remove_pct {
                db.remove(&key).unwrap_or_else(|e| fail("remove", e));
                continue;
            }

            let len = match rng.next(4) {
                0 => 1 + rng.next(30),
                1 => 1 + rng.next(400),
                2 => 400 + rng.next(800),
                _ => 1000 + rng.next(3000),
            };
            let value = format!("{n}:")
                .bytes()
                .cycle()
                .take(len as usize)
                .collect::<Vec<_>>();
            db.insert(&key, &value)
                .unwrap_or_else(|e| fail("insert", e));
        }
    }

    verify(&mut db);
    db.sync().unwrap_or_else(|e| fail("sync", e));
}

fn hold(path: &str, writer: bool, secs: u64) {
    let result = if writer {
        OpenOptions::new()
            .char_sign(char_sign())
            .write(true)
            .open(path)
            .map(|_db| {
                println!("ok");
                std::thread::sleep(std::time::Duration::from_secs(secs));
            })
    } else {
        OpenOptions::new()
            .char_sign(char_sign())
            .open(path)
            .map(|_db| {
                println!("ok");
                std::thread::sleep(std::time::Duration::from_secs(secs));
            })
    };
    match result {
        Ok(()) => (),
        Err(Error::Locked) => println!("locked"),
        Err(e) => fail("open", e),
    }
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();

    match args[..] {
        ["store", path, key, value] => {
            let mut db = OpenOptions::new()
                .char_sign(char_sign())
                .write(true)
                .create(true)
                .open(path)
                .unwrap_or_else(|e| fail("open", e));
            db.insert(&unhex(key), value)
                .unwrap_or_else(|e| fail("insert", e));
            db.sync().unwrap_or_else(|e| fail("sync", e));
        }
        ["fetch", path, key] => {
            let db = OpenOptions::new()
                .char_sign(char_sign())
                .open(path)
                .unwrap_or_else(|e| fail("open", e));
            match db.get::<_, Vec<u8>>(&unhex(key)) {
                Ok(Some(v)) => println!("{}", String::from_utf8_lossy(&v)),
                Ok(None) => {
                    println!("NOTFOUND");
                    exit(1)
                }
                Err(e) => fail("get", e),
            }
        }
        ["churn", path, mode, seed, phases] => churn(
            path,
            mode == "new",
            seed.parse().unwrap(),
            phases.parse().unwrap(),
        ),
        ["hold", path, mode, secs] => hold(path, mode == "w", secs.parse().unwrap()),
        ["fill", path, n] | ["fill", path, n, "empty"] => {
            let empty = args.len() == 4;
            let mut db = OpenOptions::new()
                .char_sign(char_sign())
                .write(true)
                .create(true)
                .numsync(true)
                .open(path)
                .unwrap_or_else(|e| fail("open", e));
            for i in 0..n.parse::<usize>().unwrap() {
                let value = if empty && i == 7 {
                    String::new()
                } else {
                    format!("value {i} é")
                };
                db.insert(format!("key {i} €").as_str(), value.as_str())
                    .unwrap_or_else(|e| fail("insert", e));
            }
            db.sync().unwrap_or_else(|e| fail("sync", e));
        }
        ["export" | "dump", path, out] => {
            let db = OpenOptions::new()
                .char_sign(char_sign())
                .open(path)
                .unwrap_or_else(|e| fail("open", e));
            let mut f = std::fs::File::create(out).unwrap();
            if args[0] == "export" {
                db.export_bin(&mut f, BinDumpFormat::host())
            } else {
                db.export_ascii(&mut f, Some(path))
            }
            .unwrap_or_else(|e| fail(args[0], e));
        }
        ["import" | "load", path, input] => {
            let mut db = OpenOptions::new()
                .char_sign(char_sign())
                .write(true)
                .create(true)
                .open(path)
                .unwrap_or_else(|e| fail("open", e));
            let mut f = std::fs::File::open(input).unwrap();
            let (count, numsync) = if args[0] == "import" {
                db.import_bin(&mut f, None, ImportMode::Replace)
                    .map(|count| (count, None))
            } else {
                db.import_ascii(&mut f, ImportMode::Replace)
                    .map(|info| (info.count, info.numsync))
            }
            .unwrap_or_else(|e| fail(args[0], e));
            let format = match numsync {
                Some(true) => "numsync",
                _ => "standard",
            };
            println!("{count} {format}");
        }
        _ => {
            eprintln!("usage: see {}", file!());
            exit(2)
        }
    }
}
