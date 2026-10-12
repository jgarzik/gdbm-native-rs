//
// basic.rs -- store, read, iterate and dump a database
//
// SPDX-License-Identifier: MIT
//
// Run with: cargo run --example basic

use gdbm_native::{BinDumpFormat, OpenOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("fruit.db");

    // Create the database and add some entries.
    let mut db = OpenOptions::new().create(true).open(&path)?;
    for (fruit, colour) in [("apple", "red"), ("banana", "yellow"), ("kiwi", "green")] {
        db.insert(fruit, colour)?;
    }
    // Values are stored as bytes: encode numbers explicitly.
    db.insert("count", &3u32.to_be_bytes())?;
    db.close()?;

    // Reopen read-only, look up and iterate.
    let db = OpenOptions::new().open(&path)?;
    let colour: Option<String> = db.get("banana")?;
    println!("banana is {}", colour.unwrap_or_default());
    let count = db.get::<_, [u8; 4]>("count")?.map(u32::from_be_bytes);
    println!("count is {count:?}");

    for key in db.keys::<Vec<u8>>() {
        println!("key: {}", String::from_utf8_lossy(&key?));
    }

    // Dumps in C GDBM's formats: portable ASCII, and binary for the local C GDBM.
    db.export_ascii(&mut std::io::stdout(), Some(&path))?;
    let mut binary = Vec::new();
    db.export_bin(&mut binary, BinDumpFormat::host())?;
    println!("binary dump: {} bytes", binary.len());

    Ok(())
}
