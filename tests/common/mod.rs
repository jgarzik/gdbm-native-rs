// shared by several test crates, each of which uses only part of it
#![allow(dead_code)]

extern crate gdbm_native;

use gdbm_native::Layout;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tempfile::NamedTempFile;

#[derive(Deserialize)]
pub struct TestMetadata {
    pub generated_by: String,
    pub generated_time: String,
    pub data_records: usize,
    pub data: Vec<Vec<String>>,
}

pub struct TestInfo {
    pub json_path: String,
    pub db_path: String,
    pub is_basic: bool,
    pub n_records: usize,
    pub metadata: TestMetadata,
    /// layout the fixture was written with, which opening must detect
    pub layout: Layout,
}

impl TestInfo {
    fn new(db_fn: &str, json_fn: &str, is_basic: bool, layout: Layout) -> Self {
        let mut dbp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        dbp.push("tests/data");
        dbp.push(db_fn);

        let mut jsp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        jsp.push("tests/data");
        jsp.push(json_fn);

        let json_path = jsp.to_str().unwrap().to_string();
        let json_data = fs::read_to_string(&json_path).expect("Unable to read JSON file");
        let metadata: TestMetadata =
            serde_json::from_str(&json_data).expect("Test JSON was not well formed");

        Self {
            db_path: dbp.to_str().unwrap().to_string(),
            json_path,
            is_basic,
            n_records: metadata.data_records,
            metadata,
            layout,
        }
    }

    pub fn tempfile(&self) -> NamedTempFile {
        let file = NamedTempFile::new().unwrap();
        let test_filename = file.path();
        fs::copy(&self.db_path, test_filename).unwrap();
        file
    }
}

pub fn init_tests() -> Vec<TestInfo> {
    [
        ("le64", Layout::Off64Align8),
        ("be64", Layout::Off64Align8),
        // i386 with large file support
        ("le32", Layout::Off64Align4),
        // 32-bit MIPS with large file support
        ("be32", Layout::Off64Align8),
    ]
    .into_iter()
    .flat_map(|(flavor, layout)| {
        ["empty", "basic"]
            .into_iter()
            .map(move |empty_or_basic| {
                TestInfo::new(
                    &format!("{empty_or_basic}.db.{flavor}"),
                    &format!("{empty_or_basic}.json.{flavor}"),
                    empty_or_basic == "basic",
                    layout,
                )
            })
            .chain(["empty", "basic"].into_iter().map(move |empty_or_basic| {
                TestInfo::new(
                    &format!("{empty_or_basic}.db.{flavor}.numsync"),
                    &format!("{empty_or_basic}.json.{flavor}"),
                    empty_or_basic == "basic",
                    layout,
                )
            }))
    })
    .collect()
}
