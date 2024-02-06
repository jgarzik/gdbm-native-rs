extern crate rs_gdbm;

use rs_gdbm::GdbmOptions;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct TestMetadata {
    pub generated_by: String,
    pub generated_time: String,
    pub data_records: usize,
    pub data: Vec<Vec<String>>,
}

pub struct TestInfo {
    #[allow(dead_code)]
    pub json_path: String,
    pub db_path: String,
    pub is_basic: bool,
    pub n_records: usize,
    #[allow(dead_code)]
    pub metadata: TestMetadata,
}

pub struct TestConfig {
    pub def_ro_cfg: GdbmOptions,
    pub def_rw_cfg: GdbmOptions,
    pub tests: Vec<TestInfo>,
}

impl TestConfig {
    fn new() -> TestConfig {
        TestConfig {
            def_ro_cfg: GdbmOptions {
                readonly: true,
                creat: false,
            },
            def_rw_cfg: GdbmOptions {
                readonly: false,
                creat: false,
            },
            tests: Vec::new(),
        }
    }
}

fn push_test(cfg: &mut TestConfig, db_fn: &str, json_fn: &str, is_basic: bool) {
    let mut dbp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dbp.push("src/data");
    dbp.push(db_fn);

    let mut jsp = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    jsp.push("src/data");
    jsp.push(json_fn);

    let json_path = jsp.to_str().unwrap().to_string();
    let json_data = fs::read_to_string(&json_path).expect("Unable to read JSON file");
    let metadata: TestMetadata =
        serde_json::from_str(&json_data).expect("Test JSON was not well formed");

    cfg.tests.push(TestInfo {
        db_path: dbp.to_str().unwrap().to_string(),
        json_path,
        is_basic,
        n_records: metadata.data_records,
        metadata,
    });
}

pub fn init_tests() -> TestConfig {
    let mut cfg = TestConfig::new();

    // NOTE: Order of push is important.
    // Some tests depend on basic.db being index 1 (2nd item)

    push_test(&mut cfg, "empty.db.le64", "empty.json.le64", false);
    push_test(&mut cfg, "basic.db.le64", "basic.json.le64", true);

    cfg
}
