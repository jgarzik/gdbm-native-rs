extern crate gdbm_native;

use gdbm_native::ser::Alignment;
use gdbm_native::GdbmOptions;
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tempfile::NamedTempFile;

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
    #[allow(dead_code)]
    pub is_basic: bool,
    #[allow(dead_code)]
    pub n_records: usize,
    #[allow(dead_code)]
    pub metadata: TestMetadata,
    pub alignment: Option<Alignment>,
}

impl TestInfo {
    fn new(db_fn: &str, json_fn: &str, is_basic: bool, alignment: Option<Alignment>) -> Self {
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

        Self {
            db_path: dbp.to_str().unwrap().to_string(),
            json_path,
            is_basic,
            n_records: metadata.data_records,
            metadata,
            alignment,
        }
    }

    #[allow(dead_code)]
    pub fn ro_cfg(&self) -> GdbmOptions {
        GdbmOptions {
            readonly: true,
            creat: false,
            alignment: self.alignment,
            block_size: None,
            bsexact: false,
            endian: None,
            newdb: false,
            numsync: false,
            offset: None,
            cachesize: None,
        }
    }

    #[allow(dead_code)]
    pub fn rw_cfg(&self) -> GdbmOptions {
        GdbmOptions {
            readonly: false,
            creat: false,
            alignment: self.alignment,
            block_size: None,
            bsexact: false,
            endian: None,
            newdb: false,
            numsync: false,
            offset: None,
            cachesize: None,
        }
    }

    #[allow(dead_code)]
    pub fn tempfile(&self) -> NamedTempFile {
        let file = NamedTempFile::new().unwrap();
        let test_filename = file.path();
        fs::copy(&self.db_path, test_filename).unwrap();
        file
    }
}

#[allow(dead_code)]
pub fn init_tests() -> Vec<TestInfo> {
    [
        ("le64", None),
        ("be64", None),
        ("le32", Some(Alignment::Align32)),
        ("be32", None),
    ]
    .into_iter()
    .flat_map(|(flavor, alignment)| {
        ["empty", "basic"]
            .into_iter()
            .map(move |empty_or_basic| {
                TestInfo::new(
                    &format!("{}.db.{}", empty_or_basic, flavor),
                    &format!("{}.json.{}", empty_or_basic, flavor),
                    empty_or_basic == "basic",
                    alignment,
                )
            })
            .chain(["empty", "basic"].into_iter().map(move |empty_or_basic| {
                TestInfo::new(
                    &format!("{}.db.{}.numsync", empty_or_basic, flavor),
                    &format!("{}.json.{}", empty_or_basic, flavor),
                    empty_or_basic == "basic",
                    alignment,
                )
            }))
    })
    .collect()
}

#[allow(dead_code)]
pub fn default_cfg() -> GdbmOptions {
    GdbmOptions {
        readonly: true,
        creat: false,
        newdb: false,
        block_size: None,
        bsexact: false,
        numsync: true,
        alignment: None,
        endian: None,
        offset: None,
        cachesize: None,
    }
}

#[allow(dead_code)]
pub fn creat_cfg() -> GdbmOptions {
    GdbmOptions {
        readonly: false,
        creat: true,
        newdb: false,
        block_size: None,
        bsexact: false,
        numsync: true,
        alignment: None,
        endian: None,
        offset: None,
        cachesize: None,
    }
}
