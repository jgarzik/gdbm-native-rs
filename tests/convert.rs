extern crate gdbm_native;

mod common;

use common::init_tests;
use gdbm_native::{ConvertOptions, Gdbm};

#[test]
fn api_convert() {
    init_tests()
        .into_iter()
        .filter(|test| test.is_basic)
        .try_for_each(|test| -> Result<(), String> {
            let tempfile = test.tempfile();
            let convert_options = ConvertOptions {
                numsync: !test.db_path.ends_with("numsync"),
            };

            // open and convert to/from numsync
            Gdbm::open(tempfile.path().to_str().unwrap(), &test.rw_cfg())
                .map_err(|e| format!("opening: {}", e))
                .and_then(|mut db| {
                    db.convert(&convert_options)
                        .map_err(|e| format!("converting: {}", e))
                        .and_then(|_| db.sync().map_err(|e| format!("synching: {}", e)))
                })
                .map_err(|e| format!("converting {} to numsync: {}", test.db_path, e))?;

            // reopen and ensure we're (non)numsync
            Gdbm::open(tempfile.path().to_str().unwrap(), &test.ro_cfg())
                .map_err(|e| format!("opening: {}", e))
                .and_then(|db| {
                    (db.header.magic.is_numsync() == convert_options.numsync)
                        .then_some(())
                        .ok_or_else(|| "file is not numsync".to_string())
                })
                .map_err(|e| format!("ensuring file {} is numsync: {}", test.db_path, e))?;

            Ok(())
        })
        .unwrap_or_else(|e| panic!("{}", e))
}
