extern crate gdbm_native;

mod common;

use common::init_tests;
use gdbm_native::OpenOptions;

#[test]
fn api_convert() {
    init_tests()
        .into_iter()
        .filter(|test| test.is_basic)
        .try_for_each(|test| -> Result<(), String> {
            let tempfile = test.tempfile();
            let numsync = !test.db_path.ends_with("numsync");

            // open and convert to/from numsync
            OpenOptions::new()
                .alignment(test.alignment)
                .write()
                .open(tempfile.path().to_str().unwrap())
                .map_err(|e| format!("opening: {e}"))
                .and_then(|mut db| {
                    db.set_numsync(numsync)
                        .map_err(|e| format!("converting: {e}"))
                        .and_then(|()| db.sync().map_err(|e| format!("synching: {e}")))
                })
                .map_err(|e| format!("converting {} to numsync: {e}", test.db_path))?;

            // reopen and ensure we're (non)numsync
            OpenOptions::new()
                .alignment(test.alignment)
                .open(tempfile.path().to_str().unwrap())
                .map_err(|e| format!("opening: {e}"))
                .and_then(|db| {
                    (db.magic().is_numsync() == numsync)
                        .then_some(())
                        .ok_or_else(|| "file is not numsync".to_string())
                })
                .map_err(|e| format!("ensuring file {} is numsync: {e}", test.db_path))?;

            Ok(())
        })
        .unwrap_or_else(|e| panic!("{e}"));
}
