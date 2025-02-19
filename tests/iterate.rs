//
// tests/iterate.rs -- testing GDBM export APIs
//
// Copyright (c) 2019-2024 Jeff Garzik
//
// This file is part of the gdbm-native software project covered under
// the MIT License.  For the full license text, please see the LICENSE
// file in the root directory of this project.
// SPDX-License-Identifier: MIT

extern crate gdbm_native;

mod common;

use std::collections::{HashMap, HashSet};

use common::init_tests;
use gdbm_native::OpenOptions;

#[test]
fn api_iter() {
    init_tests()
        .into_iter()
        .try_for_each(|test| {
            let mut keys_and_values = test
                .metadata
                .data
                .iter()
                .map(|kv| (kv[0].clone(), kv[1].clone()))
                .collect::<HashMap<_, _>>();

            OpenOptions::new()
                .alignment(test.alignment)
                .open(&test.db_path)
                .map_err(|e| e.to_string())
                .and_then(|mut db| {
                    db.iter::<String, String>().try_for_each(|kv| {
                        kv.map_err(|e| e.to_string()).and_then(|(k, v)| {
                            (keys_and_values.remove(&k) == Some(v))
                                .then_some(())
                                .ok_or_else(|| format!("key {k:?} not in metadata"))
                        })
                    })
                })
                .and_then(|()| {
                    keys_and_values
                        .is_empty()
                        .then_some(())
                        .ok_or_else(|| "iteration missed some keys".to_string())
                })
                .map_err(|e| format!("[{}]: {e}", test.db_path))
        })
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn api_keys() {
    init_tests()
        .into_iter()
        .try_for_each(|test| {
            let mut keys = test
                .metadata
                .data
                .iter()
                .map(|kv| kv[0].clone())
                .collect::<HashSet<_>>();

            OpenOptions::new()
                .alignment(test.alignment)
                .open(&test.db_path)
                .map_err(|e| e.to_string())
                .and_then(|mut db| {
                    db.keys::<String>().try_for_each(|kv| {
                        kv.map_err(|e| e.to_string()).and_then(|k| {
                            keys.remove(&k)
                                .then_some(())
                                .ok_or_else(|| format!("key {k:?} not in metadata"))
                        })
                    })
                })
                .and_then(|()| {
                    keys.is_empty()
                        .then_some(())
                        .ok_or_else(|| "iteration missed some keys".to_string())
                })
                .map_err(|e| format!("[{}]: {e}", test.db_path))
        })
        .unwrap_or_else(|e| panic!("{e}"));
}

#[test]
fn api_values() {
    init_tests()
        .into_iter()
        .try_for_each(|test| {
            let mut values = test
                .metadata
                .data
                .iter()
                .map(|kv| kv[1].clone())
                .collect::<HashSet<_>>();

            OpenOptions::new()
                .alignment(test.alignment)
                .open(&test.db_path)
                .map_err(|e| e.to_string())
                .and_then(|mut db| {
                    db.values::<String>().try_for_each(|kv| {
                        kv.map_err(|e| e.to_string()).and_then(|k| {
                            values
                                .remove(&k)
                                .then_some(())
                                .ok_or_else(|| format!("value {k:?} not in metadata"))
                        })
                    })
                })
                .and_then(|()| {
                    values
                        .is_empty()
                        .then_some(())
                        .ok_or_else(|| "iteration missed some values".to_string())
                })
                .map_err(|e| format!("[{}]: {e}", test.db_path))
        })
        .unwrap_or_else(|e| panic!("{e}"));
}
