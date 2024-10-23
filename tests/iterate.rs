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
use gdbm_native::Gdbm;

#[test]
fn api_iter() {
    init_tests()
        .into_iter()
        .try_for_each(|test| {
            let mut keys_and_values = test
                .metadata
                .data
                .iter()
                .map(|kv| (kv[0].as_bytes().to_vec(), kv[1].as_bytes().to_vec()))
                .collect::<HashMap<_, _>>();

            Gdbm::open(&test.db_path, &test.ro_cfg())
                .map_err(|e| e.to_string())
                .and_then(|mut db| {
                    db.iter().try_for_each(|kv| {
                        kv.map_err(|e| e.to_string()).and_then(|(k, v)| {
                            (keys_and_values.remove(&k.to_vec()) == Some(v))
                                .then_some(())
                                .ok_or_else(|| format!("key {:?} not in metadata", k))
                        })
                    })
                })
                .and_then(|_| {
                    keys_and_values
                        .is_empty()
                        .then_some(())
                        .ok_or_else(|| "iteration missed some keys".to_string())
                })
                .map_err(|e| format!("[{}]: {}", test.db_path, e))
        })
        .unwrap_or_else(|e| panic!("{}", e));
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
                .map(|kv| (kv[0].as_bytes().to_vec()))
                .collect::<HashSet<_>>();

            Gdbm::open(&test.db_path, &test.ro_cfg())
                .map_err(|e| e.to_string())
                .and_then(|mut db| {
                    db.keys().try_for_each(|kv| {
                        kv.map_err(|e| e.to_string()).and_then(|k| {
                            keys.remove(&k.to_vec())
                                .then_some(())
                                .ok_or_else(|| format!("key {:?} not in metadata", k))
                        })
                    })
                })
                .and_then(|_| {
                    keys.is_empty()
                        .then_some(())
                        .ok_or_else(|| "iteration missed some keys".to_string())
                })
                .map_err(|e| format!("[{}]: {}", test.db_path, e))
        })
        .unwrap_or_else(|e| panic!("{}", e));
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
                .map(|kv| (kv[1].as_bytes().to_vec()))
                .collect::<HashSet<_>>();

            Gdbm::open(&test.db_path, &test.ro_cfg())
                .map_err(|e| e.to_string())
                .and_then(|mut db| {
                    db.values().try_for_each(|kv| {
                        kv.map_err(|e| e.to_string()).and_then(|k| {
                            values
                                .remove(&k.to_vec())
                                .then_some(())
                                .ok_or_else(|| format!("value {:?} not in metadata", k))
                        })
                    })
                })
                .and_then(|_| {
                    values
                        .is_empty()
                        .then_some(())
                        .ok_or_else(|| "iteration missed some values".to_string())
                })
                .map_err(|e| format!("[{}]: {}", test.db_path, e))
        })
        .unwrap_or_else(|e| panic!("{}", e));
}
