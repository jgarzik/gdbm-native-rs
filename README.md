# gdbm-rs
Rust crate library for reading/writing GDBM key/value databases

## Goals

* Read and Write GDBM databases
* Written in safe Rust
* Better-than-C:  Support all 32-/64-bit and big/little endian variants

## Status

Alpha:  Read-only access works well.  Do not use in production.

Major categories of tasks leading to 1.0.0.  View the
[list of issues](https://github.com/jgarzik/gdbm-rs/issues) and
[milestones](https://github.com/jgarzik/gdbm-rs/milestones) for more.

- [x] Open/Close
- [x] Fetch: Get record data by key
- [x] Iterate: First-key/Next-key operations
- [x] Count: Iterate db and count all records
- [x] Exists: Quick key-exists test
- [ ] Insert: Store record
- [X] Remove: Delete record
- [ ] Reorganize: Not a priority
- [ ] Sync: Write dirty buffers, and fsync(2)
- [ ] Import/Export:  GDBM ASCII and binary dump formats
- [ ] Tests:  Healthy test coverage
- [ ] Code Cleanup:  Review by rust experts

