# gdbm-rs
Rust crate library for reading/writing GDBM key/value databases

## Goals

* Read and Write GDBM databases
* Written in safe Rust
* Better-than-C:  Support all 32-/64-bit and big/little endian variants

## Status

Major categories of tasks leading to 1.0.0:

- [x] Fetch: Get record data by key
- [x] Iterate: First-key/Next-key operations
- [ ] Insert: Store record
- [ ] Remove: Delete record
- [ ] Import/Export:  GDBM ASCII and binary dump formats
- [ ] Tests:  Healthy test coverage

