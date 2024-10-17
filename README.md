
# gdbm-native

Rust crate library for reading/writing GDBM key/value databases

## Goals

* Read and Write GDBM databases
* Written in safe, native Rust (no FFI).
* Better-than-C:  Support all 32-/64-bit and big/little endian variants
  without recompiling.
* Tuned for modern machines with solid state storage

## Status

Beta:  Read/Write facilities recently completed and needs seasoning.  Not recommended (yet) for production use.

Major categories of tasks leading to 1.0.0.  View the
[list of issues](https://github.com/jgarzik/gdbm-rs/issues) and
[milestones](https://github.com/jgarzik/gdbm-rs/milestones) for more.

## Documentation

The best documentation is the original GDBM source code from the GNU
project: https://www.gnu.org.ua/software/gdbm/

A sister project also provides [additional technical
documentation](https://github.com/jgarzik/gdbm-docs), including file
format information.

