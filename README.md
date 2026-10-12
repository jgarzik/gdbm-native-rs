
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
[list of issues](https://github.com/jgarzik/gdbm-native-rs/issues) and
[milestones](https://github.com/jgarzik/gdbm-native-rs/milestones) for more.

## Compatibility with C GDBM

Databases are interchangeable with those of the GNU GDBM C library (tested
against GDBM 1.23), including its free space management and file locking.

* Key hashing in C GDBM depends on whether the platform's `char` is signed.
  The default, `CharSign::Signed`, matches C GDBM on x86 and x86-64. Use
  `OpenOptions::char_sign(CharSign::Unsigned)` for databases written by C GDBM
  on ARM, PowerPC or RISC-V Linux, if they contain keys with bytes >= 0x80.
* Like C GDBM, readers take a shared lock and writers an exclusive lock on the
  database file. `OpenOptions::lock(false)` disables locking.

## Documentation

The best documentation is the original GDBM source code from the GNU
project: https://www.gnu.org.ua/software/gdbm/

A sister project also provides [additional technical
documentation](https://github.com/jgarzik/gdbm-docs), including file
format information.

