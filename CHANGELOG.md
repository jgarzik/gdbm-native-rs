# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/).

## [0.9.0] - Unreleased

A review against GNU GDBM 1.23–1.26 and its file formats on every platform. Many API changes
are breaking; see "Changed".

### Fixed

- Keys containing bytes >= 0x80 could not be found in, or were duplicated in, databases written
  by C GDBM on x86 (key hashing used unsigned `char`).
- Free space management could hand out the same space twice, read the file header as a free
  space block, free twice the real size of a C-written avail block (overwriting live data),
  and write files that could not be reopened or that C GDBM read as truncated. It now follows
  C GDBM's `falloc.c`.
- Databases written on i386 required an alignment hint, and unsigned-`char` (ARM, s390x)
  databases a `CharSign` hint, to be read correctly.
- Binary dumps did not interoperate with 64-bit C GDBM; ASCII dumps of numsync databases lost
  numsync; C ASCII dumps with empty values could not be imported.
- Corrupt files could cause panics, integer overflows and unbounded allocations; inserts could
  overwrite the file header given a corrupt bucket free list.
- `create` on an existing empty file failed; `set_numsync(true)` reset the numsync counter;
  `compact()` changed the block size; the numsync counter lagged C GDBM's by one.
- Records over 2 GiB were silently truncated; 32-bit-offset databases were corrupted past
  4 GiB (now limited to 2 GiB, like C GDBM).
- ASCII export of large values was quadratic.
- The crate only built on Linux and macOS.

### Added

- File locking compatible with C GDBM (`OpenOptions::lock`, `lock_wait`).
- Detection of struct layout (`Layout`) and key hash `char` signedness (`CharSign`).
- `BinDumpFormat`, `ImportMode` and `DumpInfo` for C-compatible dumps.
- `Gdbm::verify`, `recover`, `close`, `is_empty`, `is_writable`, `layout`, `endian`,
  `char_sign`, `block_size`, `bucket_size`, `dir_depth`, `bucket_count`, `is_numsync`,
  `numsync`, `path`; `Debug` for `Gdbm`.
- `OpenOptions::mode` for new database file permissions.
- `[u8; N]` keys and values.
- CI on Linux and macOS (x86-64 and arm64), i686, armv7 and s390x, with C GDBM
  interoperability tests, fuzzing, cargo-deny and MSRV checks.

### Changed

- **Breaking:** `Gdbm<ReadOnly>`/`Gdbm<ReadWrite>` are replaced by a single `Gdbm`; writes to
  a database opened read-only fail with `Error::ReadOnly`.
- **Breaking:** `OpenOptions` follows `std::fs::OpenOptions`: `write(bool)`, `create(bool)`
  (which implies write) and other `&mut self` setters taking values; `alignment()` and
  `offset()` are replaced by `layout()`.
- **Breaking:** reads (`get`, `contains_key`, `len`, iteration, export) take `&self`. `Gdbm` is
  `Send` but not `Sync`.
- **Breaking:** `ToBytesRef` and `FromBytes` are public and use `Cow<[u8]>`; the integer and
  `bool` encodings are removed.
- **Breaking:** `ExportBinMode` is replaced by `BinDumpFormat`; `import_ascii` and
  `import_bin` take an `ImportMode` and return what was imported.
- **Breaking:** `Error` implements `std::error::Error` with readable messages, and is
  `#[non_exhaustive]`; several variants were added or renamed.
- **Breaking:** creating over an existing non-database file is an error instead of replacing it.
- Rust 2024 edition; minimum Rust version 1.89.
- `COMPAT_GDBM_VERSION` is 1.26.

## [0.5.2] and earlier

See the git history.

[0.9.0]: https://github.com/jgarzik/gdbm-native-rs/compare/v0.5.2...HEAD
[0.5.2]: https://github.com/jgarzik/gdbm-native-rs/releases/tag/v0.5.2
