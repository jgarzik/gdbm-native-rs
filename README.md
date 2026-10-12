# gdbm-native

[![CI](https://github.com/jgarzik/gdbm-native-rs/actions/workflows/TestingCI.yml/badge.svg)](https://github.com/jgarzik/gdbm-native-rs/actions/workflows/TestingCI.yml)
[![crates.io](https://img.shields.io/crates/v/gdbm-native.svg)](https://crates.io/crates/gdbm-native)
[![docs.rs](https://img.shields.io/docsrs/gdbm-native)](https://docs.rs/gdbm-native)
[![MSRV 1.89](https://img.shields.io/badge/MSRV-1.89-blue)](Cargo.toml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A pure-Rust implementation of the [GNU GDBM](https://www.gnu.org.ua/software/gdbm/)
key-value database: safe Rust (`#![forbid(unsafe_code)]`), no C library or FFI, and
file-compatible with C GDBM databases and dumps from any platform.

```rust
use gdbm_native::OpenOptions;

let mut db = OpenOptions::new().create(true).open("users.db")?;
db.insert("alice", "alice@example.com")?;
let email: Option<String> = db.get("alice")?;
db.close()?;
```

See the [documentation](https://docs.rs/gdbm-native) for the full API.

## Compatibility with C GDBM

C GDBM writes files in the native format of the platform it was built for, and does not
record all of it in the file. gdbm-native handles every variant on every platform:

| Varies with | gdbm-native |
|---|---|
| Byte order | reads and writes both |
| 32- or 64-bit file offsets (`off_t`) | reads and writes both |
| Struct alignment (i386 vs. everything else) | detected |
| `char` signedness, which changes the key hash (x86 vs. ARM, s390x, ...) | detected |
| Numsync (extended) header | reads, writes and converts |
| Binary dump length fields (`sizeof(long)`, byte order) | detected on import, chosen on export |

C GDBM itself can only open files in its own platform's format. ASCII dumps are portable and
interchangeable with C GDBM's `gdbm_dump`/`gdbm_load`.

Also like C GDBM: file locking, numsync, sync mode, compaction (`gdbm_reorganize`), consistency
checking (`gdbm_avail_verify`) and recovery (`gdbm_recover`).

Compatibility is tested against databases and dumps written by C GDBM 1.24–1.26 on x86-64,
i386, arm64, armv7, s390x and 32-bit-offset builds (x86, ARM, big-endian MIPS), and by running
C GDBM and gdbm-native on the same files in CI. Crash-tolerance snapshots
(`gdbm_failure_atomic`) are not yet supported.

## Related

* [gdbmtool-rs](https://github.com/jgarzik/gdbmtool-rs): command line tools built on this crate.
* [gdbm-docs](https://github.com/jgarzik/gdbm-docs): GDBM file format documentation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md); report security issues as described in
[SECURITY.md](SECURITY.md).

## License

MIT; see [LICENSE](LICENSE).
