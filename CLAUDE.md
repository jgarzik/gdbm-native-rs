# gdbm-native

Pure-Rust GDBM; on-disk compatible with GNU GDBM 1.23 (C).

## Rules
- Pre-1.0 (0.9.0), no external users: change API freely, no compat shims. Keep `~/repo/gdbmtool-rs` (sole consumer; git dep, `diagnostic` feature) building. History is git; no changelog file.
- Never push, never open PRs, never install anything or touch the Rust toolchain (`rustup`, `cross`): ask the user.
- On-disk behaviour must match C GDBM. Port from C source (`src/falloc.c`, `avail.c`, `findkey.c`, `gdbmopen.c`), not memory; verify with real libgdbm.
- File/dump contents are untrusted: checked/saturating math, validate before use, never allocate a claimed length.

## Commands
- Test: `cargo test --all-features`
- Lint (CI uses -D warnings): `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings`
- C interop: `tests/interop/run.sh` (needs libgdbm + headers; `GDBM_PREFIX=...` if nonstandard; `REQUIRE_GDBM=1` fails instead of skipping).
- CI: linux/macos × x86_64/aarch64 + interop; i686/armv7/s390x via `cross`; lint + rustdoc `-D warnings`; cargo-deny (`deny.toml`); fuzz smoke (`fuzz/`, nightly); MSRV 1.89 (edition 2024; `File::try_lock`). Local MSRV: `cargo +1.89 test` (installed).
- Docs: `#![warn(missing_docs)]`; every public `Result` fn has `# Errors`; crate guide in `lib.rs` header.

## Map
- `lib.rs`: one `Gdbm` (runtime RO/RW: `read_write: Option<WriteMode>`, writes `check_writable()` → `Error::ReadOnly`), alloc/free (port of `falloc.c`), iteration, import/export. Reads take `&self`: cache in `RefCell`, access buckets only via `with_bucket(dir, |b| ..)` (never hold a borrow across loads); write paths use `bucket_cache.get_mut()`.
- `options.rs`: `std::fs`-style `OpenOptions` (`&mut self` setters, `create` implies write). `dump.rs`: `BinDumpFormat`, `ImportMode`, `DumpInfo`. `bytes.rs`: public `ToBytesRef`/`FromBytes` (no int/bool encodings). `header/dir/bucket/avail.rs`: on-disk structs.
- `check.rs` (test only): `check_space()` asserts no overlapping extents and file size == `next_block`; churn and corruption tests.
- `tests/data/<platform>/`: C GDBM-written `mixed` db (+numsync), C binary/ASCII dumps, `manifest.json` (char sign, sizeof long, off_t, align, endian) per platform; expected records `tests/data/mixed.json` (hex). Regenerate: `tests/testgen/gen-platforms.sh [PLATFORM...]` (Docker+QEMU, Debian 13; `*-off32` build GDBM 1.26 with 32-bit off_t). Tested by `tests/platforms.rs`.
- `tests/data/{basic,empty,hash,avail}.*`: legacy fixtures (`tests/testgen/gen-legacy.sh`, `Dockerfile.legacy`). Generator: `tests/testgen/testgen.cc`.
- Work from repo root; scripts take paths, never `cd` first.

## Gotchas
- Key hash depends on C `char` signedness (`CharSign`: Signed = x86/MIPS/macOS, Unsigned = Linux ARM/PPC/RISC-V/s390x). Auto-detected lazily (`Gdbm::hash` → `detect_char_sign`) on first key with a byte >= 0x80; ASCII keys hash identically; fallback Signed.
- Avail stack block size is C's `(sz*elem)/2 + sizeof(avail_block)`, which includes one elem: `AvailBlock::stack_block_size`.
- Free space may lie past EOF up to `next_block`; validate against `next_block`. `sync` extends the file to `next_block`; buckets are padded to `bucket_sz` (C sets it to `block_size`).
- Layout (struct alignment) and byte order are detected on open (`Header::from_reader` → `detect_format`): magic gives endian + offset size; alignment from avail capacity + bucket_elems. Public `Layout {Off32, Off64Align4, Off64Align8}`; internal `ser::Format` (offset, alignment, endian).
- `Layout::Off32` files are capped at 2 GiB (C's signed 32-bit `off_t`).
- Binary dump length = C `long` holding BE32 length, host byte order: `BinDumpFormat {Long32, Long64Le, Long64Be}`; import detects from first length field. ASCII dumps: write an empty line after `#:len=0` (C gdbm_load 1.23–1.26 cannot load C gdbm_dump's line-less form); read both. C < 1.24 cannot binary export/import empty values.
- Mutations set `WriteState::Inconsistent`; an error leaves the db refusing writes.
