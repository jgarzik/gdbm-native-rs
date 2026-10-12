# gdbm-native

Pure-Rust GDBM; on-disk compatible with GNU GDBM 1.23 (C).

## Rules
- Pre-1.0, no external users: change API freely, no compat shims. Keep `~/repo/gdbmtool-rs` (sole consumer; git dep, `diagnostic` feature) building.
- On-disk behaviour must match C GDBM. Port from C source (`src/falloc.c`, `avail.c`, `findkey.c`, `gdbmopen.c`), not memory; verify with real libgdbm.
- File/dump contents are untrusted: checked/saturating math, validate before use, never allocate a claimed length.

## Commands
- Test: `cargo test --all-features`
- Lint (CI uses -D warnings): `cargo fmt --check && cargo clippy --all-targets --all-features -- -D warnings`
- C interop: `tests/interop/run.sh` (needs libgdbm + headers; `GDBM_PREFIX=...` if nonstandard; `REQUIRE_GDBM=1` fails instead of skipping).
- CI: linux/macos × x86_64/aarch64 + interop, lint, MSRV 1.89 (edition 2024; `File::try_lock`).

## Map
- `lib.rs`: `Gdbm<ReadOnly|ReadWrite>`, alloc/free (port of `falloc.c`), iteration, import/export.
- `options.rs`: type-state `OpenOptions`. `header/dir/bucket/avail.rs`: on-disk structs.
- `check.rs` (test only): `check_space()` asserts no overlapping extents and file size == `next_block`; churn and corruption tests.
- `src/data`: fixtures written by C via `src/testgen.cc` (`Dockerfile`, `./testgen`); plans basic/empty/hash/avail. hash/avail exist only as le64 (x86-64).

## Gotchas
- Key hash depends on C `char` signedness: `CharSign` (default Signed = x86/x86-64/macOS; Unsigned = Linux ARM/PPC/RISC-V).
- Avail stack block size is C's `(sz*elem)/2 + sizeof(avail_block)`, which includes one elem: `AvailBlock::stack_block_size`.
- Free space may lie past EOF up to `next_block`; validate against `next_block`. `sync` extends the file to `next_block`; buckets are padded to `bucket_sz` (C sets it to `block_size`).
- `Offset::Small` files are capped at 2 GiB (C's signed 32-bit `off_t`).
- Mutations set `WriteState::Inconsistent`; an error leaves the db refusing writes.
