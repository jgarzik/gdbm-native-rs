# Contributing

Bug reports and pull requests are welcome at
<https://github.com/jgarzik/gdbm-native-rs>.

## Checks

CI runs these; please run them before sending a change:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

The minimum supported Rust version is 1.89.

## Compatibility with C GDBM

On-disk behaviour must match C GDBM. When changing it, work from the C GDBM sources (e.g.
`src/falloc.c`, `src/findkey.c`) rather than from memory, and add a test that fails without the
change.

- `tests/interop/run.sh` runs C GDBM and gdbm-native against the same files. It needs a C
  compiler and libgdbm with headers (e.g. `libgdbm-dev`); set `GDBM_PREFIX` if they are not in
  the default search paths.
- `tests/data/` holds databases and dumps written by C GDBM on several platforms, checked by
  `tests/platforms.rs`. `tests/testgen/gen-platforms.sh` regenerates them with Docker and QEMU.

## Fuzzing

See [fuzz/README.md](fuzz/README.md).
