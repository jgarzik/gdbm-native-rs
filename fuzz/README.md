# Fuzzing

Requires nightly Rust and `cargo install cargo-fuzz`. From the repository root, seed the corpora
with the C GDBM fixtures and run a target:

```sh
fuzz/seed-corpus.sh
cargo +nightly fuzz run --target x86_64-unknown-linux-gnu open fuzz/corpus/open -- -max_total_time=300
```

cargo-fuzz builds for the target it was itself compiled for. A prebuilt cargo-fuzz (e.g. from
`cargo binstall` or CI install actions) is built for musl, which the sanitizers do not support,
so pass `--target` for your host's gnu triple as above.
