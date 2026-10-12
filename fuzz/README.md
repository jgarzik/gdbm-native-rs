# Fuzzing

Requires nightly Rust and `cargo install cargo-fuzz`. From the repository root, seed the corpora
with the C GDBM fixtures and run a target:

```sh
fuzz/seed-corpus.sh
cargo +nightly fuzz run open fuzz/corpus/open -- -max_total_time=300
```
