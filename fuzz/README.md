# Fuzzing

Requires nightly Rust and `cargo install cargo-fuzz`. From the repository root, seed the corpora
with the C GDBM fixtures and run a target:

```sh
mkdir -p fuzz/corpus/{open,import_ascii,import_bin}
cp tests/data/*.db.* tests/data/*/mixed.db* fuzz/corpus/open/
cp tests/data/*/mixed.txt fuzz/corpus/import_ascii/
cp tests/data/*/mixed.bin fuzz/corpus/import_bin/
cargo +nightly fuzz run open fuzz/corpus/open -- -max_total_time=300
```
