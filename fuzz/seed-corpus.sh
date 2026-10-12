#!/bin/bash
# Seed fuzz/corpus/{open,import_ascii,import_bin} with the C GDBM fixtures from tests/data.
# Per-platform files share names (mixed.db, mixed.txt, ...), so they are prefixed with the
# platform directory name (e.g. arm64-mixed.db). Safe to re-run; runnable from anywhere.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
data="$root/tests/data"
corpus="$root/fuzz/corpus"

mkdir -p "$corpus/open" "$corpus/import_ascii" "$corpus/import_bin"

cp -f "$data"/*.db.* "$corpus/open/"

for dir in "$data"/*/; do
	plat=$(basename "$dir")
	for f in "$dir"mixed.db*; do
		if [ -f "$f" ]; then cp -f "$f" "$corpus/open/$plat-$(basename "$f")"; fi
	done
	if [ -f "${dir}mixed.txt" ]; then cp -f "${dir}mixed.txt" "$corpus/import_ascii/$plat-mixed.txt"; fi
	if [ -f "${dir}mixed.bin" ]; then cp -f "${dir}mixed.bin" "$corpus/import_bin/$plat-mixed.bin"; fi
done
