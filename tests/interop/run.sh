#!/bin/bash
#
# Interoperability tests between gdbm-native and the GNU GDBM C library:
# non-ASCII key hashing, free space management (alternating C/Rust churn of
# one file, checked with gdbm_avail_verify) and file locking.
#
# Usage: tests/interop/run.sh
#
# Needs a C compiler and libgdbm with headers. Set GDBM_PREFIX if they are
# not in the default search paths (e.g. GDBM_PREFIX=$(brew --prefix gdbm)).
# Exits 0 without testing if libgdbm is unavailable, unless REQUIRE_GDBM=1.

set -eu

root=$(cd "$(dirname "$0")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cflags=()
ldflags=()
if [ -n "${GDBM_PREFIX:-}" ]; then
	cflags=(-I"$GDBM_PREFIX/include")
	ldflags=(-L"$GDBM_PREFIX/lib" -Wl,-rpath,"$GDBM_PREFIX/lib")
fi

C="$work/cgdbm"
if ! "${CC:-cc}" "${cflags[@]}" -O1 -Wall -o "$C" "$root/tests/interop/cgdbm.c" \
	"${ldflags[@]}" -lgdbm; then
	if [ "${REQUIRE_GDBM:-0}" = 1 ]; then
		echo "FAIL: cannot build against libgdbm"
		exit 1
	fi
	echo "SKIP: libgdbm not available"
	exit 0
fi

cargo build --quiet --release --example interop --manifest-path "$root/Cargo.toml"
R="$root/target/release/examples/interop"

# hash keys the same way as this platform's C GDBM
GDBM_CHAR_SIGN=$("$C" char-sign)
export GDBM_CHAR_SIGN
echo "C GDBM char is $GDBM_CHAR_SIGN"

cd "$work"
failures=0

check() {
	if [ "$2" = "$3" ]; then
		echo "ok   $1"
	else
		echo "FAIL $1: expected '$2', got '$3'"
		failures=$((failures + 1))
	fi
}

# run a command, printing "ok" on success or its output on failure
run() {
	local out
	if out=$("$@" 2>&1); then echo ok; else echo "$out"; fi
}

echo "== keys, including bytes >= 0x80"
for key in 61626364 c3a9 ff80aa e282ac2d6575726f; do
	rm -f c.db r.db
	"$C" store c.db "$key" cval
	check "C store, Rust fetch $key" cval "$("$R" fetch c.db "$key" || true)"
	"$R" store r.db "$key" rval
	check "Rust store, C fetch $key" rval "$("$C" fetch r.db "$key" || true)"
done

echo "== free space: alternating churn of one file"
for first in Rust C; do
	if [ $first = Rust ]; then a="$R" b="$C" an=Rust bn=C; else a="$C" b="$R" an=C bn=Rust; fi
	rm -f churn.db
	check "$an creates and churns" ok "$(run "$a" churn churn.db new 1 30)"
	for round in 1 2 3; do
		check "$bn churns ($round)" ok "$(run "$b" churn churn.db open $((round * 10 + 2)) 20)"
		check "$an churns ($round)" ok "$(run "$a" churn churn.db open $((round * 10 + 3)) 20)"
	done
done

echo "== locking"
rm -f lock.db
"$R" store lock.db 6b v
for holder in Rust C; do
	for hmode in r w; do
		for contender in Rust C; do
			for cmode in r w; do
				if [ $holder = Rust ]; then h="$R"; else h="$C"; fi
				if [ $contender = Rust ]; then c="$R"; else c="$C"; fi
				if [ $hmode = r ] && [ $cmode = r ]; then expected=ok; else expected=locked; fi

				"$h" hold lock.db $hmode 5 >hold.out &
				pid=$!
				for _ in $(seq 100); do
					grep -q . hold.out && break
					sleep 0.05
				done
				check "$holder holds $hmode lock ($(cat hold.out)), $contender opens $cmode" \
					$expected "$("$c" hold lock.db $cmode 0)"
				kill $pid 2>/dev/null || true
				wait $pid 2>/dev/null || true
			done
		done
	done
done

if [ $failures -ne 0 ]; then
	echo "$failures FAILED"
	exit 1
fi
echo "all passed"
