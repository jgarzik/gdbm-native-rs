#!/bin/bash
#
# Generate tests/data/<platform>/ fixtures by building testgen.cc against the
# C GDBM library of each platform and running it there (Docker + QEMU binfmt):
#
#   manifest.json         C facts that shape the formats (char sign, sizeof
#                         long, off_t size and alignment, endianness)
#   mixed.db{,.numsync}   databases from the "mixed" plan
#   mixed.bin, mixed.txt  C binary (gdbm_export) and ASCII (gdbm_dump) dumps
#
# and tests/data/mixed.json, the expected records (the same everywhere).
#
# Usage: gen-platforms.sh [PLATFORM...]   (default: all)

set -eu

here=$(cd "$(dirname "$0")" && pwd)
data=$(cd "$here/../data" && pwd)

GDBM_SRC=https://ftp.gnu.org/gnu/gdbm/gdbm-1.26.tar.gz
GDBM_SHA256=6a24504a14de4a744103dcb936be976df6fbe88ccff26065e54c1c47946f4a5e

# name, docker platform, Debian image, how:
#   pkg         Debian's libgdbm-dev
#   off32       GDBM built from source with 32-bit off_t (Debian 13 armhf
#               forces 64-bit off_t along with 64-bit time_t, so use 12)
#   mips-off32  GDBM cross-built for big-endian 32-bit MIPS with 32-bit off_t,
#               run under QEMU
platforms="
amd64        linux/amd64   debian:13  pkg
i386         linux/386     debian:13  pkg
armv7        linux/arm/v7  debian:13  pkg
arm64        linux/arm64   debian:13  pkg
s390x        linux/s390x   debian:13  pkg
i386-off32   linux/386     debian:13  off32
armv7-off32  linux/arm/v7  debian:12  off32
mips-off32   linux/amd64   debian:12  mips-off32
"

generate='
	cd /tmp
	./testgen -m >/out/manifest.json
	./testgen -p mixed -o /out/mixed.db -j /tmp/mixed.json -d /out/mixed
	./testgen -n -p mixed -o /out/mixed.db.numsync -j /dev/null
	cp /tmp/mixed.json /out/mixed.json.tmp
	chown -R "$OWNER" /out
'

build_pkg='
	apt-get update -qq && apt-get install -qq -y --no-install-recommends g++ libgdbm-dev >/dev/null
	# Debian builds libgdbm with large file support; match its off_t
	g++ -Wall -O2 $(getconf LFS_CFLAGS) -o /tmp/testgen /src/testgen.cc -lgdbm
'

# $CXX_PKG, $HOST_FLAG and $CXX select a native or cross build
build_src='
	apt-get update -qq && apt-get install -qq -y --no-install-recommends g++ $CXX_PKG make wget ca-certificates >/dev/null
	cd /tmp && wget -q "$GDBM_SRC" -O gdbm.tar.gz
	echo "$GDBM_SHA256  gdbm.tar.gz" | sha256sum -c --quiet
	tar xzf gdbm.tar.gz && cd gdbm-*/
	./configure -q $HOST_FLAG --disable-largefile --disable-shared --without-readline --prefix=/tmp/gdbm >/dev/null
	make -s -j"$(nproc)" >/dev/null && make -s install >/dev/null
	$CXX -Wall -O2 -static -I/tmp/gdbm/include -o /tmp/testgen /src/testgen.cc /tmp/gdbm/lib/libgdbm.a
'

echo "$platforms" | while read -r name platform image how; do
	[ -n "$name" ] || continue
	if [ $# -gt 0 ] && ! printf '%s\n' "$@" | grep -qx "$name"; then continue; fi
	echo "== $name ($platform, $how)"
	mkdir -p "$data/$name"
	cxx_pkg= host_flag= cxx=g++
	case $how in
	pkg) build=$build_pkg ;;
	off32) build=$build_src ;;
	mips-off32)
		build=$build_src cxx_pkg=g++-mips-linux-gnu host_flag=--host=mips-linux-gnu
		cxx=mips-linux-gnu-g++
		;;
	esac
	docker run --rm --platform "$platform" \
		-e DEBIAN_FRONTEND=noninteractive -e OWNER="$(id -u):$(id -g)" \
		-e GDBM_SRC="$GDBM_SRC" -e GDBM_SHA256="$GDBM_SHA256" \
		-e CXX_PKG="$cxx_pkg" -e HOST_FLAG="$host_flag" -e CXX="$cxx" \
		-v "$here:/src:ro" -v "$data/$name:/out" \
		"$image" sh -euc "$build $generate"
	cat "$data/$name/manifest.json"
	# the records are the same on every platform
	if [ -f "$data/mixed.json" ] && ! cmp -s "$data/$name/mixed.json.tmp" "$data/mixed.json"; then
		echo "$name: mixed.json differs from other platforms" >&2
		exit 1
	fi
	mv "$data/$name/mixed.json.tmp" "$data/mixed.json"
done
