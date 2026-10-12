#!/bin/sh
#
# Regenerate the legacy fixtures (tests/data/{basic,empty,hash,avail}.*) by
# running testgen built for x86-64, i386, mips and mips64 (see Dockerfile.legacy).

cd "$(dirname "$0")" || exit 1
docker run -u"$(id -u):$(id -g)" -v"$PWD/../data:/outdir" --rm \
	"$(docker build -q -f Dockerfile.legacy --target=testdata .)"
