#!/bin/sh
#
# Regenerate the le64 legacy fixtures with a testgen built against the local
# libgdbm: g++ -O2 -o testgen testgen.cc -lgdbm

TESTGEN=$(dirname "$0")/testgen
SFX=.le64

cd "$(dirname "$0")/../data" || exit 1
for plan in basic empty hash avail
do
	$TESTGEN -o $plan.db$SFX -j $plan.json$SFX -p $plan
done
