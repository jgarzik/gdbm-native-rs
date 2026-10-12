#!/bin/sh

TESTGEN=../testgen
SFX=.le64

for plan in basic empty hash avail
do
	$TESTGEN -o $plan.db$SFX -j $plan.json$SFX -p $plan
done

