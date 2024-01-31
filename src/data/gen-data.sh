#!/bin/sh

TESTGEN=../testgen
SFX=.le64

for plan in basic empty
do
	$TESTGEN -o $plan.db$SFX -j $plan.json$SFX -p $plan
done

