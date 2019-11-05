#!/bin/sh

TESTGEN=../testgen

mkdir -p data
cd data
for plan in basic empty
do
	$TESTGEN -o $plan.db -j $plan.json -p $plan
done

