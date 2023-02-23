#!/bin/bash

OUTPUT_DIR=$1
VERIFY_DIR=$2

for f in `ls $OUTPUT_DIR`
do
	echo $f
	sort -k1n $OUTPUT_DIR/$f > tmp
	cmp $VERIFY_DIR/$f tmp
done
