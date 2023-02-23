#!/bin/bash

SYNC_DIR=$1

for h in `cat ./remaining`
do
	rsync -av --delete $1/ldbc_snb_impl $h:$1
done
