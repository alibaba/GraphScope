#!/bin/bash

INPUT_PATH=$1
OUTPUT_PATH=$2
INPUT_SCHEMA_PATH=$3
GRAPH_SCHEMA_PATH=$4
PARTITION_NUM=$5

for i in $(seq 1 $((PARTITION_NUM - 1))); do
  cmd="./target/release/load_csr_partition $INPUT_PATH $OUTPUT_PATH $INPUT_SCHEMA_PATH $GRAPH_SCHEMA_PATH -p $PARTITION_NUM -i $i"
  echo $cmd
  eval $cmd &> load$i.log &
done
