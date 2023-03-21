# Mutable CSR Store
## Build binary data
```bash
INPUT_PATH=$1
OUTPUT_PATH=$2
INPUT_SCHEMA_PATH=$3
GRAPH_SCHEMA_PATH=$4
PARTITION_NUM=$5
PARTITION_ID=$6

cmd="./target/release/build_csr_partition $INPUT_PATH $OUTPUT_PATH $INPUT_SCHEMA_PATH $GRAPH_SCHEMA_PATH -p $PARTITION_NUM -i $PARTITION_ID"
echo $cmd
eval $cmd
```

## Split raw data
You can split raw data before build binary data.
```bash
#!/bin/bash

INPUT_PATH=$1
OUTPUT_PATH=$2
INPUT_SCHEMA_PATH=$3
GRAPH_SCHEMA_PATH=$4
PARTITION_NUM=$5
PARTITION_ID=$6

cmd="./target/release/partition_raw_data $INPUT_PATH $OUTPUT_PATH $INPUT_SCHEMA_PATH $GRAPH_SCHEMA_PATH -p $PARTITION_NUM -i $PARTITION_ID"
echo $cmd
eval $cmd
```