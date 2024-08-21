# Mutable CSR Store
## Prepare schema for data loading
When loading the graph into storage, two schema files must be provided: the input schema and the graph schema. The input schema specifies the path to the input file, while the graph schema defines the structure of the graph.
### Input schema
The input schema contains the following information for the graph loading:
- Mappings from vertex label to file path of raw data
- Mappings from vertex label to column info of raw data
- Mappings from a tuple that includes the labels of the source vertex, edge, and target vertex to file path of raw data
- Mappings from a tuple that includes the labels of the source vertex, edge, and target vertex column info of raw data

The schema file is formatted using Json. We have provided a sampled schema file for modern graph in `data/modern_input.json`.

### Graph schema
The graph schema contains the following information for the graph storage:
- Mapping from vertex label to label id.
- Mapping from edge label to a 3-tuple, which contains edge label id, source vertex label id, and target vertex label id.
- The properties (name and datatype) of each type of vertex/edge.
  The schema file is formatted using Json. We have provided a sampled schema file for modern graph in `data/modern_schema.json`.

## Build Binary Data
```bash
INPUT_PATH=$1
OUTPUT_PATH=$2
INPUT_SCHEMA_PATH=$3
GRAPH_SCHEMA_PATH=$4
PARTITION_NUM=$5
PARTITION_ID=$6

#USAGE:
#    build_csr_partition <raw_data_dir> <output_dir> <input_schema_file> <graph_schema_file> -i <index> -p <partition> [--skip_header]
cmd="./target/release/build_csr_partition $INPUT_PATH $OUTPUT_PATH $INPUT_SCHEMA_PATH $GRAPH_SCHEMA_PATH -p $PARTITION_NUM -i $PARTITION_ID"
echo $cmd
eval $cmd
```