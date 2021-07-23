# Data Load Tools
This is a toolset for bulk loading data from raw files to graphscope persistent storage service. 
Currently the tool supports a specific format of the raw data as described in "Data Format", 
and the originial data must be located in an HDFS. To load the data files into GraphScope storage, 
users can run the data-loading tool from a terminal on a Client machine, and we assume that Client 
has access to a Hadoop cluster, which can run MapReduce jobs, have read/write access to the HDFS, 
and connect to a running GraphScope storage service.

## Quick Start
### Prerequisites

- Java compilation environment (Maven 3.5+ / JDK1.8), if you need to build the tools from source code
- Hadoop cluster (version 2.x) that can run map-reduce jobs and has HDFS supported
- Running GIE with persistent storage service (graph schema should be properly defined)

### Get Binary

If you have the distribution package `maxgraph.tar.gz`, decompress it. Then you can find the map-reduce job jar `data_load_tools-0.0.1-SNAPSHOT.jar` under `maxgraph/lib/` and the executable `load_tool.sh` under `maxgraph/bin/`.

If you want to build from source code, just run `mvn clean package -DskipTests`. You can find the compiled jar `data_load_tools-0.0.1-SNAPSHOT.jar` in the `target/` directory. The `load_tool.sh` is just a wrapper for java command, you can only use `data_load_tools-0.0.1-SNAPSHOT.jar` in the following demonstration.

### Data Format

The data loading tools assume the original data files are in the HDFS.

Each file should represents either a vertex type or a relationship of an edge type. Below are the sample 
data of a vertex type person and a relationShip person-knows->person of edge type knows:

- person.csv
```
id|name
1000|Alice
1001|Bob
...
```

- person_knows_person.csv
```
person_id|person_id_1|date
1000|1001|20210611151923
...
```

The first line of the data file is a header that describes the key of each field. The header is not required. 
If there is no header in the data file, you need to set `skip.header` to `true` in the data building process 
(For details, see params description in "Building a partitioned graph").

The rest lines are the data records. Each line represents one record. Data fields are seperated by a custom seperator 
("|" in the example above). In the vertex data file `person.csv`, `id` field and `name` field are the primary-key and 
the property of the vertex type `person` respectively. In the edge data file `person_knows_person.csv`, `person_id` 
field is the primary-key of the source vertex, `person_id_1` field is the primary-key of the destination vertex, `date` 
is the property of the edge type `knows`.

All the data fields will be parsed according to the data-type defined in the graph schema. If the input data field 
cannot be parsed correctly, data building process would be failed with corresponding errors.


### Loading Process
The loading process contains three steps:

- Step 1: A partitioned graph is built from the source files and stored in the same HDFS using a MapReduce job
- Step 2: The graph partitions are loaded into the store servers (in parallel)
- Step 3: Commit to the online service so that data is ready for serving queries

#### 1. Building a partitioned graph

  Build data by running the hadoop map-reduce job with following command:
  
  ```
  $ hadoop jar data_load_tools-0.0.1-SNAPSHOT.jar com.alibaba.maxgraph.dataload.databuild.OfflineBuild <path/to/config/file>
  ```

  The config file should follow a format that is recognized by Java `java.util.Properties` class. Here is an example:
  
  ```
  split.size=256
separator=\\|
input.path=/tmp/ldbc_sample
output.path=/tmp/data_output
graph.endpoint=1.2.3.4:55555
column.mapping.config={"person_0_0.csv":{"label":"person","propertiesColMap":{"0":"id","1":"name"}},"person_knows_person_0_0.csv":{"label":"knows","srcLabel":"person","dstLabel":"person","srcPkColMap":{"0":"id"},"dstPkColMap":{"1":"id"},"propertiesColMap":{"2":"date"}}}
skip.header=true
  ```
  
  Details of the parameters are listed below:
  
  | Config key | Required | Default | Description |
  | --- | --- | --- | --- |
  | split.size| false | 256 | Hadoop map-reduce input data split size in MB |
  | separator | false | \\\\\| | Seperator used to parse each field in a line | 
  | input.path | true | - | Input HDFS dir |
  | output.path | true | - | Output HDFS dir |
  | graph.endpoint | true | - | RPC endpoint of the graph storage service. You can get the RPC endpoint following this document: [GraphScope Store Service](https://github.com/alibaba/GraphScope/tree/main/charts/graphscope-store) |
  | column.mapping.config | true | - | Mapping info for each input file in JSON format. Each key in the first level should be a fileName that can be found in the `input.path`, and the corresponding value defines the mapping info. For a vertex type, the mapping info should includes 1) `label` of the vertex type, 2) `propertiesColMap` that describes the mapping from input field to graph property in the format of `{ columnIdx: "propertyName" }`. For an edge type, the mapping info should includes 1) `label` of the edge type, 2) `srcLabel` of the source vertex type, 3) `dstLabel` of the destination vertex type, 4) `srcPkColMap` that describes the mapping from input field to graph property of the primary keys in the source vertex type, 5) `dstPkColMap` that describes the mapping from input field to graph property of the primary keys in the destination vertex type, 6) `propertiesColMap` that describes the mapping from input field to graph property of the edge type |
  |skip.header|false|true|Whether to skip the first line of the input file|
  
  After data building completed, you can find the output files in the `output.path` of HDFS. The output files includes a 
  meta file named `META`, an empty file named `_SUCCESS`, and some data files that one for each partition named in the 
  pattern of `part-r-xxxxx.sst`. The layout of the output directory should look like:
  
```
/tmp/data_output
  |- META
  |- _SUCCESS
  |- part-r-00000.sst
  |- part-r-00001.sst
  |- part-r-00002.sst
  ...
```

#### 2. Loading graph partitions
  
  Now ingest the offline built data into the graph storage. If you have `load_data.sh`, then run:
  
  ```
  $ ./load_data.sh -c ingest -d hdfs://1.2.3.4:9000/tmp/data_output
  ```
  Or you can run with `java`:
  
  ```
  $ java -cp data_load_tools-0.0.1-SNAPSHOT.jar com.alibaba.maxgraph.dataload.LoadTool -c ingest -d hdfs://1.2.3.4:9000/tmp/data_output
  ```

  The offline built data can be ingested successfully only once, otherwise errors will occur.

#### 3. Commit to store service
  
  After data ingested into graph storage, you need to commit data loading. The data will not be able to read until committed successfully. If you have `load_data.sh`, then run:
  
  ```
  $ ./load_data.sh -c commit -d hdfs://1.2.3.4:9000/tmp/data_output
  ```
  Or you can run with `java`:
  
  ```
  $ java -cp data_load_tools-0.0.1-SNAPSHOT.jar com.alibaba.maxgraph.dataload.LoadTool -c commit -d hdfs://1.2.3.4:9000/tmp/data_output
  ```

  **Notice: The later committed data will overwrite the earlier committed data which have same vertex types or edge relations.**
	
