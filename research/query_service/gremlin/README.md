# Overview
GAIA (GrAph Interactive Analytics) is a full-fledged system for large-scale interactive graph analytics in the distributed context.
GAIA is based on Tinkerpop's Gremlin query language (https://tinkerpop.apache.org/). Given a Gremlin query, GAIA
will compile it into a dataflow with the help of the powerful Scope abstraction, and then schedule the computation in
a distributed runtime.

GAIA has been deployed at [Alibaba Group](https://www.alibabagroup.com/) to support a wide range of businesses from
e-commerce to cybersecurity. This repository contains three main components of its architecture:
* GAIA compiler: As a main technical contribution of GAIA, we propose a powerful abstraction
  called *Scope* in order to hide the complex control flow (e.g. conditional and loop) and fine-grained dependency in
  a Gremlin query from the dataflow engine. Taking a Gremlin query as input, the GAIA compiler is responsible for
  compiling it to a dataflow (with Scope abstraction) in order to be executed in the dataflow engine. The compiler
  is built on top of the [Gremlin server](https://tinkerpop.apache.org/docs/3.4.12/reference/##connecting-gremlin-server)
  interface so that the system can seamlessly interact with the TinkerPop ecosystem, including development tools
  such as [Gremlin Console](http://tinkerpop.apache.org/docs/3.4.12/reference/##gremlin-console)
  and language wrappers such as Java and Python.
* Distributed runtime: The GAIA execution runtime provides automatic support for efficient execution of Gremlin
  queries at scale. Each query is compiled by the GAIA compiler into a distributed execution plan that is
  partitioned across multiple compute nodes for parallel execution. Each partition runs on a separate compute node,
  managed by a local executor, that schedules and executes computation on a multi-core server.
* Distributed graph store: The storage layer maintains an input graph that is hash-partitioned across a cluster,
  with each vertex being placed together with its adjacent (both incoming and outgoing) edges and their attributes.
  Here we assume that the storage is coupled with the execution runtime for simplicity, that is each
  local executor holds a separate graph partition. In production, more functionalities of storage have been developed,
  including snapshot isolation, fault tolerance and extensible apis for cloud storage services, while they are
  excluded from the open-sourced stack for conflict of interest.

# Prerequisites
## Dependencies
GAIA builds, runs, and has been tested on GNU/Linux (more specifically Centos 7).
Even though GAIA may build on systems similar to Linux, we have not tested correctness or performance,
so please beware.

At the minimum, GAIA depends on the following software:
* [Rust](https://www.rust-lang.org/) (>= 1.50): GAIA currently works on Rust 1.50+, but we suppose that it also works
  for any later version.
* Java (Must be Java 8): Due to a known issue of gRPC that uses an older version of java annotation apis, the project is
  subject to **JDK 8** for now.
* gRPC: gRPC is used for communication between Rust (engine) and Java (Gremlin server/client).
* Protobuf (3.0): Make sure you've installed [protobuf compiler](https://grpc.io/docs/protoc-installation/) in the machine you want to compile the codes.
* HDFS: Hadoop file system may be used to maintain the LDBC raw data. For supported Hadoop version, please refer to
  LDBC [datagen](https://github.com/ldbc/ldbc_snb_datagen).
* Python3: Further install the [toml](https://pypi.org/project/toml/) library for parsing a `toml`-format file.
* Maven (>= 3.6): [Maven](https://maven.apache.org/download.cgi) is required to build the Gremlin compiler.
* Other Rust and Java dependencies, check (note that from now on, we assume you are on the root directory
  of `/research/query_serivce/gremlin`):
    * `./compiler/pom.xml`
    * `./gremlin_core/Cargo.toml`
    * `../../graph_store/Cargo.toml`
    * `../../engine/pegasus/Cargo.toml`

## Build the codes
Download the codes, get into the 'scripts' folder, and then build the codes as:
```shell
cd scripts
chmod +x ./build.sh
./build.sh
```
The building process may run for a while. Thereafter, you shall find all the required
tools in 'scripts/bin' including:
```shell
ls -l bin
par_loader
simple_loader
downloader
gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar
start_rpc_server
```

In case that you are running in a cluster of several hosts, make sure that you `scp`
the scripts folder to a **common** place in all hosts, saying `/path/to/workdir`. **From now on, we assume that you are
in such a folder**.

## Prepare the Raw Data
Here we assume that LDBC data is used (generated according to [here](https://github.com/ldbc/ldbc_snb_datagen)).
If you would like to load any data source, make sure you process the data source to name the vertex data
and edge data according to LDBC. Specifically:

* Each vertex data of type \<vertex-type\> is placed in the file of: \<vertex-type\>_0_0.csv;
  - Each line of the file maintains a record of the vertex, similar to a relation table, but must have an "id" field
  to server the primary key of a vertex.
* Each edge data of type \<edge-type\>\ is placed in the file of:
  \<start-vertex-type\>\_\<edge-type\>\_\<end_vertex_type\>_0_0.csv;
  - Each line of the file maintains a record of the edge, which must at least contain a "start_id" field, and an "end_id" field
  indicating the start vertex and end vertex of the (directed) edge, respectively. In addition, the "start_id" must be
    a foreign key to the "id" of the \<start-vertex-type\>, while the "end_id" must be a foreign key to the "id" of the \<edge-vertex-type\>.
* Prepare the schema file. We have provided the modern graph schema file in "conf/modern.schema.json" for your reference,
where [modern graph](https://tinkerpop.apache.org/docs/current/reference/) is widely used for Gremlin demo.

# Deploying GAIA
The deployment of GAIA includes loading/building the graph from the raw data and start the RPC server to accept queries
from Gremlin server. We first show how to deploy in single host, and then move forward to cluster deployment.

## Singleton Deployment
### Loading Graph
To load graph data on one machine, let's first download the LDBC raw data from HDFS to somewhere locally:
`<local_ldbc_data>`. Suppose you want to maintain the storage in the folder of `<graph_store>`.

Then run:
```shell
bin/simple_loader <local_ldbc_data> <graph_store> ./conf/ldbc.schema.json -p <number_of_partitions>
```
After the loading, you shall find the following folder in your `<graph_store>` directory:
```
graph_data_bin
----partition_0
--------graph_struct
--------index_data
--------edge_property
--------node_property
graph_schema
----schema.json
```

In addition to manually building the graph data,
we have provided two built-in toy graphs for you to play with: a sampled LDBC graph data in `gremlin_core/resource/data` and
a modern graph as used in most [Tinkepop demo](https://tinkerpop.apache.org/docs/current/reference/) cases.
To quickly simulate the distributed deployment in one single machine,
you can skip building graph storage, and jump to "Start RPC Server".

### Start RPC Server
Starting RPC server locally is simple:
```shell
RUST_LOG=Info DATA_PATH=<graph_store> bin/start_rpc_server
```

This will by default start RPC server at `0.0.0.0:1234`. Type `--help` to see available options if want to customize.
As we've mentioned earlier, we have built two toy graphs for your convenience:
* To use the sampled LDBC data, specify `DATA_PATH=/path/to/this/codebase/gremlin/gremlin_core/resource/data/ldbc_graph`.
* To use the modern graph of Tinkerpop, simply remove `DATA_PATH=<graph_store>` in the above command.


## Distributed Deployment
### Partition Raw Data
We allow you to load graph data in the distributed context.
As an initial step, it is required to partition the raw data using MapReduce or Spark. We have provided a tool based on Spark for
partitioning the LDBC data. Note that the tool is built on `Spark-2.12-3.0.0-preview2` with `Scala-2.12`.
Make sure you have deployed Spark and HDFS in your cluster, prior to doing the partitioning as:
```shell
spark-submit \
  --class sample.LDBCPartition \
  --master <spark_master> \
  bin/DataPart.jar \
  <hdfs_ldbc_raw_data_dir> \
  <hdfs_ldbc_partitioned_data_dir> \
  <spark_master> \
  <number_of_ldbc_data_partitions>
```

After partitioning, a data of certain type, for example `person_0_0.csv`, should be placed in a folder named
`person`, in which there are the partitioned fragments: part-00000, part-00001, etc.

In distributed deployment, it is required to `scp` the whole `scripts` folder to the **same** directory in all hosts
of your cluster. If it is already in a network-shared file system (NFS), you are free from this step. Let's still use
`<graph_store>` as the folder to maintain the graph storage, but make sure that you have the permission to modify
the `<graph_store>` in all hosts. In addition to `<graph_store>`, you will need a `<tmp_dir>` to store the LDBC
raw data as well as some necessary temporary files. Make sure you have write permission as well,
and its volume is large enough to contain (a part of) the LDBC raw data.

A sample host file `conf/hosts.toml` is provided for your reference to configure the hosts (ip address and engine port)
in your cluster. **Important!!! Please keep in mind that the port in the host file is used for GAIA engine to communicate,
which must be different from the RPC port that will be configured while starting the RPC server**.

### Loading Graph
We have prepared the script `run_gaia.py` to facilitate building graph storage in a cluster, whose configuration
is given in `conf/hosts.toml`. You simply run
```shell
python3 run_gaia.py
    -o par_loader
    -g <graph_store>
    -d <hdfs_partitioned_ldbc_data>
    -p <number_partitions_of_ldbc_data>
    -t <tmp_dir>
    -s /path/to/scripts/conf/ldbc.schema.json  # It'd better be absolute path
    -D
    -A /path/to/hadoop_home
    -H /path/to/scripts/conf/hosts.toml  # It'd better be absolute path
```

Note that you do not need to manually download the LDBC data. The script will download the required data in all hosts
in parallel. You simply specify the partitioned LDBC data in HDFS as `<hdfs_partitioned_ldbc_data>`. The process runs
for a while, after that you shall see in the `<graph_store>` folder in all hosts the same directory structure as in
the singleton deployment.

A log folder of `logs/par_loader/<graph_name>_w1_m<number_hosts>` can be
found, in which a log file `<which_host>.log` records the standard output and error information ever captured
in the corresponding host. Here, `<graph_name>` is simply extracted from `<graph_store>` as the suffix
of the directory.

### Start RPC Server
Before starting RPC server, you
With the scripts `run_gaia.py`, starting RPC server in a cluster is straightforward, simply run:
```shell
python3 run_gaia.py
    -o start_rpc
    -g <graph_store>
    -H /path/to/scripts/conf/hosts.toml  # It'd better be absolute path
```
By default, an RPC port of **1234** is used. An option of `-P <port>` of run_gaia.py is provided to
manually set the RPC port if necessary. Note you may want to start multiple hosts in one machine. In this
case, the port will conflict. For now, we simply increment the configured RPC port for the following
hosts. For example, if the configured port is 1234, then the second host will use the port 1235, and so on.


In case of any error, a log folder of `logs/start_rpc/<graph_name>_m<number_hosts>` can be
found, in which a file `<which_host>.log` records the standard output and error ever captured
in the corresponding host.

### Cleaning up
In the distributed context, it is often the case that a process is not terminated cleanly. Such
residual process can cause an error message like: `Address already in use`.

To avoid this case, we provide an option in `run_gaia.py` to clean up residual processes:
```shell
python3 run_gaia.py
    -o clean_up
    -H /path/to/scripts/conf/hosts.toml  # It'd better be absolute path
```

# Gremlin Server and Run Queries
There are some configurations to make in `conf`:
* The graph storage schema: For your reference, we've provided the schema file
  `conf/modern.schema.json` for [Tinkerpop's modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/),
  and `conf/ldbc.schema.json` for [LDBC generated data](https://github.com/ldbc/ldbc_snb_datagen).
* Gremlin server uses `conf/ldbc.schema.json` as the default graph schema, reconfig `gremlin.graph.schema`
  in `conf/graph.properties` for your convenience.
* Gremlin server address and port: You are free from this configuration if the service is deployed in single host.
  Otherwise, please configure the "hosts" field in `conf/gaia.args.json` corresponding to the hosts that are running
  the RPC services (while starting the RPC server):

  ```json
  {
    "workers": 2,
    "hosts": [
      "host1:<rpc_port1>",
      "host2:<rpc_port2>",
      "...",
      "hostx:<rpc_portx>"
    ]
  }
  ```

  Note that if the RPC port has **not** been set while starting RPC server, the default port **1234** shall be specified.

* Gremlin server limits the maximum length of the aggregated content for a message (request or response) within 65536(B)
  re-config `maxContentLength` in `conf/gremlin-server.yaml` if needed.

Then start up the Gremlin server using
```shell
java -cp bin/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GremlinServiceMain
```

## Run Query
- Download TinkerPop's official [gremlin-console](https://archive.apache.org/dist/tinkerpop/3.4.12/apache-tinkerpop-gremlin-console-3.4.12-bin.zip)
  in your client machine
- cd `/path/to/gremlin/console`, modify `conf/remote.yaml`
  ```yaml
  hosts: [localhost]  # TODO: The hosts and port should align to the above server configuration?
  port: 8182
  serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0, config: { serializeResultToString: true }}
  ```
- Console startup
  ```shell
  bin/gremlin.sh
  :remote connect tinkerpop.server conf/remote.yaml
  :remote console
  ```
- parameterized query
  ```gremlin
   # if not set, use conf/gaia.args.json
   graph.variables().set("workers",4)
   # check variable value
   graph.variables().get("workers")
  ```
- Submit query in console. For example:
  ```gremlin
  g.V().hasLabel('PERSON').has('firstName', 'John' ).out('KNOWS').limit(1)
  ```
Note that the available labels/properties are confined to what is defined in the schema file, e.g.
`/path/to/workdir/conf/modern.schema.json` for modern graph, or `/path/to/workdir/conf/ldbc.schema.json` for ldbc data.
The compiler shall complain if the other labels are used.

## LDBC Benchmark Driver
We have connected GAIA with LDBC driver for the convenience of benchmarking.
Please refer to `/path/to/this/codebase/benchmark/README.md` for details.

# Contact
* Zhengping Qian: zhengping.qzp@alibaba-inc.com
* ChenQiang Min: chenqiang.mcq@alibaba-inc.com
* Longbin Lai: longbin.lailb@alibaba-inc.com

# Acknowledge
We thank Benli Li, Pin Gao, and Donghai Yu for answering [Plato](https://github.com/Tencent/plato) related questions.
We are grateful to Alibaba Big Data team members for their support.

# Publications
1. GAIA: A System for Interactive Analysis on Distributed Graphs Using a High-Level Language, Zhengping Qian,
Chenqiang Min, Longbin Lai, Yong Fang, Gaofeng Li, Youyang Yao, Bingqing Lyu, Xiaoli Zhou, Zhimin Chen, Jingren Zhou,
   18th USENIX Symposium on Networked Systems Design and Implementation (NSDI 2021), to appear.
