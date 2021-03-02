# Overview
GAIA (GrAph Interactive Analytics) is a full-fledged system for large-scale interactive graph analytics in the distributed context. 
GAIA is based on the Tinkerpop's Gremlin query language (https://tinkerpop.apache.org/). Given a Gremlin query, Gaia
will compile it into a dataflow with the help of the powerful Scope abstraction, and then schedule the computation in
a distributed runtime.

GAIA has been deployed at [Alibaba Corporation](https://www.alibaba.com/) to support a wide range of businesses from
e-commerce to cybersecurity. This repository contains three main components of its architecture: 
* GAIA compiler: As a main technical contribution of GAIA, we propose a powerful abstraction 
  called *Scope* in order to hide the complex control flow (e.g. conditional and loop) and fine-grained dependency in
  a Gremlin query from the dataflow engine. Taking a Gremlin query as input, the GAIA compiler is responsible for
  compiling it to a dataflow (with Scope abstraction) in order to be executed in the dataflow engine. The compiler
  is built on top of the [Gremlin server](http://tinkerpop.apache.org/docs/3.4.3/reference/##connecting-gremlin-server)
  interface so that the system can seamlessly interact with the TinkerPop ecosystem, including development tools
  such as [Gremlin Console](http://tinkerpop.apache.org/docs/3.4.3/reference/##gremlin-console)
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

# Preparement
## Dependencies
GAIA builds, runs, and has been tested on GNU/Linux (more specifically Centos 7). 
Even though GAIA may build on systems similar to Linux, we have not tested correctness or performance,
so please beware.

At the minimum, Galois depends on the following software:
* [Rust](https://www.rust-lang.org/) (>= 1.49): GAIA currently works on Rust 1.49, but we suppose that it also works
  for any later version.
* Java (jdk 8): Due to a known issue of gRPC that uses an older version of java annotation apis, the project is 
  subject to jdk 8 for now.
* Protobuf (3.0): The rust codegen is powered by [prost](https://github.com/danburkert/prost).
* gRPC: gRPC is used for communication between Rust (engine) and Java (Gremlin server/client). The Rust
implementation is powered by [tonic](https://github.com/hyperium/tonic)
* Other Rust and Java dependencies, check
    * `./gremlin/compiler/pom.xml`
    * `./gremlin/gremlin_core/Cargo.toml`
    * `./graph_store/Cargo.toml`
    * `./pegasus/Cargo.toml`

## Building codes
TODO

## Generate Graph Data
Please refer to `./graph_store/README.rd` for details.

# Deployment
## Deploy GAIA services
TODO 
### Single-machine Deployment
### Distributed Deployment
## Start Gremlin Server
After successfully building the codes, you can find `gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar` in 
`./gremlin/compiler/gremlin-server-plugin/target`, copy it to wherever you want to start the server
```
cp ./gremlin/compiler/gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/your/dir
cp -r ./gremlin/compiler/conf /path/to/your/dir
cd /path/to/your/dir
```

There are some configurations to make in `./conf`:
* Gremlin server address and port: TODO 
* The graph storage schema: For your reference, we've provided the schema file
`./conf/modern.schema.json` for [Tinkerpop's modern graph](https://tinkerpop.apache.org/docs/current/tutorials/getting-started/), 
and `./conf/ldbc.schema.json` for [LDBC generated data](https://github.com/ldbc/ldbc_snb_datagen). 
TODO: How to customize the schema

Then start up the Gremlin server using
```
java -cp .:gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar com.compiler.demo.server.GremlinServiceMain
```

## Run Query
- Download TinkerPop's official [gremlin-console](https://archive.apache.org/dist/tinkerpop/3.4.9/apache-tinkerpop-gremlin-console-3.4.9-bin.zip)
- cd `path/to/gremlin/console`, modify `conf/remote.yaml`
  ```
  hosts: [localhost]  # TODO: The hosts and port should align to the above server configuration?
  port: 8182
  serializer: { className: org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0, config: { serializeResultToString: true }}
  ```
- Console startup
  ```
  ./bin/gremlin.sh
  :remote connect tinkerpop.server conf/remote.yaml
  :remote console
  ```
- Submit query in console. Have fun!!

# Contact
TODO

# Acknowledge
TODO

# Publications
1. GAIA: A System for Interactive Analysis on Distributed Graphs Using a High-Level Language, Zhengping Qian,
Chenqiang Min, Longbin Lai, Yong Fang, Gaofeng Li, Youyang Yao, Bingqing Lyu, Xiaoli Zhou, Zhimin Chen, Jingren Zhou,
   18th USENIX Symposium on Networked Systems Design and Implementation (NSDI 2021), to appear.