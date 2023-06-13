# GraphScope(Flex) for LDBC SNB Interactive (SUT)

## 1. Introduction

Real-life graph applications are diverse and complex in a number of ways. There are multiple types of graph workloads, such as graph analytics, graph interactive queries, graph pattern matching and GNN. There are different approaches to organize and store graph data, for example, on-disk or in-memory? mutable or immutable? distributed? transactional? There are various programming interfaces for users to write queries or algorithms: GQL, Cypher, or Gremlin? Pregel, Gather-scatter, GraphBLAS or PIE? There are diverse deployment modes and performance requirements as well, for example, as a graph computing task in an offline data analytical pipeline, or as an online service? for a higher query volume or for a lower query latency? To address such diversities, we are developing the next generation of [GraphScope](https://github.com/alibaba/GraphScope), a unified distributed graph computing platform from Alibaba. It follows a modular and disaggregated design, where components are like LEGO bricks, where users can make their customized builds and deployments of GraphScope to meet their specific graph computing needs. The development is still on-going with some efforts yet open-sourced, and we aim to fully open-source the entire stack some time in 2023.

<h1 align="center">
    <img src="https://user-images.githubusercontent.com/10632052/203890742-590b2711-27de-4ab1-b6ad-8f5e8598840b.png" alt="dataflow-graphscope">
</h1>
<p align="center">
    The architecture of GraphScope and the SUT for LDBC SNB Interactive.
</p>

To support LDBC SNB interactive benchmark, we build the SUT as follows (marked in the red box in the figure above).

- Queries: all interactive queries specifeid in the SNB benchmark as stored procedures written in C++.
- Engine: the high QPS engine [Hiactor](https://github.com/alibaba/hiactor) from the GraphScope Interactive Engine (GIE) for query execution.
- Graph store: an internal proprietary single-machine transactional graph store.
* Other unused componets such as query language front ends (Gremlin, Cypher), the compiler, query optimization, code generator and data-parallel executor Pegasus in GIE are excluded from the SUT.

## 2. Storage

The graph store is an implementation of [Real-Time Mutable Graph](../storages/rt_mutable_graph/README.md). It supports MVCC and is optimized for executing read and insert transactions concurrently. 

## 3. Engine

The [engine](../engines/graph_db/README.md) is based on [Hiactor](https://github.com/alibaba/hiactor). It provides:
 - a high-performance actor model http server
 - a transactional interface for graph store and version management
 - stored procedures management and concurrent execution

## 4. Executables

The SUT consists of the following executables:

### 4.1 Server

```
rt_server -g /path/to/schema_config -d /path/to/data [-l /path/to/load_schema] [-s n]
```

- `-d` Specifies the work directory, snapshots and write-ahead log will be generated here.
  - If the work directory is empty, the query engine will load graph defined by the input description file, and generate an initial snapshot in work directory.
  - Otherwise, the query engine will recover by loading the initial snapshot and replaying update operations.
- `-g` Specifies the schema description file.
  - `storages/rt_mutable_graph/modern_graph/modern_graph.yaml` is an example of schema description file.
- `-l` Specifies the bulk loading description file.
  - `storages/rt_mutable_graph/modern_graph/bulk_load.yaml` is an example of bulk loading description file, it defines the path of raw files and the mapping from raw files to vertex/edge labels.
- `-s` Specifies the concurrency.

Recovering from an existing work directory and then bulk loading from raw files is not supported. When the specified work directory is recoverable and the bulk loading description file is provided, the bulk loading will be ignored.

### 4.2 Client (Admin)

`rt_admin` is a tool for managing stored procedures and inspecting graph data. The corresponding `rt_admin` service can be configured with environment variables `GRAPHSCOPE_IP` and `GRAPHSCOPE_PORT`. It currently supports 5 types of operations.

```
rt_admin <operation> <args...>
```

- Show all registered stored procedures:

```
rt_admin show_stored_procedures
```

- Query vertex by providing label and id:

```
rt_admin query_vertex <LABEL> <ID>
```

- Query edges by providing labels and ids:

```
rt_admin query_edge <SRC-LABEL> <SRC-ID> <DST-LABEL> <DST-ID> <EDGE-LABEL>
```

Labels can be specified as `_ANY_LABEL` and ids can be specified as `_ANY_ID` to query a set of edges.
