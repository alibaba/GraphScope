# Overview

GraphScope Interactive is a specialized construction of [GraphScope Flex](https://github.com/alibaba/GraphScope/tree/main/flex), designed to handle concurrent graph queries at an impressive speed. Its primary goal is to process as many queries as possible within a given timeframe, emphasizing a high query throughput rate. To quickly get started with GraphScope Interactive, please refer to the guideline on the [page](./getting_started.md).

## Built on a Solid Foundation
GraphScope Interactive is built upon two key components:
1. GraphScope Flex: GraphScope Flex is a new architecture of GraphScope that provides a solid foundation for GraphScope Interactive. It is designed to handle high-QPS (Queries Per Second) scenarios, making it an ideal base for the high-throughput demands of GraphScope Interactive.
2. Record-Breaking Benchmarking Results: GraphScope Interactive is built upon the [record-breaking LDBC SNB Interactive benchmarking results](https://ldbcouncil.org/benchmarks/snb-interactive/), which has achieved a throughput of 33,180.87 ops/s for the benchmarking.  making it one of the most efficient systems for graph query processing.

## Key Features

GraphScope Interactive boasts several key features:
1. High-QPS Scenario Handling: As mentioned, GraphScope Interactive is built upon GraphScope Flex, which is designed to handle high-QPS scenarios. This allows GraphScope Interactive to process a large number of queries in a short amount of time.
2. Support for Cypher and C++: GraphScope Interactive supports using [Cypher](https://opencypher.org/) for ad-hoc queries, and both Cypher and C++ for registering stored procedures. This provides flexibility for developers and allows for efficient processing of graph queries.
3. Adaptable Expansion: Leveraging the capabilities of GraphScope Flex, GraphScope Interactive offers a flexible extension in several aspects: 
  a. Support for Multiple Languages: In the near future, GraphScope Interactive will extend its language support to include [Gremlin](https://tinkerpop.apache.org/gremlin.html), and [GQL](https://www.gqlstandards.org/), further enhancing its versatility. 
  b. Scalability: Currently optimized for high throughput on a single-machine architecture, GraphScope Interactive possesses the potential for distributed processing. This means it can be expanded with few effort to handle larger-scale graphs, ensuring it remains effective as your data grows.

## Property Graph Model and Graph Queries
GraphScope Interactive supports the property graph model, which allows for the representation of complex structures in an intuitive manner. It also supports various types of graph queries, including:
1. Traversal Query: This type of query involves traversing the graph from a set of source vertices while satisfying constraints on the vertices and edges that the traversal passes. It typically accesses a small portion of the graph rather than the whole graph.
2. Pattern Matching: Given a pattern that is a small graph, pattern matching aims to compute all occurrences of the pattern in the graph. It often involves relational operations to project, order, and group the matched instances.

## The Cypher Query Language
GraphScope Interactive supports Cypher, a graph query language developed by Neo4j. Cypher is designed specifically for working with graph data and provides an efficient and scalable solution for querying and manipulating graph data. It offers a visual way of matching patterns and relationships, making it intuitive and easy to use. 

The application of Cypher in GraphScope can be categorized into two main areas:
1. On-the-Fly Read Queries: Users can submit Cypher read queries directly to the engine. These queries are then automatically compiled into a dynamic library for execution, providing immediate results.
2. Stored Procedures: Users can create a Cypher file with specified parameters denoted as ``${param_name}''. Using the admin tool, this code can be compiled into a stored procedure and registered within the system. This stored procedure can then be invoked for future use via Cypher's CALL clause.

## Limitations
1. GraphScope Interactive now only supports a subset of Cypher read clause, as indicated [here](https://graphscope.io/docs/latest/interactive_engine/neo4j/supported_cypher). Write and DDL (for defining and modifying schema) functionalities are not currently provided. 
2. Writes are fulfilled via built-in stored procedures. The currently supports writes are: add a vertex of given type, add an edge of given type, modify vertex/edge's given properties. Details are in: TODO
3. ACID supports: TODO
