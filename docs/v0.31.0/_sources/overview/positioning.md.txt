# Positioning of GraphScope

[GraphScope](./overview.md) is an open-source, cloud-native computing framework for a variety of graph workloads,
including analytical processing, interactive querying and graph learning. But how does GraphScope stand out
among a bunch of graph products that offer similar functionalities? In summary,
- If you are searching for a scalable system for graph analytical processing, GraphScope may be your best option.
- If you are searching for a graph database for graph queries, and performance and scalability are your primary concerns
rather than transaction guarantees, then GraphScope is a suitable platform.
- If you are searching for large-scale distributed graph learning, GraphScope is just the solution.
- If you are searching for a system that simultaneously offers various types of graph workloads, GraphScope
is the ideal platform.

We elaborate in details in the following.

## Compare with Graph Processing Systems
Most large-scale graph processing systems, including [PowerGraph](https://github.com/jegonzal/PowerGraph), [GeminiGraph](https://github.com/thu-pacman/GeminiGraph) and [Plato](https://github.com/Tencent/plato), handles only the graph analytical processing workloads.
Our [empirical studies](./performance_and_benchmark.md) via the LDBC [Graphalytics](http://graphalytics.org/) benchmark
have demonstrated that GraphScope outperforms these systems in all algorithms.
Furthermore, GraphScope has the added advantages of supporting a range of SDKs and programming interfaces for graph analytics:
- It offers data scientists a [Python SDK](../python_tutorials.md) to quickly integrate graph processing into their existing data analytics ecosystem.
- It allows Java users of [Giraph](https://giraph.apache.org/) and [GraphX](https://spark.apache.org/graphx/) to seamlessly
  [migrate the existing code](../java_tutorials.md) (without changing a line) to GraphScope, enabling them to enjoy automatic performance improvements.

## Compare with Graph Databases
Graph databases, such as [Neo4j](https://neo4j.com/), [JanusGraph](http://www.janusgraph.cn/) and [Nebula](https://www.nebula-graph.com.cn/),
allow users to query and manipulate the graph data using a declarative query language, e.g.
[Gremlin](https://tinkerpop.apache.org/gremlin.html), [Cypher](https://neo4j.com/developer/cypher/), and [GQL](https://www.gqlstandards.org/).
Like graph databases, GraphScope supports graph queries via the interactive computing engine, or [GIE](./graphscope_for_graph_queries.md),
using widely-used query languages such as Gremlin (and Cypher will be supported soon).
However, GraphScope mainly differs from typical graph databases in two aspects.

Firstly, graph databases are often designed to have full DBMS features that allow users to not only
run queries, but also store and manage graph data. In comparison, GraphScope is more of a computing
framework that focus on running queries but has rather limited support on storage and transactions.
Specifically,
 - GraphScope contains graph stores, such as the in-memory [vineyard](https://v6d.io) and persistent groot,
  but these are only used for caching graph data for efficiency, not for storage. The failover mechanism,
  enabled by [k8s](https://kubernetes.io/), ensures the relaunch of computation upon failure, but not
  the recovery of graph data.
 - GraphScope can handle data update through a snapshot (version) protocol (provided by groot), which guarantees
  data consistencies across different versions. But the snapshot protocol is too coarse-grained (in seconds)
  to satisfy any consistency requirements for transactions.

Secondly, while most graph databases emphasize high throughput of small (short-running) queries, GraphScope
concentrate on low latency of large (long-running) analytical queries. Such kinds of queries
are widely used in [business intelligence](https://ldbcouncil.org/benchmarks/snb/),
including querying cycles (e.g. in anti money laundering),
paths (e.g. in investment tracing) and complex patterns (e.g. in fraud detection).

## Compare with Graph Learning Systems
GraphScope offers a distributed graph learning framework(GLE) to ease the development and training
of graph neural networks(GNNs). In industrial scenarios, it is common that GNNs are applied over large-scale graph,
which cannot be efficiently supported by GNN systems such as [PyG](https://github.com/pyg-team/pytorch_geometric)
and [DGL](https://github.com/dmlc/dgl). While, GLE targets for GNN training in industrial scenarios,
where the graphs are often large, e.g., billion of nodes and tens of billions of edges.
To better accommodate the resource requirements of large-scale GNN training, GLE decouples the graph
sampling and model training into independent stages, and each stage can be flexibly scaled to improve
the end-to-end throughput.

## Handling Various Types of Graph Workloads
Given various types of graph workloads, existing systems have proposed two solutions.

- One System: This solution is often referred to as ``in-database
  analytics``, and has been adopted by graph databases such as Neo4j and Nebular. It aims to
  extend the capabilities of analytical processing and graph learning on top of the existing
  engine, mainly for graph queries. However, due to the differing computing patterns of these graph
  workloads, this one-system design may experience performance degradation when adapting the
  graph-query-oriented engine for other types of workloads.
- Multi System: Given the performance issue of the one-system solution, the multi-system solution
  attempts to adopt multiple independent systems, each processing the corresponding workloads.
  The main [issue](design_of_graphscope.md) with this solution is that it requires users to manually compose the complex
  pipeline using a script language. Additionally, the requirement
  for data movement among different systems may introduce significant overhead.

In comparison, GraphScope has a [loosely-coupled](design_of_graphscope.md) design, which integrates three
engines - GAE for analytical processing, GIE for graph queries, and GLE for learning.
The non-trivial integration features a common programming interface on top for easily implement
a complex graph pipeline that can involve
one or more types of workloads, and a distributed storage layer [vineyard](https://v6d.io) for
efficient, copy-free in-memory data movements across the engines. Compared to the one-system design,
GraphScope has demonstrated superior performance due to the separated computing engine.
Compared to the multi-system design, GraphScope offers more user-friendly programming
interfaces and more efficient data movements. Additionally, GraphScope allows users to [independently
deploy each engine](./overview.md), if they only need to focus on certain workloads.

