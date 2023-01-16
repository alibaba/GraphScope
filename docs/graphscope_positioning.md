# The Product Positioning of GraphScope

GraphScope is an open-source, cloud-native computing framework for a variety of graph workloads, including analytical processing, interactive querying and graph learning.
But how GraphScope differs from many other graph products that offer similar functionality.
What is the advantages. We elaborate in details.

## Compare with Graph Database
 GraphScope offers a distributed computing engine that can
efficiently process interactive graph queries at large scale using widely used languages
such as Gremlin (and Cypher will be supported soon).
Unlike graph databases, such as [Neo4j](https://neo4j.com/), [JanusGraph](http://www.janusgraph.cn/) and [Nebula](https://www.nebula-graph.com.cn/), GraphScope
does not provide full-functional graph data management. In details:
 - Users must always keep a copy of the original data: GraphScope provides
the graph-native store to **cache** the graph data for efficiency, but it will not be responsible for maintaining the data. With the help of [k8s](https://kubernetes.io/), the failover mechanism ensures the relaunching of the computation, but not the recovery of graph data.
 - Users must not assume transaction, i.e. ACID: GraphScope may rely on groot to handle graph data update with a snapshot (version) protocol, which guarantees data consistencies in different snapshots. But the snapshot protocol is developed in the granularity of **seconds**, and does not have any consistency guarantee.

If you always retain a local backup of your data, and performance and scalability are your primary concerns rather than transaction guarantees, then GraphScope may be a suitable platform to try for interactive graph queries.


## Compare with Graph Processing System

## Compare with Disaggregated Design
