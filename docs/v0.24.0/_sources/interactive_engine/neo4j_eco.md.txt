# Neo4j Ecosystem

[Neo4j](https://neo4j.com/) is a graph database management system that utilizes graph natively to store and process data.
Unlike traditional relational databases that rely on relational schemas, Neo4j leverages the power of interconnected nodes and relationships,
forming a highly flexible and expressive data model. GIE implements Neo4j's HTTP and TCP protocol so that the system can
seamlessly interact with the Neo4j ecosystem, including development tools such as [cypher-shell] (https://dist.neo4j.org/cypher-shell/cypher-shell-4.4.19.zip)
and [drivers] (https://neo4j.com/developer/language-guides/).

The following documentations will guide you through empowering the Neo4j ecosystem
with GIE's distributed capability for large-scale graph.

```{toctree} arguments
---
caption: GIE For Tinkerpop Ecosystem
maxdepth: 2
---
neo4j/cypher_sdk
neo4j/supported_cypher
```
