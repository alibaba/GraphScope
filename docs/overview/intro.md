# Introducing GraphScope

## What is GraphScope

GraphScope is a comprehensive distributed graph computing platform that offers a user-friendly Python interface for performing various graph operations on a cluster of computers. By integrating key Alibaba technologies such as GRAPE, MaxGraph, Graph-Learn (GL), and Vineyard, GraphScope simplifies multi-stage processing of large-scale graph data on compute clusters. These technologies enable analytics, interactive, and graph neural networks (GNN) computation, while Vineyard provides efficient in-memory data transfers.

## Why use GraphScope

GraphScope has several advantages:

- **Performance:** GraphScope significantly outperforms other state-of-the-art systems, offering up to 2X or higher performance improvements. Moreover, it includes a collection of optimized built-in graph algorithms for immediate use.

- **Compatibility**: It supports Gremlin and Cypher(coming soon) for quering graph, and NetworkX compatible APIs for graph analytcis.

- **PyData Integration:** GraphScope offers a user-friendly Python interface, allowing seamless integration with PyData frameworks and management of complex workflows involving multiple stages beyond graph processing.

- **Cloud Native:** GraphScope is designed for easy deployment on Kubernetes, offering excellent elasticity and scalability.


## What are the Use Cases

GraphScope is versatile and can be used for:

- **Offline Graph Analytical Jobs:** Iterative graph computations like pagerank, centrality, community detection, and graph clustering. Utilize built-in algorithms or custom ones for large graphs.

- **Online Graph BI Analysis:** Perform interactive graph analysis with complex Gremlin or Cypher queries, focusing on low latency.

- **High QPS Graph Queries:** Process high-rate graph queries with exceptional throughput.

- **Graph Neural Network Training and Inference:** Support large-scale GNNs and integrate seamlessly with PyG/TensorFlow.

- **One-stop Graph Processing**: Manage complex workflows involving multiple stages of graph analytics, queries, GNNs, and beyond.


## What are the Limitations

- **GraphScope is not a graph database:** GraphScope focuses on analyzing and computing large-scale graphs using distributed computing, rather than storing and managing data in a graph format with ACID and transactions. See [Positioning of GraphScope](positioning.md) for more information.

- **Security limitations:** GraphScope currently lacks essential security features, such as authentication and encryption.

## What are the Next

You may want to

- [Get Started](getting_started.md) with GraphScope;
- Learn more about the [Design of GraphScope](design_of_graphscope.md).