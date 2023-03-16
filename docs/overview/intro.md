# Introducing GraphScope

## What is GraphScope

GraphScope is a unified graph computing platform that provides a one-stop solution for graph computing. It provides a unified programming model for both graph analytics and graph neural networks, and supports a variety of graph data formats, including property graphs, heterogeneous graphs, and relational graphs. GraphScope also provides a unified runtime engine that can run both graph analytics and graph neural networks on a single cluster.

## Why use GraphScope

GraphScope has several advantages:

- **Performance:** GraphScope outperforms other state-of-the-art systems by 2X to magnitudes. Additionally, it provides a set of optimized built-in graph algorithms for out-of-the-box use. 

- **Compatibility**: It supports Gremlin and Cypher(coming soon) for quering graph, and NetworkX compatible APIs for graph analytcis.

- **Integration to PyData:** GraphScope provides a user-friendly Python interface, making it easy to integrate with other PyData frameworks and manage complex workflows that involve multiple stages beyond graph processing. 

- **Cloud Native:** which enabling it to be easily deployed on Kubernetes and providing good elasticity and scalability. 


## What are the Use Cases

GraphScope can be used for various purposes:

- **For Offline Graph Analytical Jobs:** These jobs are usually iterative graph computation jobs that require multiple rounds of computation, e.g., pagerank, centrality, community detection, and graph clustering. You can use either built-in algorithms or plug-in customized algorithms, and run them over large graphs.

- **For Online Graph BI Analysis:** You can use GraphScope for interactive graph analysis, which usually complex graph queries written in Gremlin or Cypher, the concurrency is unlikely to be high, but the latency for complex queries is the key concern. 

- **For High QPS Graph Queries:** You can also process graph queries coming at an extremely high rate and demand high throughput.

- **For Graph Neural Network Training and Inference:** GraphScope supports large-scale GNN and easily integrates with PyG/TensorFlow.

- **One-stop Graph Processing**: For a complex workflow that involves multiple stages of graph analytics, graph queries and GNNs, even beyond graph processing.


## What are the Limitations

- **GraphScope is not a graph database:** A graph database stores and manages data in a graph format, and provides ACID and transactions. While GraphScope focuses on analyzing and computing large-scale graphs using distributed computing. Refer to [Positioning of GraphScope](overview/positioning.md) for more details.

- **Security issue:** Currently, GraphScope lacks important security features such as authentication and encryption.

## What are the Next

You may want to

- [Get Started](overview/getting_started.md) with GraphScope;
- Learn more about the [Design of GraphScope](overview/design_of_graphscope.md);