# Graph Analytics Workloads
## What is Graph Analytics
Broadly speaking, any kind of computation over graph data can be regarded as graph analytics. We find that computation patterns of different graph analytics vary a lot: some only involve a small number of vertices/edges, while others access a large fraction of (even all) vertices/edges of a graph. In GraphScope, we call the former as *graph traversal*, whil use the term *graph analytics* to refer to the latter, unless otherwise specified. 

Currently, there exist diverse types of graph analytics algorithms, which usually iteratively access a large fraction of (even all) vertices/edges of a graph to explore underlying insights hidden in graph data. Typical graph analytics algorithms include general analytics algorithms (e.g., PageRank, the shortest path, and maximum flow), community detection algorithms (e.g., maximum clique/bi-clique, connected components, Louvain and label propagation), graph mining algorithms (e.g., frequent structure mining and graph pattern discovery). These graph analytics algorithms have been widely applied in real scenarios. For example,  as shown in the following figure, Google uses PageRank to rank web pages in their search engine results, and the shortest path algorithm can help path planning in logistics and delivery services.


## Characteristics of Graph Analytics Workloads
Based on our experience, the graph data we need to process and the frameworks (systems) used to process graph data have the following characteristics:

### Large-scale and complex graph data

We have observed that vast majority of real-world graph data is large-scale, heterogeneous, attributed. For example, nowadays e-commerce graphs often contain billions of vertices and tens of billions of edges, with various types and rich attributes. How to represent and store such graph data is nontrivial.

### Various programming models/languages

Currently, many graph processing systems have been developed to handle graph analytics algorithms. These systems are implemented with different programming models (e.g., vertex-centric model and PIE model) and programming languages (e.g., C++, Java and Python). As a result, users usually suffer from high learning curves.

### Requirement for high performance

The scale and efficiency of processing large graphs is still limited. Although current systems have largely benefited from years of work in optimizations for each individual system, they still suffer from efficiency and/or scale problems. Offering superior performance when facing large-scale graph data is highly desired.

## What can GraphScope Do 

In GraphScope, Graph Analytics Engine (GAE) is responsible for handling graph analytics algorithms.It addresses the abovementioned challenges in the following ways:

### Managing graph data in a distributed way 

In GraphScope, graph data is represented as property graph model, which can model vertices, edges and properties well. To support large-scale graph, GraphScope automatically partitions the whole graph into several subgraphs (fragments) distributed into multiple machines in a cluster. Meanwhile, GraphScope provides user-friendly interfaces for loading graphs to allow users to manage graph data easily.


### Supporting various programming models/languages

On the programming model side, GraphScope supports both the vertex-centric model (Pregel) and PIE (PEval-IncEval-Assemble) programming model. Both programming models have been widely applied in existing graph processing systems, and readers can refer to [this blog](https://graphscope.io/blog/tech/2021/03/25/a-review-of-programming-models-for-parallel-graph-processing.html) for more details.

On the programming language side, GraphScope provides a multi-language SDK, and users can choose to write their own algorithms in C++, Java or Python. Readers can refer to the following tutorials to see how to develop their own algorithms with different programming languages.

- [Tutorials for C++ Users](https://graphscope.io/docs/latest/cpp_tutorials.html)
- [Tutorials for Java Users](https://graphscope.io/docs/latest/java_tutorials.html)
- [Tutorials for Python Users](https://graphscope.io/docs/latest/python_tutorials.html)

### High-performance runtime

GAE achieves high performance through a highly optimized analytical runtime. Many optimization techniques, such as pull/push dynamic switching, cache-efficient memory layout, and pipelining were employed in the runtime. We have performed a comparison with state-of-the-art graph processing systems on LDBC Graph Analytics Benchmark, and the results show GraphScope outperforms other graph systems (see more detailed results [here](https://graphscope.io/docs/latest/performance_and_benchmark.html)). 
