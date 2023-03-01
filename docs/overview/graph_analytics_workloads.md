# Graph Analytics Workloads

## What is Graph Analytics
A graph is a data structure composed of vertices (or nodes) connected by edges. Graphs can represent many real-world data, such as social networks, transportation networks, and protein interaction networks, as shown in the following figure.

![Examples of graphs](./images/graph_examples.png)

Broadly speaking, any kind of computation over graph data can be regarded as graph analytics. The goal of graph analytics is to discover and exploit the structure of graphs, which can provide insights into the relationships and connections between the different elements in graphs. We find that computation patterns of different graph analytics vary a lot: some only involve a small number of vertices/edges, while others access a large fraction of (even all) vertices/edges of a graph. In GraphScope, we call the former as *graph traversal*, while use the term *graph analytics* to refer to the latter, unless otherwise specified. 

Currently, there exist diverse types of graph analytics algorithms, which usually iteratively access a large fraction of (even all) vertices/edges of a graph to explore underlying insights hidden in graph data. Typical graph analytics algorithms include general analytics algorithms (e.g., PageRank, the shortest path, and maximum flow), community detection algorithms (e.g., maximum clique/bi-clique, connected components, Louvain and label propagation), graph mining algorithms (e.g., frequent structure mining and graph pattern discovery). Next, we give several typical examples to show how graph analytics algorithms work.

The [PageRank](https://en.wikipedia.org/wiki/PageRank) algorithm can measure the importance of each vertex in a graph, by iteratively counting the number and importance of its neighbors to a vertex to determine a rough estimate of how important the vertex is. Specifically, the computation of PageRank consists of multiple iterations, each vertex in a graph is assigned a value indicating its importance at first. In each iteration, each vertex sums the values of its neighbors pointing to it, and updates its own value. 

![PageRank](./images/pagerank.png)

[The shortest path problem](https://en.wikipedia.org/wiki/Shortest_path_problem) aims to find a path between two vertices such that the sum of the weights of its constituent edges is minimized, and several well-known algorithms (e.g., Dijkstra's algorithm and Bellman–Ford algorithm) have been proposed to solve this problem. Taking Dijkstra's algorithm as an example, it selects a single vertex as the "source" vertex and attempts to find shortest paths from the source to all other vertices in the graph. The computation of Dijkstra's algorithm also contains multiple iterations, and in each iteration, a vertex, whose shortest path to the source has been obtained, is selected, and updates the shortest path value of its neighbors, as shown in the following [figure](https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm).

![Dijkstra's algorithm](./images/sssp.gif)

[The community detection algorithms](https://en.wikipedia.org/wiki/Community_structure) (e.g., Louvain) are to find groups of vertices that are, in some sense, more similar to each other than to the other vertices, based on the observation that vertices of a graph can be grouped into (potentially overlapping) sets of vertices such that each set of vertices is densely connected internally. Basically, in community detection algorithms, each vertex repeatedly sends its label to its neighbors, and each vertex updates its own label according to some rules after receiving labels of its neighbors. After multiple iterations, vertices which are densely connected internally have the same/similar label.

![Community detection algorithm](./images/comm_detection.png)

From the above examples, we can see that the graph analytics algorithms can analyze the properties of a set of vertices/edges in the graph.
In real applications, many problems can be modeled as graph analytics problems. As shown in the following figure, Google Search treats all websites as well as links among websites as a graph, and then the PageRank algorithm can be applied to find the most important websites on the Internet. The road map of a city can also be modeled as a graph, and the shortest path algorithm can help path planning in logistics and delivery services. If we consider all users on a social media as a graph, community detection techniques (e.g., Louvain) are useful to discover users with common interests and keep them tightly connected.


![Applications of graph analytics](./images/analytics_examples.png)


## Challenges of Graph Analytics on Large Graphs
Based on our experience, the graph data we need to process and the frameworks (systems) used to process graph data face the following challenges:

- **Supporting large-scale and complex graph data**

    We have observed that vast majority of real-world graph data is large-scale, heterogeneous, attributed. For example, nowadays e-commerce graphs often contain billions of vertices and tens of billions of edges, with various types and rich attributes. How to represent and store such graph data is nontrivial.

- **Various programming models/languages**

    Currently, many graph processing systems have been developed to handle graph analytics algorithms. These systems are implemented with different programming models (e.g., vertex-centric model and PIE model) and programming languages (e.g., C++, Java and Python). As a result, users usually suffer from high learning curves.

- **Requirement for high performance**
    
    The scale and efficiency of processing large graphs is still limited. Although current systems have largely benefited from years of work in optimizations for each individual system, they still suffer from efficiency and/or scale problems. Offering superior performance when facing large-scale graph data is highly desired.

## What can GraphScope Do 

In GraphScope, Graph Analytics Engine (GAE) is responsible for handling graph analytics algorithms. It addresses the abovementioned challenges in the following ways:

- **Managing graph data in a distributed way**

    In GraphScope, graph data is represented as property graph model. To support large-scale graph, GraphScope automatically partitions the whole graph into several subgraphs (fragments) distributed into multiple machines in a cluster. Meanwhile, GraphScope provides user-friendly interfaces for loading graphs to allow users to manage graph data easily. More detials about how to manage large-scale graphs can refer to [this](https://graphscope.io/docs/latest/graph_formats.html).


- **Supporting various programming models/languages**

    On the programming model side, GraphScope supports both the vertex-centric model (Pregel) and PIE (PEval-IncEval-Assemble) programming model. Both programming models have been widely applied in existing graph processing systems, and readers can refer to our introduction to [Pregel](https://graphscope.io/docs/latest/analytical_engine/vertex_centric_models.html) and [PIE](https://graphscope.io/docs/latest/analytical_engine/programming_model_pie.html) models for more details.

    On the programming language side, GraphScope provides  SDKs for multiple languages, and users can choose to write their own algorithms in C++, Java or Python. Users can develop their own algorithms with different programming languages.

    Please check out [our turorials on how to develop customized algorithms](https://graphscope.io/docs/latest/analytical_engine/customized_algorithms.html) for more details.

- **High-performance runtime**

    GAE achieves high performance through a highly optimized analytical runtime. Many optimization techniques, such as pull/push dynamic switching, cache-efficient memory layout, and pipelining were employed in the runtime. We have performed a comparison with state-of-the-art graph processing systems on LDBC Graph Analytics Benchmark, and the [results](https://graphscope.io/docs/latest/performance_and_benchmark.html) show GraphScope outperforms other graph systems. 
