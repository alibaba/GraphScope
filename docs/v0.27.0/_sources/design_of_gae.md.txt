# Design of GAE

In GraphScope, Graph Analytics Engine (GAE) is responsible for handling various graph analytics algorithms. GAE in GraphScope derives from [GRAPE](https://dl.acm.org/doi/10.1145/3282488), a graph processing system proposed on SIGMOD-2017. GRAPE differs from prior systems in its ability to parallelize sequential graph algorithms as a whole. Different from other parallel graph processing systems which need to recast the entire algorithm into a new model, in GRAPE, sequential algorithms can be easily “plugged into” with only minor changes and get parallelized to handle large graphs efficiently. In addition to the ease of programming, GRAPE is designed to be highly efficient and flexible, to cope the scale, variety and complexity from real-life graph applications. 

GAE has three main components: graph storage, execution framework and algorithm library. Next, we give an overview to each of them below.

## Graph Storage

GAE as well as other execution engines of GraphScope work on top of a unified graph storage. The graph storage consists of different graph formats with different features, e.g., some support real time graph update while others treat graph data as immutable data once created; some store graph data in memory for better performance, while others support persistent storage. Although there exist diverse types of graph storage, GraphScope offers a unified interfaces for graph storage, and thus GAE does not care about how each type of graph storage is implemented. Please check out [our introduction for various graph storage](https://graphscope.io/docs/latest/unified_interfaces.html) for more details.


## Execution Framework

At the core of GAE is its execution framework, which has the following features in order to handle efficient graph analytics execution over distributed and large-scale graph data.

### Flexible programming models
There exist many programming models in various graph analytics systems, and GAE supports both PIE model vertex-centric model (Pregel). Different from other programming models which need to recast the entire algorithm into a new model, in PIE, the execution of existing sequential algorithms can be automatically parallelized with only minor changes. To be more specific, as shown in the following figure, in the PIE model, users only need to provide three functions, (1) PEval, a function for given a query, computes the answer on a local graph; (2) IncEval, an incremental function, computes changes to the old output by treating incoming messages as updates; and (3) Assemble, which collects partial answers, and combines them into a complete answer. After that, GAE auto-parallelizes the graph analytics tasks across a cluster of workers. More details of the PIE model can refer to [this](https://dl.acm.org/doi/10.1145/3282488).

![Workflow of PIE](./images/pie.png)

### Multi-language SDKs
Multi-language SDKs are provided by GAE. Users choose to write their own algorithms in either C++, Java or Python. With Python, users can still expect a high performance. GAE integrated a compiler built with Cython. It can generate efficient native code from Python algorithms behind the scenes, and dispatch the code to the GraphScope cluster for execution. The SDKs further lower the total cost of ownership of graph analytics.

### High-performance runtime

GAE achieves high performance through a highly optimized analytical runtime based on libgrape-lite. Many optimization techniques, such as pull/push dynamic switching, cache-efficient memory layout, and pipelining were employed in the runtime. It performs well in LDBC Graph Analytics Benchmark, and outperforms other state-of-the-art graph systems. GAE is designed to be highly efficient and flexible, to cope with the scale, variety and complexity from real-life graph analytics applications.


## Algorithm Library

GAE of GraphScope provides 20 graph analytics algorithms as built-in algorithms, and users can directly invoke them. The full lists of build-in algorithms are:

- `sssp(src)`
- `pagerank()`
- `lpa()`
- `kkore()`
- `kshell()`
- `hits()`
- `dfs(src)`
- `bfs(src)`
- `voderank()`
- `clustering()`
- `all_pairs_shortest_path_length()`
- `attribute_assortativity.()`
- `average_degree_assortativity.()`
- `degree_assortativity.()`
- `betweenness_centrality()`
- `closeness_centrality()`
- `degree_centrality()`
- `eigenvector_centrality()`
- `katz_centrality()`
- `sampling_path()`


In addition, GraphScope is compatible with NetworkX APIs, and thus diverse kinds of [built-in algorithms in NetworkX](https://networkx.org/documentation/stable/reference/algorithms/index.html) can also be directly invoked by users. In total, over 100 build-in graph analytical algorithms can be directly executed over GraphScope, without any developing effort.