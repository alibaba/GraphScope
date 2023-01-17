# GraphScope for Graph Analytics

Currently, there exist diverse types of graph analytics algorithms, which usually iteratively scan vertices/edges of a graph to explore underlying insights hidden in graph data. Typical graph analytics algorithms include general analytics algorithms (e.g., PageRank, shortest path, and maximum flow), community detection algorithms (e.g., maximum clique/bi-clique, connected components, Louvain and label propagation), graph mining algorithms (e.g., frequent structure mining and graph pattern discovery). 

In GraphScope, Graph Analytics Engine (GAE) is responsible for handling such graph analytics algorithms, which allows users to easily define and implement their algorithms while providing efficient execution for these algorithms.

##Rich built-in graph analytics algorithms
GAE of GraphScope provides 20 graph analytics algorithms as built-in algorithms, and users can directly invoke them. The build-in algorithms contain most commonly used algorithms, including PageRank, BFS, DFS, shortest path and LCC. In addition, GraphScope is compatible with NetworkX APIs, and thus diverse kinds of [built-in algorithms in NetworkX](https://networkx.org/documentation/stable/reference/algorithms/index.html) can also be directly invoked by users. In total, over 100 build-in graph analytical algorithms can be directly executed over GraphScope, without any developing effort.


##Customize your own algorithms
In many cases, users need to develop their customized graph analytics algorithms. To this end, GraphScope allows users to succinctly develop their own algorithms, with multiple programming models and programming languages support.

On the programming model side, GraphScope supports both the vertex-centric model (Pregel) and PIE (PEval-IncEval-Assemble) programming model. Both programming models have been widely applied in existing graph processing systems, and readers can refer to [this blog](https://graphscope.io/blog/tech/2021/03/25/a-review-of-programming-models-for-parallel-graph-processing.html) for more details.

On the programming language side, GraphScope provides a multi-language SDK, and users can choose to write their own algorithms in C++, Java or Python. With Python, users can still expect a high performance. GAE integrated a compiler built with Cython and it can generate efficient native code from Python algorithms behind the scenes, and dispatch the code to the GraphScope cluster for efficient execution. Readers can refer to the following tutorials to see how to develop their own algorithms with different programming languages.

- [Tutorials for C++ Users](https://graphscope.io/docs/latest/cpp_tutorials.html)
- [Tutorials for Java Users](https://graphscope.io/docs/latest/java_tutorials.html)
- [Tutorials for Python Users](https://graphscope.io/docs/latest/python_tutorials.html)

##High-performance runtime
GAE achieves high performance through a highly optimized analytical runtime. Many optimization techniques, such as pull/push dynamic switching, cache-efficient memory layout, and pipelining were employed in the runtime. We have performed a comparison with state-of-the-art graph processing systems on LDBC Graph Analytics Benchmark, and the results show GraphScope outperforms other graph systems (see more detailed results [here](https://github.com/alibaba/libgrape-lite/blob/master/Performance.md)). 
