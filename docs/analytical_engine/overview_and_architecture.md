# Overview and Architecture

In GraphScope, Graph Analytical Engine (GAE) is responsible for handling various graph analytics algorithms. GAE derives from [GRAPE](https://dl.acm.org/doi/10.1145/3282488), a graph processing system proposed on SIGMOD 2017. GRAPE differs from prior systems in its ability to parallelize sequential graph algorithms as a whole. Different from other parallel graph processing systems which need to recast the existing sequential algorithm into [a new model](https://graphscope.io/docs/latest/analytical_engine/vertex_centric_models.html) (e.g., Pregel and GAS), in GRAPE, sequential algorithms can be easily “plugged into” with only minor changes and get parallelized to handle large graphs efficiently. In addition to the ease of programming, GRAPE is designed to be highly efficient and flexible, to cope the scale, variety and complexity from real-life graph applications. 

## Architecture

GAE is a full-fledged, in-production system for graph analytics algorithms over large scale graph data. As shown in the following figure, there exist three major layers in GAE, namely storage layer, engine layer and application layer. Next, we give an overview to each of them below.

:::{figure-md}

<img src="../images/gae_arch.png"
     alt="Architecture of GAE."
     width="70%">

Architecture of GAE. 
:::

- Storage layer: Graph data in real-life is extremely large scale, and thus graph storage stores graph data in a distributed fashion. The graph storage consists of different graph formats with different features. Although there exist diverse types of graph storage, unified interfaces for graph storage are provided for GAE as well as other computation engines of GraphScope, and thus GAE does not care about how each type of graph storage is implemented.
- Engine layer: At the core of GAE is its engine layer. It offers user-friendly interfaces for graph algorithms, supporting various programming models, programming languages and computation patterns, so that users can develop their algorithms freely. GAE implements many optimization techniques in its engine layer, such as pull/push dynamic switching, cache-efficient memory layout, and pipelining, to achieve better performance.
- Application layer: The application layer offers an application SDK which provides user-friendly interfaces of accessing graph structural/property data, and communication between different sub-graphs (fragments), so that users can develop their own algorithms easily. While graph analytics algorithms can be directly written using the application SDK, we have built a built-in algorithm library consisting of common algorithms for various application domains, to ease the development of new graph applications.

## Storage Layer

The graph data is represented with the [property graph model](https://www.dataversity.net/what-is-a-property-graph/) in GraphScope. A property graph is a directed graph in which vertices and edges can have a set of properties. Every entity (vertex or edge) is identified by a unique identifier (`ID`), and has a (`label`) indicating its type or role. Each property is a key-value pair with combination of entity ID and property name as the key. 

:::{figure-md}

<img src="../images/property_graph.png"
     alt="An example of property graph."
     width="40%">

An example of property graph. 
:::

The above figure shows an example property graph. It contains `user`, `product`, and `address` vertices connected by `order`, `deliver`, `belongs_to`, and `home_of` edges. A path following vertices 1–>2–>3, shown as the dotted line, indicates that a buyer “Tom” ordered a product “gift” offered by a seller “Jack”, with a price of “$99”.

To support large scale graph data, GraphScope applies *edge-cut* partitioning strategy to partition a graph into multiple sub-graphs (fragments). Specifically, as shown in the following figure, the edge-cut partitioning splits vertices of a graph into roughly equal size clusters. The edges are stored in the same cluster as one or both of its endpoints. Edges with endpoints distributed across different clusters are *crossing edges*.

:::{figure-md}

<img src="../images/ecut.png"
     alt="An example of edge-cut partitioning."
     width="40%">

An example of edge-cut partitioning. 
:::

The graph storage consists of multiple types of graph formats, and each format is dedicated to a specific scenario: some support real time graph update while others treat graph data as immutable data once created; some store graph data in memory for better performance, while others support persistent storage. Fortunately, all these storages are integrated with GRIN, a unified graph storage interface defined by GraphScope. Therefore, the computation engines are not aware of the implementation differences of different storages. Please check out [our introduction to GRIN](https://graphscope.io/docs/latest/storage_engine/grin.html) for more details about graph storage interface.

## Engine Layer

There are four major components in the engine layer of GAE, which offers easy-to-use interfaces for graph algorithms.

### GRAPE Engine

The foundation of the engine layer is GRAPE, which offers core functions for graph analytics, including accessing to vertices/edges of a sub-graph (fragment), visiting property data of vertices/edges, state synchronization between different fragments, and intermediate data management.  Meanwhile, GRAPE applies many graph-level optimizations, such as indexing, compression, dynamic push/push switch, pipelining, and load balancing, to achieve better performance. Please refer to [this](https://dl.acm.org/doi/10.1145/3282488) for more details about these optimizations.

### Programming Model APIs 

Currently, GAE has supported APIs of three programming models: Pregel, PIE and FLASH. [Pregel](https://graphscope.io/docs/latest/analytical_engine/vertex_centric_models.html#pregel-model) is a widely-applied vertex-centric programming model in existing popular graph processing systems, such as GraphX and Giraph. [PIE](https://graphscope.io/docs/latest/analytical_engine/programming_model_pie.html) is proposed in GRAPE, and it can automatically parallelize existing sequential graph algorithms with some minor changes. [FLASH](https://graphscope.io/docs/latest/analytical_engine/flash.html) is a flexible programming model which can help users to implement complex graph algorithms easily. Users can freely choose one of these three programming models according to their backgrounds and demands.

### FFIs

The GRAPE engine is written with C++ for better performance, and GAE also provides Python and Java interfaces through FFI (Foreign Function Interface). With Python, it is natural and easy for GraphScope to inter-operate with other Python data processing systems such as Pandas and Mars. Based on an efficient FFI for Java and C++ [fastFFI](https://github.com/alibaba/fastFFI), GAE allows users to write Java applications and run these applications on GAE directly. Please check out our tutorials on how to develop algorithms with Python and Java.

- [Tutorial: Develop Algorithms in Python](https://graphscope.io/docs/latest/analytical_engine/tutorial_dev_algo_python.html)
- [Tutorial: Develop your Algorithm in Java with PIE Model](https://graphscope.io/docs/latest/analytical_engine/tutorial_dev_algo_java.html)

### Incremental Computation Engine 

GAE also supports incremental computation over graph data via the [Ingress](http://vldb.org/pvldb/vol14/p1613-gong.pdf) engine, where we apply a batch algorithm to compute the result over the original graph *G* once, followed by employing an incremental algorithm to adjust the old result in response to the input changes *$\Delta$**G* to *G*. To achieve this , based on the dynamic graph storage, we have implemented an Ingress fragment which can capture the changes between original and new graphs. Please check out [our detailed introduction to the Ingress engine](https://graphscope.io/docs/latest/analytical_engine/ingress.html) for more details.


## Application Layer

GAE provides C++, Python and Java SDKs for graph applications, where users can freely choose programming models, programming languages, and computation patterns (batch computation or incremental computation) to develop their own applications. GAE of GraphScope also provides [20 graph analytics algorithms](https://graphscope.io/docs/latest/analytical_engine/builtin_algorithms.html) as built-in algorithms, and users can directly invoke them. GraphScope is compatible with NetworkX APIs, and thus diverse kinds of [built-in algorithms in NetworkX](https://networkx.org/documentation/stable/reference/algorithms/index.html) can also be directly invoked by users. In total, over 100 build-in graph analytical algorithms can be directly executed over GraphScope, without any developing effort. In addition, we have implemented the support for Pregel model in GAE, and graph aglorithms implemented in Giraph or GraphX can also be directly run on GAE. Please refer to the following tutorials on how to run NetworkX/Giraph/GraphX applications on GAE.

- [Tutorial: Graph Operations with NetowrkX APIs](https://graphscope.io/docs/latest/analytical_engine/tutorial_networkx_operations.html)
- [Tutorial: Graph Algorithms with NetowrkX APIs](https://graphscope.io/docs/latest/analytical_engine/tutorial_networkx_algorithms.html)
- [Tutorial: Run Giraph Applications on GraphScope](https://graphscope.io/docs/latest/analytical_engine/tutorial_run_giraph_apps.html)
- [Tutorial: Run GraphX Applications on GraphScope](https://graphscope.io/docs/latest/analytical_engine/tutorial_run_graphx_apps.html)