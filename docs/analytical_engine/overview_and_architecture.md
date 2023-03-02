# Overview and Architecture

In GraphScope, Graph Analytics Engine (GAE) is responsible for handling various graph analytics algorithms. GAE derives from [GRAPE](https://dl.acm.org/doi/10.1145/3282488), a graph processing system proposed on SIGMOD 2017. GRAPE differs from prior systems in its ability to parallelize sequential graph algorithms as a whole. Different from other parallel graph processing systems which need to recast the existing sequential algorithm into [a new model](https://graphscope.io/docs/latest/analytical_engine/vertex_centric_models.html) (e.g., Pregel and GAS), in GRAPE, sequential algorithms can be easily “plugged into” with only minor changes and get parallelized to handle large graphs efficiently. In addition to the ease of programming, GRAPE is designed to be highly efficient and flexible, to cope the scale, variety and complexity from real-life graph applications. 

## Architecture

GAE is a full-fledged, in-production system for graph analytics over large scale graph data. As shown in the following figure, there exist three major layers in GAE, namely graph storage, execution runtime and algorithm library. Next, we give an overview to each of them below.

- Graph storage: Graph data in real-life is extremely large scale, and thus graph storage stores graph data in a distributed fashion. The graph storage consists of different graph formats with different features. Although there exist diverse types of graph storage, unified interfaces for graph storage are provided for GAE as well as other computation engines of GraphScope, and thus GAE does not care about how each type of graph storage is implemented.
- Execution runtime: At the core of GAE is its execution runtime. It offers user-friendly interfaces for graph library, supporting various programming models and programming languages, so that users can develop their algorithms freely. GAE implements many optimization techniques in its execution runtime, such as pull/push dynamic switching, cache-efficient memory layout, and pipelining, to achieve better performance.
- Algorithm library: While graph analytics algorithms can be directly written using the interfaces offered by graph storage and execution runtime, we have built an algorithm library consisting of common algorithms for various application domains, to ease the development of new graph applications. In addition, GAE has implemented the support for NetworkX, Giraph and GraphX, and existing algorithms developed for NetworkX, Giraph and GraphX can directly run on GAE directly with no or some small changes.

## Graph Storage

The graph data is represented with the [property graph model](https://www.dataversity.net/what-is-a-property-graph/) in GraphScope. A property graph is a directed graph in which vertices and edges can have a set of properties. Every entity (vertex or edge) is identified by a unique identifier (`ID`), and has a (`label`) indicating its type or role. Each property is a key-value pair with combination of entity ID and property name as the key. 

:::{figure-md}

<img src="../images/property_graph.png"
     alt="An example of property graph."
     width="80%">

An example of property graph. 
:::

The above figure shows an example property graph. It contains `user`, `product`, and `address` vertices connected by `order`, `deliver`, `belongs_to`, and `home_of` edges. A path following vertices 1–>2–>3, shown as the dotted line, indicates that a buyer “Tom” ordered a product “gift” offered by a seller “Jack”, with a price of “$99”.

To support large scale graph data, GraphScope applies *edge-cut* partitioning strategy to partition a graph into multiple sub-graphs (fragments). Specifically, as shown in the following figure, the edge-cut partitioning splits vertices of a graph into roughly equal size clusters. The edges are stored in the same cluster as one or both of its endpoints. Edges with endpoints distributed across different clusters are *crossing edges*.

:::{figure-md}

<img src="../images/ecut.png"
     alt="An example of edge-cut partitioning."
     width="80%">

An example of edge-cut partitioning. 
:::

The graph storage consists of multiple types of graph formats, and each format is dedicated to a specific scenario: some support real time graph update while others treat graph data as immutable data once created; some store graph data in memory for better performance, while others support persistent storage. Fortunately, all these storages are integrated with GRIN, a unified graph storage interface defined by GraphScope. Therefore, the computation engines are not aware of the implementation differences of different storages. Please check out [our introduction to GRIN](https://graphscope.io/docs/latest/storage_engine/grin.html) for more details about graph storage interface.

## Execution Runtime

## Algorithm Library