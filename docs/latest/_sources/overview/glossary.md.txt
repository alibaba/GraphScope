# Glossary

This page lists all the golssary used in the GraphScope project.

#### FLASH

FLASH is a distributed programming model for programming a broad spectrum of graph algorithms, including clustering, centrality, traversal, matching, mining, etc. FLASH follows the vertex-centric philosophy, but it moves a step further for stronger expressiveness by providing flexible control flow, the operations on arbitrary vertex sets and beyond-neighborhood communication. FLASH makes diverse complex graph algorithms easy to write at the distributed runtime. The algorithms expressed in FLASH take only a few lines of code, and provide a satisfactory performance.

FLASH is planned to be integrated to GraphScope, as a module in the analytical engine(GAE).

Learn more: [Paper on ICDE2023](#), [Doc](https://graphscope.io/docs/latest/analytical_engine/flash.html)


#### GRAPE

the code name of analytical engine in GraphScope. It was first indroduced in the SIGMOD2017 paper, represents <u>GRA</u>ph <u>P</u>arallel processing <u>E</u>ngine. The analytical engine inherits the design proposed in this paper.

The core part of GRAPE is opensourced at https://github.com/alibaba/libgrape-lite, which is a dependency and serves as the analytical engine in GraphScope. 

Read more: [Best Paper on SIGMOD2017](https://homepages.inf.ed.ac.uk/wenfei/papers/sigmod17-GRAPE.pdf), [Design of GAE](https://graphscope.io/docs/latest/analytical_engine/design_of_gae.html)

#### GAE

GAE is short for GraphScope Anlaytical Engine, a.k.a., GRAPE.

#### GART

GART is short for <u>G</u>raph <u>A</u>nalysis and <u>R</u>elational <u>T</u>ransactions. It is a loosely-coupled framework that can be deployed to bridge an existing relational OLTP system (e.g., MySQL) with the graph-specific system GraphScope. In a nutshell, GART exploits the high availability mechanism of modern OLTP systems, and leverages write-ahead logs (WALs) of OLTP systems to extract graphs on which to perform graph-related workloads over execution engines of GraphScope. Instead of offline data migration, GART reuses WALs to replay graph data online with multi-version support.

GART is planned to be integrated to GraphScope, in the storage layer.

#### GRIN

GRIN is a proposed standard <u>G</u>raph <u>R</u>etrieval <u>IN</u>terface in GraphScope. The goal of GRIN is to provide a common way for the graph computing engines to retrieve graph data stored in different storage engines in GraphScope, and to simplify the integration of these engines with each other.

GRIN is defined in C, which makes it portable to systems written in different programming languages, such as C++, Rust and Java. It provides a set of common operations and data structure handlers that can be used to access graph data, regardless of the underlying storage engine.

Repo: https://github.com/graphscope/grin


#### GraphAr

GraphAr (short for <u>Graph</u> <u>Ar</u>chive) is a project that aims to make it easier for diverse applications and systems (in-memory and out-of-core storages, databases, graph computing systems, and interactive graph query frameworks) to build and access graph data conveniently and efficiently.

Repo: https://github.com/alibaba/GraphAr

#### Groot

Groot is the code name of the persistent storage in GraphScope. It was named after the tree Groot in the movie "Guardians of the Galaxy". The name "g-root" also indicates the fundamental role of the storage component in the graph system. 

[Read more](https://graphscope.io/docs/latest/storage_engine/groot.html)


#### GUM

GUM is a highly efficient multi-GPU graph analytics engine that employs a work-stealing mechanism to tackle both dynamic load imbalances and long-tail issues. It does so by utilizing the high-speed NVLinks found in modern multi-GPU servers. Additionally, GUM is acutely aware of the asymmetric topology of GPU connections and generates an optimal task-stealing plan from a holistic perspective during each iteration. With this innovative approach, GUM can offer a significant order-of-magnitude improvement in performance.

GUM serves as an GPU-speedup module for GRAPE when applicable.

Read more: [Paper on ICDE2023](#), [Code](https://github.com/alibaba/libgrape-lite/tree/gum)


#### MaxGraph (deprecated)

MaxGraph is the deprecated interactive engine in GraphScope. It was replaced by a new designed engine GAIA with multiple query language (e.g., Gremlin/Cypher...) support. 


#### Vineyard, or v6d

Vineyard is the code name of the immutable memory mangement. Its name suggests it was a underlying component of GRAPE.

Vineyard (v6d) is an innovative in-memory immutable data manager that offers out-of-the-box high-level abstractions and zero-copy in-memory sharing for distributed data in various big data tasks, such as graph analytics (e.g., GraphScope), numerical computing (e.g., Mars), and machine learning. It is a CNCF sandbox project.

Repo: https://github.com/v6d-io/v6d


#### GAIA

TODO(Robin): Add this.

#### Ingress

Ingress is an automated system for incremental graph processing. It is able to incrementalize batch vertex-centric algorithms into their incremental counterparts as a whole, without the need of redesigned logic or data structures from users. Underlying Ingress is an automated incrementalization framework equipped with four different memoization policies, to support all kinds of vertex-centric computations with optimized memory utilization.

Ingress is planned to be integrated into GraphScope, as an incrementalization module in the analytical engine(GAE).

Read more: [Paper on VLDB2021](http://vldb.org/pvldb/vol14/p1613-gong.pdf)

#### Pegasus

The code name of runtime engine under the GAIA.
TODO(Robin): Add this.

#### PIE-model

The subgraph centric programming model proposed in GRAPE, which is short for <u>P</u>artial evaluation, <u>I</u>ncremental evaluation and assembl<u>E</u>, the three core functions to fulfill in GRAPE.