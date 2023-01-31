# Key Features

## Ease-of-use: Python Interface

The [Python interface](https://graphscope.io/docs/latest/reference/python_index.html) of GraphScope offers an intuitive and user-friendly way for data scientists to develop, test and deploy complex graph computation workflows quickly and correctly. The [session](https://graphscope.io/docs/latest/reference/session.html#session-object) abstraction encapsulates the environment where data manipulation and graph computation operations are executed or evaluated. The Python clients interact with the GraphScope graph computation service cluster via sessions. 

GraphScope also provides [NetworkX-compatible APIs](https://graphscope.io/docs/reference/networkx/index.html) to support the creation, manipulation, query and analysis of graphs, offering an easy transition for users with prior experiences in the NetworkX library. The built-in graph analysis algorithms in GraphScope have compatible interfaces with the corresponding NetworkX counterparts, and can be well parallelized with a performance boost of several orders of magnitudes.

## Graph Traversal Support, in Gremlin and Cypher

GraphScope Interactive Engine(GIE) leverages Gremlin, a graph traversal language developed by [Apache TinkerPop](https://tinkerpop.apache.org/), to offer a powerful and intuitive way of querying graphs interactively, and supports automatic query parallelization. GIE implements TinkerPop’s [Gremlin Server](https://tinkerpop.apache.org/docs/current/reference/#gremlin-server) interface such that the system can seamlessly interact with the TinkerPop ecosystem, including development tools such as [Gremlin Console](https://tinkerpop.apache.org/docs/current/tutorials/the-gremlin-console/) and language wrappers such as [Gremlin-Python](https://pypi.org/project/gremlinpython/) and [Gremlin-Java](https://tinkerpop.apache.org/docs/current/reference/#gremlin-java).

[Cypher](https://neo4j.com/developer/cypher/) is a declarative programming language for graph queries. It is designed to make it easy for users to specify the graph patterns they are interested in. We are currently working on adding support for the Cypher language to provide users with more flexibility and options on graph queries. 

## High Performant Built-in Algorithms
TODO: How many, for analytics and learning, high performance, link to performance.

## Extensible Algorithm Library for Graph Analytics 

GraphScope Analytical Engine(GAE) provides a rich set of commonly used algorithms, including connectivity and path analysis, community detection and centrality computations. This [directory](https://github.com/alibaba/GraphScope/tree/main/analytical_engine/apps) includes a full list of the built-in algorithms, which is continuously growing.

GAE also provides developers the flexibility to customize their own algorithms in a pure Python mode. Currently, the programing models that GAE supports include the sub-graph based [PIE](https://dl.acm.org/doi/10.1145/3282488) model and the vertex-centric [Pregel](https://dl.acm.org/doi/10.1145/1807167.1807184) model. GAE also supports implementing and running algorithms in Java (only in the PIE model).

TODO(wanglei): mention, support Java, PIE, Pregel models...

## GNN Training & Inference

GraphScope Learning Engine (GLE) offers users an easy-to-use approach to take advantage of high-performance GNN training and inference on large-scale graphs. GLE provides multiple commonly-used (negative)sampling operators to facilitate graph sampling, and allows users to define graph queries (e.g., N-hop sampling queries) in a Gremlin-style Graph Query Language ([GSL](https://graph-learn.readthedocs.io/en/latest/zh_CN/gl/graph/gsl.html)). It also comes with a wide range of GNN models, like GCN, GAT, GraphSAGE, and SEAL, and includes commonly-utilized algorithm modules (e.g., DeepWalk and TransE). Furthermore, GLE provides a set of paradigms and processes to ease the development of customized models. GLE is compatible with [PyG](https://github.com/pyg-team/pytorch_geometric), e.g., this [example](https://github.com/alibaba/graph-learn/tree/66229a6dcf7b45a340a39ff9d0796ec11bf78d79/graphlearn/examples/pytorch/gcn) shows that a PyG model can be trained using GLE with very minor modifications. Users can flexibly choose [TensorFlow](https://github.com/tensorflow/tensorflow) or [PyTorch](https://github.com/pytorch/pytorch) as the training backend.

To support online inference on dynamic graphs, we propose Dynamic Graph Service ([DGS](https://graph-learn.readthedocs.io/en/latest/en/dgs/intro.html)) in GLE to facilitate real-time sampling on dynamic graphs. The sampled subgraph can be fed into the serving modules (e.g., [TensorFlow Serving](https://github.com/tensorflow/serving)) to obtain the inference results. This [document](https://graph-learn.readthedocs.io/en/latest/en/dgs/tutorial.html#prepare-data) is organized to provide a detailed, step-by-step tutorial specifically demonstrating the use of GLE for offline training and online inference.


## Cloud Native Design
support k8s, TBF. 

## Across-Engine Workflow Orchestration

GraphScope allows users to orchestrate complex graph computation workflows that combine tasks of various workloads, such as graph query, graph analytics and graph learning. With Vineyard that offers efficient in-memory data management, graph computation tasks backed up by different engines (GAE, GIE and GLE) can be seamlessly orchestrated into customized workflows. Users only need to concentrate on the workflow logics, complex operations such as storing and transferring intermediate results, computation parallelization and workflow pipelining in a distributed cluster are automatically handled by GraphScope. This [tutorial](https://nbviewer.org/github/alibaba/GraphScope/blob/main/tutorials/01_node_classification_on_citation.ipynb) demonstrates an example of across-engine workflow orchestration in GraphScope.
