# User Guide and Examples of GAE

In [Getting Started for GAE](https://graphscope.io/docs/latest/getting_started_gae.html), we have introduced how to run built-in algorithms with Python. In many cases, users need to use different programming languages, or customize their own graph analytics algorithms. To this end, GraphScope has provided various mechanisms to satisfy such demands. 

## Developing Your Algorithms with Python

Users may write their own algorithms if the built-in algorithms do not meet their needs. GraphScope enables users to write algorithms in the [PIE programming model](https://dl.acm.org/doi/10.1145/3282488) in a pure Python mode. In addition to the sub-graph based PIE model, GraphScope supports vertex-centric Pregel model as well. Please refer to [how to customize algorithms with Python](https://graphscope.io/docs/latest/how_to_customize_algorithms_with_python.html) for more details.

## Developing Your Algorithms with C++

GraphScope also allows users to develop their algorithms in C++. Specially, GraphScope provides a C++ SDK and users can develop their own algorithms with the PIE programming model. In each customized algorithms, users only need to three functions: the Init is a function to set the initial status; PEval is a sequential method for partial evaluation; and IncEval is a sequential function for incremental evaluation over the partitioned fragment.  The full tutorials can be found in [how to customize algorithms with C++](https://graphscope.io/docs/latest/how_to_customize_algorithms_with_cpp.html).

## Developing Your Algorithms with Java


Java plays an important role in big data ecosystem, and a lot of graph processing systems developed by Java (e.g., Apache Giraph and GraphX) have been widely applied. To integrate with Java ecosystem, GraphScope allows graph analytics algorithms developed for Apache Giraph and GraphX to directly run over GraphScope GAE.

In addition, GraphScope GAE supports the PIE programming model, which can provide better performance than widely-used vertex-centric model. GraphScope GAE is equipped with `Java PIE` interface exposed by `GRAPE-jdk`, and users can develop their own algorithms using the PIE programming model.

The following tutorials will introduce how to leverage the above features in GraphScope GAE Jave SDK.

- [Querying graph via GraphScope JavaSDK](https://graphscope.io/docs/latest/java_tutorial_0_pie.html)
- [Graphscope integration with Apache Giraph](https://graphscope.io/docs/latest/java_tutorial_1_giraph.html)
- [GraphScope integration with Apache GraphX](https://graphscope.io/docs/latest/java_tutorial_2_graphx.html)