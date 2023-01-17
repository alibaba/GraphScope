# Tutorials for Java Users

Java plays an important role in big data ecosystem, and a lot of graph processing systems developed by Java (e.g., Apache Giraph and GraphX) have been widely applied. To integrate with Java ecosystem, GraphScope allows graph analytics algorithms developed for Apache Giraph and GraphX to directly run over GraphScope GAE.

In addition, GraphScope GAE supports the PIE programming model, which can provide better performance than widely-used vertex-centric model. GraphScope GAE is equipped with `Java PIE` interface exposed by `GRAPE-jdk`, and users can develop their own algorithms using the PIE programming model.

The following tutorials will introduce how to leverage the above features in GraphScope GAE Jave SDK.


- [Querying graph via GraphScope JavaSDK](./java_tutorial_0_pie.md)
- [Graphscope integration with Apache Giraph](./java_tutorial_1_giraph.md)
- [GraphScope integration with Apache GraphX](./java_tutorial_2_graphx.md)