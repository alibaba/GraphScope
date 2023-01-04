# GraphScope 交互查询引擎

[![Translation](https://shields.io/badge/README-English-blue)](https://github.com/alibaba/GraphScope/tree/main/interactive_engine)

GraphScope的交互查询引擎（简称GIE）是一个分布式系统，它为不同经验的用户提供了一个易用的交互式环境，支持海量复杂图数据上的*实时分析与交互探索*。该引擎支持[Gremlin](http://tinkerpop.apache.org/)语言表达的交互图查询，并提供了自动化和用户透明的分布式并行执行。


## 可（分布式）扩展的Gremlin查询

[Gremlin](http://tinkerpop.apache.org/)编程模型被包括[Neo4j](https://neo4j.com/)、[OrientDB](https://www.orientdb.org/)、[JanusGraph](https://janusgraph.org/)、[Microsoft Cosmos DB](https://azure.microsoft.com/en-us/services/cosmos-db/)和[Amazon Neptune](https://aws.amazon.com/neptune/)在内的主流图数据库或系统厂商广泛采用，GIE以忠实保留Gremlin为设计目标，从而让已有的Gremlin应用只需最小化的修改就可以扩展到大规模计算集群。

您可以通过查阅[完整的文档](https://graphscope.io/docs/interactive_engine.html)了解当前实现的更多信息。


## 高性能图遍历

GIE通过业界领先（首创）的并行化编译技术和定制的分布式数据流计算引擎，在大规模集群上实现了高性能、低延时的复杂Gremlin图遍历。

下图利用公认的[LDBC Social Network Benchmark](http://ldbcouncil.org/benchmarks/snb)（交互查询负载），比较了GIE（图中标注为GraphScope）和流行的JanusGraph分布式图数据库的性能差异：

<div align="center">
    <img src="benchmark/figures/summary.jpg" width="500" alt="summary-perf">
</div>

我们选取了5个具有代表性的典型查询（包含1个小查询和4个大查询）并在图中展示了查询延时的比较：**GraphScope相比JanusGraph快13~147倍，且可以在集群中近乎线性的扩展大查询的性能**。

[性能报告](benchmark/README-zh.md)提供了更完整的报告和解释说明。


## 快速入门

[快速入门指南](https://graphscope.io/docs/interactive_engine.html#connecting-gremlin-within-python)包含了快速搭建GraphScope集群、载入图和提交Gremlin查询的步骤。


## <a name="background_roadmap"></a>项目背景与规划蓝图

从大约2年前（即2018年）开始，从阿里巴巴内部的数据科学家和分析师那里，我们陆续听到了越来越多的根据业务场景、从海量图数据中*探索*多样化*模式*的需求，一个例子是二部图中的团模式[2]。这些场景往往需要领域专家（非技术用户）用Java或C++等编程语言、为每个特定问题去实现特定的分布式算法。

<div align="center">
    <img src="../docs/images/cycle_detection.png" width="400" alt="An example graph model for fraud detection.">
</div>

作为一个例子，上图显示了金融风控真实应用的一个简化场景[3]，它用于检测信用卡（或虚拟电子信用）的非法套现。"犯罪分子"利用一个虚假身份从银行（顶点4）获取了一个（短期）信用卡。随后他/她就试图在一个（同伙）商家（顶点3）的协助下，通过一笔发生在时间t1的伪造交易（边2-->3）实现非法套现。一旦商家从银行得到真实的支付（边4-->3），它就通过一系列中间人账户（顶点1）将钱款转回给"犯罪分子"：图中展示了发生在t3和t4的两笔转账（边3-->1和边1-->2）。这一模式最终构成了一个资金流转闭环（2-->3-->1...-->2）。

现实世界中，这样的图模型可能包含几十亿的点（如账户）和成千上万亿的边（如支付、转账），同时整个欺诈犯罪过程可能涉及很多账户之前包含各种约束条件的复杂交易链路。因此需要风控人员复杂深入的实时交互去识别和发现这些模式。

我们由此开始了GraphScope交互查询引擎项目，系统为这一类新的大图交互式应用提供一个全新的分布式系统基础架构。**当前发布的是我们第一个版本 -- GAIA 技术预览版**，它已经上线阿里巴巴内部生产一年多，被成功应用于几个大型应用和几十个小型应用。

我们从用户那儿得到了很好的反馈。尽管如此，随着它被用于更多场景，我们对这一类场景和应用需求的理解和经验也在快速积累，也发现了一系列需要改进的地方。例如：

* *内存管理*：图遍历的中间结果会包含任意长度的路径，这些路径会随着遍历的深度呈指数增长，由此造成内部溢出。例如，假设平均度数100（这个规模在阿里巴巴内部生产很常见），从单个点出发的5跳遍历就会产生100亿的路径。

* *早停优化*：在有动态条件的情况下，遍历所有的完整路径往往是不必要的。例如下面的查询只需要前k个结果。对于真实大规模数据上的复杂遍历查询，这些条件可能被隐藏在任意的嵌套查询中，从而造成浪费的计算和严重影响性能。对于一个串行的实现，要避免这样的浪费还相对容易。但对于分布式并行的执行，这个问题的解决需要考虑浪费的计算和并行度之间的权衡，非常挑战。

    ```java
    g.V(2).repeat(out().simplePath())
     .times(4).path()
     .limit(k)
    ```

我们已经在真实生产环境解决了上述问题，详见论文[1]。这些改进目前还没有包含在 GAIA 技术预览发布版中，但我们正在努力完成这部分代码的开源，计划在2021年上半年提供一个更新。

我们也非常欢迎更多的社区建议，包括问题、功能需求等等。最好的给我们提供反馈的方式是在这个代码库创建一个issue，谢谢！


## 论文

1. Zhengping Qian, Chenqiang Min, Longbin Lai, Yong Fang, Gaofeng Li, Youyang Yao, Bingqing Lyu, Zhimin Chen, Jingren Zhou.  GraphScope: A System for Interactive Analysis on Distributed Graphs Using a High-Level Language.  本文被NSDI ’21接受，最终版本将于2021年3月2日发布。

2. Bingqing Lyu, Lu Qin, Xuemin Lin, Ying Zhang, Zhengping Qian, and Jingren Zhou.  Maximum biclique search at billion scale.  本文获VLDB 2020最佳论文奖亚军。（[pdf](http://www.vldb.org/pvldb/vol13/p1359-lyu.pdf)）

3. Xiafei Qiu, Wubin Cen, Zhengping Qian, You Peng, Ying Zhang, Xuemin Lin, and Jingren Zhou.  Real-time constrained cycle detection in large dynamic graphs.  本文发表于VLDB 2018。（[pdf](http://www.vldb.org/pvldb/vol11/p1876-qiu.pdf)）
