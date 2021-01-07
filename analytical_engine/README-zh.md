# GraphScope 图分析引擎 - GRAPE

[![Translation](https://img.shields.io/badge/Translation-English-success)](https://github.com/alibaba/GraphScope/tree/main/analytical_engine)


GraphScope 中的图分析引擎继承自 **GRAPE**，该系统实现了论文 [Parallelizing Sequential Graph Computations](https://dl.acm.org/doi/10.1145/3282488) 中提出的不动点计算模型。

与现有系统不同，GRAPE 通过自动并行化整体的单机顺序图算法，[即插即用](https://github.com/alibaba/libgrape-lite/blob/master/examples/analytical_apps/sssp/sssp_auto.h)已有的图算法程序，使其很容易的运行在分布式环境，高效处理大规模图。除了易于编程之外，**GRAPE** 还被设计为[高效](https://github.com/alibaba/libgrape-lite/blob/master/Performance.md)和[高度可拓展](https://github.com/alibaba/libgrape-lite/blob/master/examples/gnn_sampler)的，以应对现实图应用程序多变的规模，多样性和复杂性。

GRAPE 的核心轻量版本以 [libgrape-lite](https://github.com/alibaba/libgrape-lite/) 开源。GraphScope 中的分析引擎扩展了 libgrape-lite 的功能，支持了可变子图，[vineyard](https://github.com/alibaba/libvineyard/) 支持以及引擎的服务模式等。

## 论文列表

- Wenfei Fan, Jingbo Xu, Wenyuan Yu, Jingren Zhou, Xiaojian Luo, Ping Lu, Qiang Yin, Yang Cao, and Ruiqi Xu. [Parallelizing Sequential Graph Computations](https://dl.acm.org/doi/10.1145/3282488). ACM Transactions on Database Systems (TODS) 43(4): 18:1-18:39.

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang. [GRAPE: Parallelizing Sequential Graph Computations](http://www.vldb.org/pvldb/vol10/p1889-fan.pdf). The 43rd International Conference on Very Large Data Bases (VLDB), demo, 2017 (the Best Demo Award).

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang, Zeyu Zheng, Bohan Zhang, Yang Cao, and Chao Tian. [Parallelizing Sequential Graph Computations](https://dl.acm.org/doi/10.1145/3035918.3035942). ACM SIG Conference on Management of Data (SIGMOD), 2017 (the Best Paper Award).
