# GraphScope Analytical Engine - GRAPE

[![Translation](https://shields.io/badge/README-%E4%B8%AD%E6%96%87-blue)](README-zh.md)

The analytical engine in GraphScope originated from **GRAPE**, a system that implemented the fix-point model proposed in the paper [Parallelizing Sequential Graph Computations](https://dl.acm.org/doi/10.1145/3282488). 

GRAPE differs from prior systems in its ability to parallelize sequential graph algorithms as a whole by following the PIE programming model from the paper. Sequential algorithms can be easily ["plugged into"](https://github.com/alibaba/libgrape-lite/blob/master/examples/analytical_apps/sssp/sssp_auto.h) **GRAPE** with only minor changes and get parallelized to handle large graphs efficiently. In addition to the ease of programming, **GRAPE** is designed to be [highly efficient](https://github.com/alibaba/libgrape-lite/blob/master/Performance.md) and [flexible](https://github.com/alibaba/libgrape-lite/blob/master/examples/gnn_sampler), to cope the scale, variety and complexity from real-life graph applications.

A lightweight version of GRAPE is open-sourced as [libgrape-lite](https://github.com/alibaba/libgrape-lite/). The analytical engine extends libgrape-lite with features for mutable fragments, [vineyard](https://github.com/alibaba/libvineyard/) support, and the service mode for the engine, etc.

## Java PIE SDK

Apart from the PIE programming interfaces exposed in Python, GraphScope also
Provides an efficient **Java SDK** for users to write Graph analytical algorithms in Java . See [GRAPE-JDK](java/) for details.

## Publications

- Wenfei Fan, Jingbo Xu, Wenyuan Yu, Jingren Zhou, Xiaojian Luo, Ping Lu, Qiang Yin, Yang Cao, and Ruiqi Xu. [Parallelizing Sequential Graph Computations](https://dl.acm.org/doi/10.1145/3282488). ACM Transactions on Database Systems (TODS) 43(4): 18:1-18:39.

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang. [GRAPE: Parallelizing Sequential Graph Computations](http://www.vldb.org/pvldb/vol10/p1889-fan.pdf). The 43rd International Conference on Very Large Data Bases (VLDB), demo, 2017 (the Best Demo Award).

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang, Zeyu Zheng, Bohan Zhang, Yang Cao, and Chao Tian. [Parallelizing Sequential Graph Computations](https://dl.acm.org/doi/10.1145/3035918.3035942). ACM SIG Conference on Management of Data (SIGMOD), 2017 (the Best Paper Award).
