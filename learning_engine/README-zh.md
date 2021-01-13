# GraphScope 图学习引擎: Graph-Learn

[![Translation](https://shields.io/badge/README-English-blue)](https://github.com/alibaba/GraphScope/tree/main/learning_engine)

GraphScope 图学习引擎即为 GraphLearn，在其之上做了一些适配和封装。

[Graph-Learn(GL，原AliGraph)](https://github.com/alibaba/graph-learn) 是面向大规模图神经网络的研发和应用而设计的一款分布式框架， 它从实际问题出发，提炼和抽象了一套适合于当下图神经网络模型的编程范式， 并已经成功应用在阿里巴巴内部的诸如搜索推荐、网络安全、知识图谱等众多场景。

GL注重可移植和可扩展，对于开发者更为友好，为了应对GNN在工业场景中的多样性和快速发展的需求。 基于GL，开发者可以实现一种GNN算法，或者面向实际场景定制化一种图算子，例如图采样。 GL的接口以Python和NumPy的形式提供，可与TensorFlow或PyTorch兼容但不耦合。 目前GL内置了一些结合TensorFlow开发的经典模型，供用户参考。 GL可运行于Docker内或物理机上，支持单机和分布式两种部署模式。