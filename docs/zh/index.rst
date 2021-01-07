.. graphscope documentation master file, created by
   sphinx-quickstart on Tue Aug 27 10:19:05 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GraphScope: 一站式图计算系统
=========================

GraphScope 是阿里巴巴达摩院智能计算实验室研发并开源的一站式图计算平台。
依托于阿里海量数据和丰富场景，与达摩院的高水平研究，GraphScope致力于针对
实际生产中图计算的上述挑战，提供一站式高效的解决方案。

GraphScope 提供Python客户端，能十分方便的对接上下游工作流，具有一站式、
开发便捷、性能极致等特点。它具有高效的跨引擎内存管理，在业界首次支持Gremlin
分布式编译优化，同时支持算法的自动并行化和支持自动增量化处理动态图更新，
提供了企业级场景的极致性能。在阿里巴巴内部和外部的应用中，GraphScope已经
证明在多个关键互联网领域（如风控，电商推荐，广告，网络安全，知识图谱等）
实现重要的业务新价值。

GraphScope集合了达摩院的多项学术研究成果，其中的核心技术曾获得数据库领域
顶级学术会议 SIGMOD2017 最佳论文奖、VLDB2017 最佳演示奖、VLDB2020 
最佳论文提名奖、世界人工智能创新大赛SAIL奖。GraphScope的交互查询引擎的
论文也已被 NSDI 2021录用，即将发表。还有其它围绕 GraphScope 的十多项
研究成果发表在领域顶级的学术会议或期刊上，如TODS、SIGMOD、VLDB、KDD等。

.. toctree::
   :maxdepth: 2
   :caption: 目录

   installation
   getting_started
   deployment
   loading_graph
   interactive_engine
   analytics_engine
   learning_engine
   developer_guide

.. toctree::
   :maxdepth: 2
   :caption: API 参考

   Python API 参考 <https://graphscope.io/docs/reference/python_index.html>
   图分析引擎 API 参考 <https://graphscope.io/docs/reference/analytical_engine_index.html>

索引
====

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
