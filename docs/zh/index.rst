.. graphscope documentation master file, created by
   sphinx-quickstart on Tue Aug 27 10:19:05 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GraphScope: 一站式图计算系统
=========================

GraphScope 是阿里巴巴达摩院智能计算实验室研发并开源的一站式图计算平台。
依托于阿里海量数据和丰富场景，与达摩院的高水平研究，
GraphScope 致力于针对实际生产场景中图计算的挑战，提供一站式高效的解决方案。

GraphScope 提供了 Python 客户端，
能十分方便的对接上下游工作流，具有一站式、开发便捷、性能极致等特点。
它整合了智能计算实验室多个重要的创新性技术，包括 GRAPE、MaxGraph、Graph-Learn，
支持了图分析、图的交互式查询和图学习。
其中核心优势包括在业界首次支持了 Gremlin 分布式编译优化，
支持了算法的自动并行化、提供了企业级场景下的极致性能等。
在阿里巴巴内外部应用中，GraphScope 已经证明在多个关键互联网领域
（如风控，电商推荐，广告，网络安全，知识图谱等）实现了重要的业务新价值。

GraphScope 整合了达摩院的多项学术研究成果，
其中的核心技术曾获得数据库领域顶级学术会议 SIGMOD2017 最佳论文奖、
VLDB2017 最佳演示奖、VLDB2020 最佳论文奖亚军、世界人工智能创新大赛SAIL奖等。
GraphScope 的交互查询引擎的论文已被 NSDI 2021录用。
还有其它围绕 GraphScope 的十多项研究成果发表在领域顶级的学术会议或期刊上，
如 TODS、SIGMOD、VLDB、KDD等。

.. toctree::
   :maxdepth: 2
   :caption: 目录

   installation
   getting_started
   deployment
   tutorials
   loading_graph
   graph_transformation
   analytics_engine
   interactive_engine
   learning_engine
   frequently_asked_questions
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
