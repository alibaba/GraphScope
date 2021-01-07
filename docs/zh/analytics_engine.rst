分析引擎
============================

GraphScop的分析引擎源自 `GRAPE <https://dl.acm.org/doi/10.1145/3282488>`_ ，
一个发表于SIGMOD-2017的图处理系统。
GRAPE与以前的系统的不同之处在于，它可以使整个串行图算法整体并行化。
在GRAPE中，只需进行很小的更改即可轻松地将串行算法“插入”系统中，并使其并行化以高效地处理大规模图数据。
除了易于编程外，GRAPE还被设计为高效且灵活的系统，可灵活应对现实中图应用的规模，多样性和复杂性。

内置算法
---------------------

GraphScope分析引擎提供了许多常用的图分析算法，包括连通性分析算法和路径分析算法，
社区检测，集中度计算等。

内置算法可以在已加载的图数据上轻松调用。例如，

.. code:: ipython

    from graphscope import pagerank
    from graphscope import lpa

    # algorithms defined on property graph can be invoked directly.
    # 定义在属性图上的算法可以直接调用。
    result = lpa(g)

    # 其他一些算法可能只支持在简单图上进行计算，因此我们需要先通过顶点和边的类型来生成一个简单图。
    simple_g = g.project_to_simple(v_label="users", e_label="follows")
    result_pr = pagerank(simple_g)

内置算法的完整列表如下所示。算法是否支持属性图也在其文档进行了描述。

.. currentmodule:: graphscope

.. autosummary::

- :func:`bfs`
- :func:`cdlp`
- :func:`clustering`
- :func:`degree_centrality`
- :func:`eigenvector_centrality`
- :func:`hits`
- :func:`k_core`
- :func:`katz_centrality`
- :func:`lpa`
- :func:`pagerank`
- :func:`sssp`
- :func:`triangles`
- :func:`wcc`

算法支持列表会持续更新。

计算结果的处理
---------------------
当完成一次图计算，计算结果会被包成 :ref:`Context` 类，保存在分布式集群中。

用户可能希望将结果获取到客户端，写入云端或分布式文件系统。GraphScope支持用户通过以下方法来获取数据。

.. code:: iPython

    # 转化为相应数据类型
    result_pr.to_numpy()
    result_pr.to_dataframe()

    # 或写入 hdfs、oss， 或本地目录中（pod中的本地目录）
    result_pr.output("hdfs://output")
    result_pr.output("oss://id:key@endpoint/bucket/object")
    result_pr.output("file:///tmp/path")

    # 或写入本地的client中
    result_pr.output_to_client("local_filename")

    # 或 seal to vineyard
    result_pr.to_vineyard_dataframe()
    result_pr.to_vineyard_numpy()

此外，如 :ref: `快速上手` 中所示，用户可以将计算结果可以加回到该图数据中作为顶点（边）的新属性（列）。

.. code:: iPython

    simple_g = sub_graph.project_to_simple(vlabel="paper", elabel="cites")
    ret = graphscope.kcore(simple_g, k=5)

    # 将结果作为新列添加到citation图中
    subgraph = sub_graph.add_column(ret, {'kcore': 'r'})

用户可以分配一个 :ref:`Selector` 来定义将计算结果中的哪些部分写回图数据。
选择器指定了计算结果中的哪一部分会被保留。同样地，图数据也可以作为结果的一部分，例如顶点ID。
我们为选择器保留了三个关键字。`r` 代表结果，`v` 和 `e` 分别代表顶点和边。
以下是结果处理中选择器的一些示例。

.. code:: iPython

    # 获取顶点上的结果
    result_pr.to_numpy('r')

    # 转换为 dataframe,
    # 使用顶点的 `id` 作为名为 df_v 的列
    # 使用顶点的 `data` 作为名为 df_vd 的列
    # 使用结果列作为名为 df_result 的列
    result_pr.to_dataframe({'df_v': 'v.id', 'df_vd': 'v.data', 'df_result': 'r'})

    # using the property0 written on vertices with label0 as column `result`
    # 对于属性图的结果
    # 使用 `:` 作为v和e的标签选择器
    # 将 label0 顶点的 id (`v:label0.id`)作为 `id` 列
    # 使用写在带有label0的顶点上的property0作为`result`列
    result.output(fd='hdfs:///gs_data/output', \
            selector={'id': 'v:label0.id', 'result': 'r:label0.property0'})

可以在 :ref:`Context` 和 :ref:`Selector` 中获取更多信息。


使用PIE编程模型自定义算法
----------------------------------------------

如果内置算法无法满足需求，用户可以编写自己的算法。用户可以通过 `graphscope` 在纯python模式
下使用 `PIE <https://dl.acm.org/doi/10.1145/3282488>k_ 编程模型编写算法。

.. image:: images/pie.png
    :width: 600
    :align: center
    :alt: Workflow of PIE


为了实现自己的算法，用户只需要实现此类。

.. code:: ipython

    @graphscope.analytical.udf.pie
    class YourAlgorithm(AppAssets):
        @staticmethod
        def Initialize(context, frag):
            pass

        @staticmethod
        def PEval(context, frag):
            pass

        @staticmethod
        def IncEval(context, frag):
            pass

如代码所示，用户需要实现一个以 `@graphscope.analytical.udf.pie` 装饰的类，并提供三个串行
图函数。其中，`Initialize` 函数用于设置算法初始状态， `PEval` 函数负责算法的部分计算，
`IncEval` 函数负责对分区数据进行增量计算。与fragment相关的完整API可参考 :ref:`Cython SDK API`。

以SSSP为例，用户在PIE模型中定义的SSSP算法可如下所示。

.. code:: ipython

    @graphscope.analytical.udf.pie
    class SSSP:
        @staticmethod
        def Initialize(context, frag):
            v_label_num = frag.vertex_label_num()
            # 初始化每个顶点的距离
            for v_label_id in range(v_label_num):
                nodes = frag.nodes(v_label_id)
                context.init_value(nodes, v_label_id, 1000000000.0,
                               PIEAggregateType.kMinAggregate)
            context.register_sync_buffer(MessageStrategy.kSyncOnOuterVertex)

        @staticmethod
        def PEval(context, frag):
            # 从context中获取源顶点
            src = int(context.get_config(b'src'))
            graphscope.declare(graphscope.Vertex, source)
            native_source = False
            v_label_num = frag.vertex_label_num()
            for v_label_id in range(v_label_num):
                if frag.get_inner_node(v_label_id, src, source):
                    native_source = True
                    break
            if native_source:
                context.set_node_value(source, 0)
            else:
                return

            # 在源顶点所在分区中，运行dijkstra算法作为部分计算
            e_label_num = frag.edge_label_num()
            for e_label_id in range(e_label_num):
                edges = frag.get_outgoing_edges(source, e_label_id)
                for e in edges:
                    dst = e.neighbor()
                    distv = e.get_int(2)
                    if context.get_node_value(dst) > distv:
                        context.set_node_value(dst, distv)

        @staticmethod
        def IncEval(context, frag):
            v_label_num = frag.vertex_label_num()
            e_label_num = frag.edge_label_num()
            # 增量计算，更新最短距离
            for v_label_id in range(v_label_num):
                iv = frag.inner_nodes(v_label_id)
                for v in iv:
                    v_dist = context.get_node_value(v)
                    for e_label_id in range(e_label_num):
                        es = frag.get_outgoing_edges(v, e_label_id)
                        for e in es:
                            u = e.neighbor()
                            u_dist = v_dist + e.get_int(2)
                            if context.get_node_value(u) > u_dist:
                                context.set_node_value(u, u_dist)

如代码所示，用户仅需要设计和实现单分区的串行算法，而不需要考虑分布式部署中的分区通信和消息传递
在这种情况下，经典的dijxkstra算法及其增量版本就可以用于在集群上已分区的大规模图数据计算。


使用Pregel编程模型自定义算法
----------------------------------------------

除了基于子图的PIE模型之外，`graphscope` 也支持以顶点为中心的 `Pregel` 模型。
您可以通过实现以下算法类来在 `Pregel` 模型中开发算法。

.. code:: ipython

    @pregel(vd_type='double', md_type='double')
    class YourPregelAlgorithm(AppAssets):

        @staticmethod
        def Init(v, context):
            pass

        @staticmethod
        def Compute(messages, v, context):
            pass

        @staticmethod
        def Combine(messages):
            pass

与PIE模型不同，Pregel算法类的装饰器为 `@graphscope.analytical.udf.pregel` ，并且类方法是
定义在顶点上的，而不是PIE模型的分区上。
以SSSP为例，Pregel模型中的算法如下所示。

.. code:: ipython

    # 装饰器, 定义顶点数据和消息数据的类型
    @pregel(vd_type='double', md_type='double')
    class SSSP_Pregel(AppAssets):

        @staticmethod
        def Init(v, context):
            v.set_value(1000000000.0)

        @staticmethod
        def Compute(messages, v, context):
            src_id = context.get_config(b"src")
            cur_dist = v.value()
            new_dist = 1000000000.0
            if v.id() == src_id:
                new_dist = 0
            for message in messages:
                new_dist = min(message, new_dist)
            if new_dist < cur_dist:
                v.set_value(new_dist)
                for e_label_id in range(context.edge_label_num()):
                    edges = v.outgoing_edges(e_label_id)
                    for e in edges:
                        v.send(e.vertex(), new_dist + e.get_int(2))
            v.vote_to_halt()

        @staticmethod
        def Combine(messages):
            ret = 1000000000.0
            for m in messages:
                ret = min(ret, m)
            return ret


运行自定义算法
-------------------------

运行自定义算法，用户需要在定义算法的位置触发算法。

.. code:: ipython

    import graphscope

    sess = graphscope.session()
    g = sess.load_from("...")

    # 加载自己的算法
    my_app = SSSP_Pregel()

    # 在图上运行自己的算法，得到计算结果
    ret = my_app(g, source="0")

在开发和测试之后，您可以通过 `to_gar` 方法将算法保存成gar包以备将来使用。

.. code:: ipython

    SSSP_Pregel.to_gar("file:///var/graphscope/udf/my_sssp_pregel.gar")

在此之后，您可以从gar包加载自定义的算法。

.. code:: ipython

    import graphscope

    sess = graphscope.session()
    g = sess.load_from("...")

    # 从gar包中加载自己的算法
    my_app = load_app('SSSP_Pregel', 'file:///var/graphscope/udf/my_sssp_pregel.gar')

    # 在图上运行自己的算法，得到计算结果
    ret = my_app(g, src="0")


**Publications**

- Wenfei Fan, Jingbo Xu, Wenyuan Yu, Jingren Zhou, Xiaojian Luo, Ping Lu, Qiang Yin, Yang Cao, and Ruiqi Xu. `Parallelizing Sequential Graph Computations. <https://dl.acm.org/doi/10.1145/3282488>`_, ACM Transactions on Database Systems (TODS) 43(4): 18:1-18:39.

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang. `GRAPE: Parallelizing Sequential Graph Computations. <http://www.vldb.org/pvldb/vol10/p1889-fan.pdf>`_, The 43rd International Conference on Very Large Data Bases (VLDB), demo, 2017 (the Best Demo Award).

- Wenfei Fan, Jingbo Xu, Yinghui Wu, Wenyuan Yu, Jiaxin Jiang, Zeyu Zheng, Bohan Zhang, Yang Cao, and Chao Tian. `Parallelizing Sequential Graph Computations. <https://dl.acm.org/doi/10.1145/3035918.3035942>`_, ACM SIG Conference on Management of Data (SIGMOD), 2017 (the Best Paper Award).