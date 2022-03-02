.. _loading_graphs:

载图
====
GraphScope 以 
`属性图 <https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model>`_ 建模图数据。
图上的点/边都带有标签（label），每个标签都可能带有许多属性（property）。

载入内置数据集
----------------------

GraphScope 内置了一组流行的数据集，以及载入他们的工具函数，帮助用户更容易的上手。

来看一个例子：

.. code:: python

    from graphscope.dataset import load_ogbn_mag
    graph = load_ogbn_mag()

在单机模式下，GraphScope 会将数据文件下载到 :file:`~/.graphscope/dataset`，并且会保留以供将来使用。

在 Kubernetes 集群模式下，将数据文件下载到 Pod 的本地存储比较复杂，所以我们提供了一个挂载了 OSS 的数据集桶的一个容器，

在下面的例子里，我们将载入和上面同样的一张图，只不过这次 graphscope 部署在集群中。

.. code:: python

    import graphscope
    from graphscope.dataset import load_ogbn_mag
    sess = graphscope.session(cluster_type='k8s', mount_dataset='/dataset')
    graph = load_ogbn_mag(sess, '/dataset/ogbn_mag_small')

这里，我们首先创建一个会话，然后将数据集桶挂载到 :file:`/dataset`，此路径相对于 Pod 的本地路径。然后我们将会话作为参数传入，路径 :file:`/dataset/ogbn_mag_small` 作为第二个参数。 :file:`/dataset` 是我们通过 `mount_dataset` 的参数指定的挂载路径， `ogbn_mag_small` 是这个数据集所在的文件夹的名字。

你可以在 `<https://github.com/alibaba/GraphScope/tree/main/python/graphscope/dataset>`_ 找到所有目前支持的数据集，文件中包括详细的介绍和用法。


手动配置图
---------------------

然而，更常见的情况是用户需要使用自己的数据集，并做一些数据分析的工作。

我们提供了一个函数用来定义一个属性图的模型（schema），并以将属性图载入 GraphScope：

首先，我们创建一个会话，然后在此会话内创建图。

.. code:: python

    import graphscope
    sess = graphscope.session()
    # Use `sess = graphscope.session(cluster_type='hosts')` if you are in standalone mode.
    graph = sess.g()

`Graph` 有几个方法来配置：

.. code:: python

    def add_vertices(self, vertices, label="_", properties=None, vid_field=0):
        pass

    def add_edges(self, edges, label="_e", properties=None, src_label=None, dst_label=None, src_field=0, dst_field=1):
        pass

这些方法可以增量的构建一个属性图。

我们将使用 :file:`ldbc_sample` 里的文件做完此篇教程的示例。你可以在 `这里 <https://github.com/GraphScope/gstest/tree/master/ldbc_sample>`_ 找到源数据。你可以随时使用 `print(graph.schema)` 来查看图的模型.

Build Vertex
^^^^^^^^^^^^

我们可以向图内添加一个点标签。相关的参数含义如下：

vertices
++++++++

:ref:`Loader Object`，代表数据源，指示 `graphscope` 可以在哪里找到源数据，可以为文件路径，或者 numpy 数组等；

一个简单的例子：

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv')

这将会从文件 :file:`/home/ldbc_sample/person_0_0.csv` 载入数据，并且创建一个名为 `_` 的边，但是有不同的起始点标签和终点标签。

Label
+++++

点标签的名字，默认为 `_`.

一张图中不能含有同名的标签，所以若有两个或以上的标签，用户必须指定标签名字。另外，总是给标签一个有意义的名字也有好处。

可以为任何标识符 (identifier)。

举个例子：

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')

结果与上一步结果除了标签名完全一致。

properties
++++++++++

一组属性名字。可选项，默认为 `None`。

属性名应当与数据中的首行表头中的名字相一致。

如果省略或为 `None`，除ID列之外的所有列都将会作为属性载入；如果为空列表 `[]`，那么将不会载入任何属性；其他情况下，只会载入指定了的列作为属性。

比如说：

.. code:: python

    # properties will be firstName,lastName,gender,birthday,creationDate,locationIP,browserUsed
    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person', properties=None)

    # properties will be firstName, lastName
    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person', properties=['firstName', 'lastName'])

    # no properties
    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person', properties=[])


vid_field
+++++++++

作为 ID 列的列名，默认为 0。此列将在载入边时被用做起始点 ID 或目标点 ID。

其值可以是一个字符串，此时指代列名；

或者可以是一个正整数，代表第几列 （从0开始）。

默认为第0列。

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', vid_field='firstName')

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', vid_field=0)


Build Edge
^^^^^^^^^^

现在我们可以向图中添加一个边标签。

edges
++++++

与构建点标签一节中的 `vertices` 类似，为指示去哪里读数据的路径。

让我们来看一个例子：

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    # Note we already added a vertex label named 'person'.
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', src_label='person', dst_label='person')

这将会载入一个标签名为 `_e` 的边，源节点标签和终点节点标签都为 `person`，第一列作为起点的点ID，第二列作为终点的点ID。其他列都作为属性。

label
+++++

边的标签名，默认为 `_e`。推荐总是使用一个有意义的标签名。

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', label='knows', src_label='person', dst_label='person')


properties
++++++++++

一列属性，默认为 `None`。 意义与行为都和点中的一致。

src_label and dst_label
++++++++++++++++++++++++++++++++++

起点的标签名与终点的标签名。我们在上面的例子中已经看到过了，在那里将其赋值为 `person`。这两者可以取不同的值。举例来说：

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    graph = graph.add_vertices('/home/ldbc_sample/comment_0_0.csv', label='comment')
    # Note we already added a vertex label named 'person'.
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment')


src_field and dst_field
++++++++++++++++++++++++++++++++++

起点的 ID 列名与终点的 ID 列名。 默认分别为 0 和 1。

意义和表现与点中的 `vid_field` 类似，不同的是需要两列，一列为起点 ID， 一列为终点 ID。 以下是个例子：

.. code:: python

    # Steps to init a graph and add vertices are omitted
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment', src_field='Person.id', dst_field='Comment.id')
    # Or use the index.
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment', src_field=0, dst_field=1)


高级用法
------------

这是一些用来处理特别简单或特别复杂的高级一些的用法。

没有歧义时，自动推断点标签
^^^^^^^^^^^^^^^^^^^^^^^^

如果图中只存在一个点标签，那么可以省略指定点标签。
GraphScope 将会推断起始点标签和终点标签为这一个点标签。


.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    # GraphScope will assign `src_label` and `dst_label` to `person` automatically.
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv')


从边中推断点
^^^^^^^^^^^^^

如果用户的 `add_edges` 中 `src_label` 或者 `dst_label` 取值为图中不存在的点标签，graphscope 会从边的端点中聚合出点表。

.. code:: python

    graph = sess.g()
    # Deduce vertex label `person` from the source and destination endpoints of edges.
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', src_label='person', dst_label='person')

    graph = sess.g()
    # Deduce the vertex label `person` from the source endpoint,
    # and vertex label `comment` from the destination endpoint of edges.
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment')


多种边关系
^^^^^^^^^^^

在一些情况下，一种边的标签可能连接了两种及以上的点。例如，在下面的属性图中，有一个名为 `likes` 的边标签，
连接了两种点标签，i.e., `person` -> `likes` <- `comment` and `person` -> `likes` <- `post`。
在这种情况下，可以添加两次名为 `likes` 的边，但是有不同的起始点标签和终点标签。


.. code:: python

    # Steps to init a graph and add vertices are omitted
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv',
            label="likes",
            src_label="person", dst_label="comment",
        )

    graph = graph.add_edges('/home/ldbc_sample/person_likes_post_0_0.csv',
            label="likes",
            src_label="person", dst_label="post",
        )


.. note:

   1. 这个功能目前只在 `lazy` 会话中支持。
   2. 对于同一个标签的多个定义，其属性列表的数量和类型应该一致，最好名字也一致，
      因为同一个标签的所有定义的数据都将会被放入同一张表，属性名将会使用第一个定义中指定的名字。


指定属性的数据类型
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GraphScope 可以从输入文件中推断点的类型，大部分情况下工作的很好。

然而，用户有时需要更多的自定义能力。为了满足此种需求，可以在属性名之后加入一个额外类型的参数。像这样：

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/post_0_0.csv', label='post', properties=['content', ('length', 'int'), ])

这将会将属性的类型转换为指定的类型，注意属性名字和类型需要在同一个元组中。

在这里，属性 `length` 的类型将会是 `int`，而默认不指定的话为 `int64_t`。 常见的使用场景是指定 `int`, `int64`, `float`, `double`, 'str` 等类型。


图的其他参数
^^^^^^^^^^^^^^^^^^^^^^^^^

类 `Graph` 有三个配置元信息的参数，分别为：

- `oid_type`, 可以为 `int64_t` 或 `string`。 默认为 `int64_t`，会有更快的速度，和使用更少的内存。当ID不能用 `int64_t` 表示时，才应该使用 `string`。
- `directed`, bool, 默认为`True`. 指示载入无向图还是有向图。
- `generate_eid`, bool, 默认为 `True`. 指示是否为每条边分配一个全局唯一的ID。


完整的示例
^^^^^^^^^^^^^^^

让我们写一个完整的图的定义。

.. code:: python

    graph = sess.g(oid_type='int64_t', directed=True, generate_eid=True)
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    graph = graph.add_vertices('/home/ldbc_sample/comment_0_0.csv', label='comment')
    graph = graph.add_vertices('/home/ldbc_sample/post_0_0.csv', label='post')
    
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', label='knows', src_label='person', dst_label='person')
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment')
    graph = graph.add_edges('/home/ldbc_sample/person_likes_post_0_0.csv', label='likes', src_label='person', dst_label='post')

    print(graph.schema)

这里是一个更复杂的载入 LDBC-SNB 属性图的 `例子 <https://github.com/alibaba/GraphScope/blob/main/python/graphscope/dataset/ldbc.py>`_ 。

从 Pandas 或 Numpy 中载图
---------------------------

上文提到的数据源是一个 :ref:`Loader Object` 的类。`Loader` 包含文件路径或者数据本身。
`graphscope` 支持从 `pandas.DataFrame` 或 `numpy.ndarray` 中载图，这可以使用户仅通过 Python 控制台便可以创建图。

除了 Loader 外，其他属性，ID列，标签设置等都和之前提到的保持一致。

从 Pandas 中载图
^^^^^^^^^^^^^^^^^

.. code:: python

    import pandas as pd

    df_v = pd.read_csv('/home/ldbc_sample/comment_0_0.csv', sep='|')
    df_e = pd.read_csv('/home/ldbc_sample/comment_replyOf_comment_0_0.csv', sep='|')

    # use a dataframe as datasource, properties omitted,
    # for edges, col_0/col_1 will be used as src/dst by default.
    # for vertices, col_0 will be used as vertex_id by default.
    graph = sess.g().add_vertices(df_v).add_edges(df_e)


从 Numpy 中载图
^^^^^^^^^^^^^^^^

注意每个数组都代表一列，我们将其以 COO 矩阵的方式传入。

.. code:: python

    import numpy

    array_v = [df_v[col].values for col in df_v.columns.values]
    array_e = [df_e[col].values for col in df_e.columns.values]

    graph = sess.g().add_vertices(array_v).add_edges(array_e)


Loader 的变种
-----------------

当 `loader` 包含文件路径时，它可能仅包含一个字符串。
文件路径应遵循 URI 标准。当收到包含文件路径的载图请求时， `graphscope` 将会解析 URI，调用相应的载图模块。

目前, `graphscope` 支持多种数据源：本地, OSS，S3，和 HDFS:
数据由 `Vineyard <https://github.com/v6d-io/v6d>`_ 负责载入，`Vineyard` 使用 `fsspec <https://github.com/intake/filesystem_spec>`_ 解析不同的数据格式以及参数。任何额外的具体的配置都可以在Loader的可变参数列表中传入，这些参数会直接被传递到对应的存储类中。比如 `host` 和 `port` 之于 `HDFS`，或者是 `access-id`, `secret-access-key` 之于 oss 或 s3。

.. code:: python

    from graphscope.framework.loader import Loader

    ds1 = Loader("file:///var/datafiles/group.e")
    ds2 = Loader("oss://graphscope_bucket/datafiles/group.e", key='access-id', secret='secret-access-key', endpoint='oss-cn-hangzhou.aliyuncs.com')
    ds3 = Loader("hdfs:///datafiles/group.e", host='localhost', port='9000', extra_conf={'conf1': 'value1'})
    d34 = Loader("s3://datafiles/group.e", key='access-id', secret='secret-access-key', client_kwargs={'region_name': 'us-east-1'})

用户可以方便的实现自己的driver来支持更多的数据源，比如参照 `ossfs <https://github.com/v6d-io/v6d/blob/main/modules/io/adaptors/ossfs.py>`_ driver的实现方式。
用户需要继承 `AbstractFileSystem` 类用来做scheme对应的resolver， 以及 `AbstractBufferedFile`。用户仅需要实现 ``_upload_chunk``,
``_initiate_upload`` and ``_fetch_range`` 这几个方法就可以实现基本的read，write功能。最后通过 ``fsspec.register_implementation('protocol_name', 'protocol_file_system')`` 注册自定义的resolver。
