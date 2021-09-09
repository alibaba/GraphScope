.. _loading_graphs:

载图
====
GraphScope 以 
`属性图 <https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model>`_ 建模图数据。
图上的点/边都带有标签（label），每个标签都可能带有许多属性（property）。

配置图
-------------------------

我们提供了一个函数用来定义一个属性图的模型（schema），并以将属性图载入 GraphScope：

.. code:: python

    def add_vertices(self, vertices, label="_", properties=[], vid_field=0):
        pass

    def add_edges(self, edges, label="_", properties=[], src_label=None, dst_label=None, src_field=0, dst_field=1):
        pass


这些方法可以增量的构建一个属性图。

首先，我们创建一个会话，然后在此会话内创建图。

.. code:: python

    sess = graphscope.session()
    graph = sess.g()

我们可以向图内添加一个点标签。相关的参数含义如下：

- :ref:`Loader Object`，代表数据源，指示 `graphscope` 可以在哪里找到源数据，可以为文件路径，或者 numpy 数组等；
- 点标签的名字；
- 一组属性名字。属性名应当与数据中的首行表头中的名字相一致。可选项，如果省略或为空，除ID列之外的所有列都将会作为属性载入；
- 作为ID列的列名，此列将会载入边时被用来做起始点ID或目标点ID。

来看一个例子。

.. code:: python
        
    graph = graph.add_vertices(
        "file:///home/admin/student.v",  # 数据源
        label="student",  # 标签名
        properties=["name", "lesson_number", "avg_score"],  # 列名，被作为属性载入
        vid_field="student_id"  # ID 列名
    )

我们也可以省略点定义的某些项。

- 属性列表可以为空，代表所有列都将作为属性；
- vid 可以用索引来表示。默认值为0，代表第一列。

.. code:: python
        
    graph.add_vertices("file:///home/admin/student.v", label="student")


在完整的图定义中，`vertices` 可以被整体省略。
`graphscope` 将会从边的起始点和目标点中提取点ID，将 `_` 作为点标签名字。

.. code:: python

    g = graphscope_session.load_from(
        edges={
            "group": "file:///home/admin/group.e"
            }
        )



我们可以再向图内添加一个边标签。相关的参数含义如下：

- :ref:`Loader Object`，代表数据源，告知 `graphscope` 可以在哪里找到源数据，可以是文件路径，或者 numpy 数组等；
- 边标签的名字；
- 一组属性名字。属性名应当与数据中的首行表头中的名字相一致。可选项，如果省略或为空，除起始点列和目标点列之外的所有列都将会作为属性载入；
- 起点标签名；
- 终点标签名；
- 起点的ID列名；
- 终点的ID列名。

让我们看一个例子。

.. code:: python

    graph = graph.add_edges(
        "file:///home/admin/group.e",  # 数据源
        label="group",  # 标签名
        properties=["group_id", "member_size"],  # 选择数据中的一些列，作为属性载入
        src_label="student",  # 起始点标签名
        dst_label="student",  # 终点标签名
        src_field="leader_student_id",  # 使用 `leader_student_id` 列作为起始点ID列
        dst_field="member_student_id",  # 使用 `member_student_id` 列作为终点ID列
    )


在一些情况下，一种边的标签可能连接了两种及以上的点。例如，在下面的属性图中，有一个名为 `group` 的边标签，
连接了两种点标签，即既有学生之间组成的 `group`，又有教师和学生之间组成的 `group`。
在这种情况下，可以添加两次名为 `group` 的边，但是有不同的起始点标签和终点标签。


.. code:: python

    graph = graph.add_edges("file:///home/admin/group.e",
            label="group",
            properties=["group_id", "member_size"],
            src_label="student", dst_label="student",
            src_field="leader_student_id", dst_field="member_student_id"
        )

    graph = graph.add_edges("file:///home/admin/group_for_teacher_student.e",
        label="group",
        properties=["group_id", "member_size"],
        src_label="teacher", dst_label="student",
        src_field="teacher_in_charge_id", dst_field="member_student_id"
    )

值得注意的是，对于同一个标签的多个定义，其属性列表的数量和类型应该一致，最好名字也一致，
因为同一个标签的所有定义的数据都将会被放入同一张表，属性名将会使用第一个定义中指定的名字。


定义边类时可以省略某些项。
比如，属性列表可以为空，表示载入所有列。

.. code:: python

    graph = graph.add_edges(
        "file:///home/admin/group.e",
        label="group",
        src_label="student", dst_label="student",
        src_field="leader_student_id", dst_field="member_student_id"
    )


另外，所有的属性名都可以由索引来指代（索引从0开始）。
在下例中，第一列被指定为起始点的ID，第二列被指定为目标点的ID。

.. code:: python

    graph = graph.add_edges(
    "file:///home/admin/group.e",
    label="group",
    src_label="student", dst_label="student",
    src_field=0, dst_field=1,
    )

`src_field` 默认为 0， `dst_field` 默认为 1， 所以若边数据使用第一列为起始点ID，第二列为终点ID，
则参数可使用默认值。

.. code:: python

    graph = graph.add_edges(
    "file:///home/admin/group.e",
    label="group",
    src_label="student", dst_label="student",
    )

如果图中只存在一个点标签，那么可以省略指定点标签。
GraphScope 将会推断起始点标签和终点标签为这一个点标签。

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices("file:///home/admin/student.v", label="student")
    graph = graph.add_edges("file:///home/admin/group.e", label="group")
    # GraphScope 会将 `src_label` 和 `dst_label` 自动赋值为 `student`.


更进一步，我们可以完全省略掉点。 GraphScope 将会从边的起始点和目标点中提取点ID，将 `_` 作为点标签名字。
注意此时将不允许图中有手动配置的点，即只适合在很简单的图中使用。

.. code:: python

    graph = sess.g()
    graph.add_edges("file:///home/admin/group.e", label="group")
    # 载图后，图中将会包含一个点标签，名为 `_`, 和一个边标签，名为 `group`.

类 `Graph` 有三个配置元信息的参数，分别为：

- `oid_type`, 可以为 `int64_t` 或 `string`. 默认为 `int64_t`，会有更快的速度，和使用更少的内存.
- `directed`, bool, 默认为`True`. 指示载入无向图还是有向图.
- `generate_eid`, bool, 默认为 `True`. 指示是否为每条边分配一个全局唯一的ID.


让我们看一个完整的例子：

.. code:: python

    sess = graphscope.session()
    graph = sess.g()
    
    graph = graph.add_vertices(
        "/home/admin/student.v",
        "student",
        ["name", "lesson_nums", "avg_score"],
        "student_id",
    )
    graph = graph.add_vertices(
        "/home/admin/teacher.v", "teacher", ["name", "salary", "age"], "teacher_id"
    )
    graph = graph.add_edges(
        "file:///home/admin/group.e",
        "group",
        ["group_id", "member_size"],
        src_label="student",
        dst_label="student",
    )
    graph = graph.add_edges(
        "file:///home/admin/group_for_teacher_student.e",
        "group",
        ["group_id", "member_size"],
        src_label="teacher",
        dst_label="student",
    )

这里是一个更复杂的载入 LDBC-SNB 属性图的 `例子 <https://github.com/alibaba/GraphScope/blob/main/python/graphscope/dataset/ldbc.py>`_ 。


从 Numpy 和 Pandas 中载图
----------------------------

上文提到的数据源是一个 :ref:`Loader Object` 的类。`Loader` 包含文件路径或者数据本身。
`graphscope` 支持从 `pandas.DataFrame` 或 `numpy.ndarray` 中载图。

.. code:: python

    import pandas as pd

    df_e = pd.read_csv('group.e', sep=',',
                     usecols=['leader_student_id', 'member_student_id', 'member_size'])

    df_v = pd.read_csv('student.v', sep=',', usecols=['student_id', 'lesson_nums', 'avg_score'])

    # use a dataframe as datasource, properties omitted, col_0/col_1 will be used as src/dst by default.
    # (for vertices, col_0 will be used as vertex_id by default)
    g1 = sess.load_graph(edges=df_e, vertices=df_v)


从 `numpy.ndarray` 中载图。

.. code:: python

    import numpy

    array_e = [df_e[col].values for col in ['leader_student_id', 'member_student_id', 'member_size']]
    array_v = [df_v[col].values for col in ['student_id', 'lesson_nums', 'avg_score']]

    g2 = sess.load_graph(edges=array_e, vertices=array_v)


从文件路径载图
--------------------------

当 `loader` 包含文件路径时，它可能仅包含一个字符串。
文件路径应遵循 URI 标准。当收到包含文件路径的载图请求时， `graphscope` 将会解析 URI，调用相应的载图模块。

目前, `graphscope` 支持多种数据源：本地, OSS，S3，和 HDFS:
数据由 `libvineyard <https://github.com/alibaba/libvineyard>`_ 负责载入，`libvineyard` 使用 `fsspec <https://github.com/intake/filesystem_spec>`_ 解析不同的数据格式以及参数。任何额外的具体的配置都可以在Loader的可变参数列表中传入，这些参数会直接被传递到对应的存储类中。比如 `host` 和 `port` 之于 `HDFS`，或者是 `access-id`, `secret-access-key` 之于 oss 或 s3。

.. code:: python

    from graphscope.framework.loader import Loader

    ds1 = Loader("file:///var/datafiles/group.e")
    ds2 = Loader("oss://graphscope_bucket/datafiles/group.e", key='access-id', secret='secret-access-key', endpoint='oss-cn-hangzhou.aliyuncs.com')
    ds3 = Loader("hdfs:///datafiles/group.e", host='localhost', port='9000', extra_conf={'conf1': 'value1'})
    d34 = Loader("s3://datafiles/group.e", key='access-id', secret='secret-access-key', client_kwargs={'region_name': 'us-east-1'})

用户可以方便的实现自己的driver来支持更多的数据源，比如参照 `ossfs <https://github.com/alibaba/libvineyard/blob/main/modules/io/adaptors/ossfs.py>`_ driver的实现方式。
用户需要继承 `AbstractFileSystem` 类用来做scheme对应的resolver， 以及 `AbstractBufferedFile`。用户仅需要实现 ``_upload_chunk``,
``_initiate_upload`` and ``_fetch_range`` 这几个方法就可以实现基本的read，write功能。最后通过 ``fsspec.register_implementation('protocol_name', 'protocol_file_system')`` 注册自定义的resolver。
