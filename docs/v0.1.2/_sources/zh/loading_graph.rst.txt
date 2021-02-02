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

    load_from(edges, vertices)

`edges` 是一个 `Dict`（字典），字典中的每一项键值对都代表一个边类的标签。具体来说，每一项的键是标签的名字，
其值则是一个 `Tuple`（元组） 或 `List`（列表），包含以下几项：

- :ref:`Loader Object`，代表数据源，告知 `graphscope` 可以在哪里找到源数据，可以是文件路径，或者 numpy 数组等；
- 一组属性名字。属性名应当与数据中的首行表头中的名字相一致。可选项，如果省略或为空，除起始点列和目标点列之外的所有列都将会作为属性载入；
- 定义边的起始点的元组，格式为（起点列名，起点标签名）；
- 定义边的目标点的元组，格式为（目标列名，目标标签名）；

一个实际的例子如下：

.. code:: python

    edges={
        # 一个边类，标签名为 "group"
        "group": (
            # 数据源，在这里是本地文件路径
            "file:///home/admin/group.e",
            # 选择 group.e 中的若干列作为属性载入
            ["group_id", "member_size"],
            # 'leader_student_id' 列作为起始点列, 起始点标签为 'student'
            ("leader_student_id", "student"),
            # 'member_student_id' 列作为目标点列, 目标点标签为 'student'
            ("member_student_id", "student")
        )
    }

另外，还可以用字典来定义图模型，其中有若干个保留字作为字典中的键，分别为 `loader`, `properties`, `source` 和 `destination`。
以下例子中的图模型的定义和上例完全一致。

.. code:: python

    edges = {
        "group": {
                "loader": "file:///home/admin/group.e",
                "properties": ["group_id", "member_size"],
                "source": ("leader_teacher_id", "teacher"),
                "destination": ("member_teacher_id", "teacher"),
            },
        }

在某些情况下，一种边的标签可能连接了两种及以上的点。例如，在下面的属性图中，有一个名为 `group` 的边标签，
连接了两种点标签，即既有学生之间组成的 `group`，又有教师和学生之间组成的 `group`。
在这种情况下，`group` 边类接受一组定义的列表。

.. code:: python

    edges={
        # 标签名为 "group" 的边类
        "group": [
            (
                "file:///home/admin/group.e",
                ["group_id", "member_size"],
                ("leader_student_id", "student"),
                ("member_student_id", "student")
            ),
            (
                "file:///home/admin/group_for_teacher_student.e",
                ["group_id", "group_name", "establish_date"],
                ("teacher_in_charge_id", "teacher"),
                ("member_student_id", "student")
            )
        ]
    }

值得注意的是，对于同一个标签的多个定义，其属性列表的数量和类型应该一致，最好名字也一致，
因为同一个标签的所有定义的数据都将会被放入同一张表，属性名将会使用第一个定义中指定的名字。

定义边类时可以省略某些项。
比如，属性列表可以为空，表示载入所有列。

.. code:: python

    edges={
        "group": (
            "file:///home/admin/group.e",
            [],
            ("leader_student_id", "student"),
            ("member_student_id", "student")
        )
    }

另外，所有的属性名都可以由索引来指代（索引从0开始）。
在下例中，第一列被指定为起始点的ID，第二列被指定为目标点的ID。

.. code:: python

    edges={
        "group": (
            "/home/admin/group.e",
            ["group_id", "member_size"],
            # 0 代表第1列，用作起始点ID列
            (0, "student"),
            # 1 代表第二列，用作目标点ID列
            (1, "student"),
        )
    }

如果属性图中只有一个点标签，那么可以省略起始点和目标点标签的名字（默认边的两端都为这一种点标签）

.. code:: python

    edges={
        "group": (
            "file:///home/admin/group.e",
            ["group_id", "member_size",]
            # 省略起始点和目标点的点标签名字
            "leader_student_id",
            "member_student_id",
        )
    }

在极简的情况下，边的定义可以只包含一个数据源。
默认情况下，第一列被用作起始点ID，第二列被用作目标点ID，剩余所有列被用作属性。

.. code:: python

    edges={
        "group": "file:///home/admin/group.e"
    }


同边类似，`vertices` 通常也是一个字典，包含点标签的名字以及其具体定义。定义包含如下几项：

- :ref:`Loader Object`，代表数据源，指示 `graphscope` 可以在哪里找到源数据，可以为文件路径，或者 numpy 数组等；
- 一组属性名字。属性名应当与数据中的首行表头中的名字相一致。可选项，如果省略或为空，除ID列之外的所有列都将会作为属性载入；
- 作为ID列的列名，此列将会载入边时被用来做起始点ID或目标点ID。

如下是一个点定义的例子:

.. code:: python

    vertices={
        "student": (
            # 标签为 student 的数据源
            "file:///home/admin/student.v",
            # 载入为属性的列名
            ["name", "lesson_number", "avg_score"],
            # 用此列作为 ID
            "student_id"
        )
    }


与边类似，每个点的定义也可以是一个字典，其保留字为 `loader`, `properties` 和 `vid`。

.. code:: python

    vertices={
        "student": {
            "loader": "file:///home/admin/student.v",
            "properties": ["name", "lesson_nums", "avg_score"],
            "vid": "student_id",
        },
    },

我们也可以省略点定义的某些项。

- 属性列表可以为空，代表所有列都将作为属性；
- vid 可以用索引来表示。

在极简情况下，点的定义可以只包含一个数据源。此时，第 1 列被用作 ID 列，其余列都将作为属性。

.. code:: python

    vertices={
        "student": "file:///home/admin/student.v"
    }

在完整的图定义中，`vertices` 可以被整体省略。
`graphscope` 将会从边的起始点和目标点中提取点ID，将 `_` 作为点标签名字。

.. code:: python

    g = graphscope_session.load_from(
        edges={
            "group": "file:///home/admin/group.e"
            }
        )


来看一个完整的例子:

.. code:: python

    g = graphscope_session.load_from(
        edges={
            "group": [
                (
                    "file:///home/admin/group.e",
                    ["group_id", "member_size"],
                    ("leader_student_id", "student"),
                    ("member_student_id", "student"),
                ),
                (
                    "file:///home/admin/group_for_teacher_student.e",
                    ["group_id", "group_name", "establish_date"],
                    ("teacher_in_charge_id", "teacher"),
                    ("member_student_id", "student"),
                ),
            ]
        },
        vertices={
            "student": (
                "/home/admin/student.v",
                ["name", "lesson_nums", "avg_score"],
                "student_id",
            ),
            "teacher": (
                "/home/admin/teacher.v",
                ["name", "salary", "age"],
                "teacher_id",
            ),
        },
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

    from graphscope import Loader

    ds1 = Loader("file:///var/datafiles/group.e")
    ds2 = Loader("oss://graphscope_bucket/datafiles/group.e", key='access-id', secret='secret-access-key', endpoint='oss-cn-hangzhou.aliyuncs.com')
    ds3 = Loader("hdfs://datafiles/group.e", host='localhost', port='9000', extra_conf={'conf1': 'value1'})
    d34 = Loader("s3://datafiles/group.e", key='access-id', secret='secret-access-key', client_kwargs={'region_name': 'us-east-1'})

用户可以方便的实现自己的driver来支持更多的数据源，比如参照 `ossfs <https://github.com/alibaba/libvineyard/blob/main/modules/io/adaptors/ossfs.py>`_ driver的实现方式。
用户需要继承 `AbstractFileSystem` 类用来做scheme对应的resolver， 以及 `AbstractBufferedFile`。用户仅需要实现 ``_upload_chunk``,
``_initiate_upload`` and ``_fetch_range`` 这几个方法就可以实现基本的read，write功能。最后通过 ``fsspec.register_implementation('protocol_name', 'protocol_file_system')`` 注册自定义的resolver。


+------------------------------+---------------------------------------------+
| :meth:`graphscope.load_from` | Loading from local filesystem, OSS, or ODPS |
+------------------------------+---------------------------------------------+