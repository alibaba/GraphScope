.. _loading_graphs:

Loading Graphs
==============

GraphScope models graph data as 
`Property Graph <https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model>`_,
in which the edges/vertices are labeled and each label may have many properties.

Configurations of a Graph
-------------------------

To load a property graph to GraphScope, we provide a function:

.. code:: python

    load_from(edges, vertices)

This function helps users to construct the schema of the property graph.
`edges` is a `Dict`. Each pair item in the dict determines a label for the edges.
More specifically, the key of the pair item is the label name, the value of
the pair is a configuration `Tuple` or `List`, which contains:

- a :ref:`Loader Object` for data source, it tells `graphscope` where to find the data for this label, it can be a file location, or a numpy, etc.
- a list of properties, the names should consistent to the header_row of the data source file or pandas. This list is optional. When it omitted or empty, all columns except the src/dst columns will be added as properties.
- a pair of str for the edge source, in the format of (column_name_for_src, label_of_src);
- a pair of str for the edge destination, in the format of (column_name_for_dst, label_of_dst);

Let's see an example:

.. code:: python

    edges={
        # a kind of edge with label "group"
        "group": (
            # the data source, in this case, is a file location.
            "file:///home/admin/group.e",
            # selected column names in group.e, will load as properties
            ["group_id", "member_size"],
            # use 'leader_student_id' column as src id, the src label should be 'student'
            ("leader_student_id", "student"),
            # use 'member_student_id' column as dst id, the dst label is 'student'
            ("member_student_id", "student")
        )
    }

Alternatively, the configuration can be a `Dict`,
The reserved keys of the `Dict` are "loader", "properties", "source" and "destination".
This configuration for edges are exactly the same to the above configuration.

.. code:: python

    edges = {
        "group": {
                "loader": "file:///home/admin/group.e",
                "properties": ["group_id", "member_size"],
                "source": ("leader_teacher_id", "teacher"),
                "destination": ("member_teacher_id", "teacher"),
            },
        }

In some cases, an edge label may connect two kinds of vertices. For example, in a
graph, two kinds of edges are labeled with `group` but represents two relations.
i.e., `teacher`-`group`-> `student` and `student`-`group`-> `student`. 
In this case, a `group` key follows a list of configurations.

.. code:: python

    edges={
        # a kind of edge with label "group"
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

.. It is worth noting that for several configurations in the side `Label`, 
.. the attributes should be the same in number and type, and preferably 
.. have the same name, because the data of the same `Label` will be put into one Table, 
.. and the attribute names will uses the names specified by the first configuration.

Some configurations can omit for edges.
e.g., properties can be empty, which means to select all columns

.. code:: python

    edges={
        "group": (
            "file:///home/admin/group.e",
            [],
            ("leader_student_id", "student"),
            ("member_student_id", "student")
        )
    }

Alternatively, all column names can be assigned with index.
For example, the number in the src/dst assigned 
the first column is used as src_id and the second column is used as dst_id:

.. code:: python

    edges={
        "group": (
            "/home/admin/group.e",
            ["group_id", "member_size"],
            # 0 represents the first column.
            (0, "student"),
            # second column used as dst.
            (1, "student"),
        )
    }

If there is only one label in the graph, the label of vertices can be omitted.

.. code:: python

    edges={
        "group": (
            "file:///home/admin/group.e",
            ["group_id", "member_size",]
            # vertex labels in the two ends of the edges are omitted.
            "leader_student_id",
            "member_student_id",
        )
    }

In the simplest case, 
the configuration can only assign a loader with path. 
By default, 
the first column will be used as src_id, 
the second column will be used as dst_id.
all the rest columns in the file are parsed as properties.

.. code:: python

    edges={
        "group": "file:///home/admin/group.e"
    }


Similar to edges, a vertex `Dict` contains a key as the label, and a set of configuration
for the label. The configurations contain:

- a loader for data source, which can be a file location, or a numpy, etc. See more details in :ref:`Loader Object`.
- a list of properties, the names should consistent to the header_row of the data source file or pandas. This list is optional. When it omitted, all columns except the vertex_id column will be added as properties.
- the column used as vertex_id. The value in this column of the data source will be used for src/dst when loading edges.

Here is an example for vertices:

.. code:: python

    vertices={
        "student": (
            # source file for vertices labeled as student;
            "file:///home/admin/student.v",
            # columns loaded as property
            ["name", "lesson_number", "avg_score"],
            # the column used for vertex_id
            "student_id"
        )
    }


Like the edges, the configuration for vertices can also be a `Dict`, 
in which the keys are "loader", "properties" and "vid"

.. code:: python

    vertices={
        "student": {
            "loader": "file:///home/admin/student.v",
            "properties": ["name", "lesson_nums", "avg_score"],
            "vid": "student_id",
        },
    },

We can also omit certain configurations for vertices.

- properties can be empty, which means that all columns are selected as properties;
- vid can be represented by a number of index,

In the simplest case, the configuration can only contains a loader. In this case, the first column
is used as vid, and the rest columns are used as properties.


.. code:: python

    vertices={
        "student": "file:///home/admin/student.v"
    }

Moreover, the vertices can be totally omitted. 
`graphscope` will extract vertices ids from edges, and a default label `_` will assigned 
to all vertices in this case.

.. code:: python

    g = graphscope_session.load_from(
        edges={
            "group": "file:///home/admin/group.e"
            }
        )


Let's make the example complete:

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

A more complex example to load LDBC snb graph can be find `here <https://github.com/alibaba/GraphScope/blob/main/python/graphscope/dataset/ldbc.py>`_.


Graphs from Numpy and Pandas
----------------------------

The datasource aforementioned is an object of :ref`Loader`. A loader wraps
a location or the data itself. `graphscope` supports load a graph
from pandas dataframes or numpy ndarrays.

.. code:: python

    import pandas as pd

    df_e = pd.read_csv('group.e', sep=',',
                     usecols=['leader_student_id', 'member_student_id', 'member_size'])

    df_v = pd.read_csv('student.v', sep=',', usecols=['student_id', 'lesson_nums', 'avg_score'])

    # use a dataframe as datasource, properties omitted, col_0/col_1 will be used as src/dst by default.
    # (for vertices, col_0 will be used as vertex_id by default)
    g1 = sess.load_graph(edges=df_e, vertices=df_v)


Or load from numpy ndarrays

.. code:: python

    import numpy

    array_e = [df_e[col].values for col in ['leader_student_id', 'member_student_id', 'member_size']]
    array_v = [df_v[col].values for col in ['student_id', 'lesson_nums', 'avg_score']]

    g2 = sess.load_graph(edges=array_e, vertices=array_v)


Graphs from Given Location
--------------------------

When a loader wraps a location, it may only contains a str.
The string follows the standard of URI. When receiving a request for loading graph
from a location, `graphscope` will parse the URI and invoke corresponding loader
according to the schema.

Currently, `graphscope` supports loaders for `local`, `s3`, `oss`, `hdfs`:
Data is loaded by `libvineyard <https://github.com/alibaba/libvineyard>`_ , `libvineyard` takes advantage
of `fsspec <https://github.com/intake/filesystem_spec>`_ to resolve specific scheme and formats.
Any additional specific configurations can be passed in kwargs of `Loader`, and these configurations will
directly be passed to corresponding storage class. Like `host` and `port` to `HDFS`, or `access-id`, `secret-access-key` to `oss` or `s3`.

.. code:: python

    from graphscope import Loader

    ds1 = Loader("file:///var/datafiles/group.e")
    ds2 = Loader("oss://graphscope_bucket/datafiles/group.e", key='access-id', secret='secret-access-key', endpoint='oss-cn-hangzhou.aliyuncs.com')
    ds3 = Loader("hdfs://datafiles/group.e", host='localhost', port='9000', extra_conf={'conf1': 'value1'})
    d34 = Loader("s3://datafiles/group.e", key='access-id', secret='secret-access-key', client_kwargs={'region_name': 'us-east-1'})

User can implement customized driver to support additional data sources. Take `ossfs <https://github.com/alibaba/libvineyard/blob/main/modules/io/adaptors/ossfs.py>`_ as an example, User need to subclass `AbstractFileSystem`, which
is used as resolve to specific protocol scheme, and `AbstractBufferFile` to do read and write.
The only methods user need to override is ``_upload_chunk``,
``_initiate_upload`` and ``_fetch_range``. In the end user need to use ``fsspec.register_implementation('protocol_name', 'protocol_file_system')`` to register corresponding resolver.

+------------------------------+---------------------------------------------+
| :meth:`graphscope.load_from` | Loading from local filesystem, OSS, or ODPS |
+------------------------------+---------------------------------------------+
