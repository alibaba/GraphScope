.. _loading_graphs:

Loading Graphs
==============

GraphScope models graph data as 
`Property Graph <https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model>`_,
in which the edges/vertices are labeled and each label may have many properties.

Building a Graph
-------------------------

To load a property graph to GraphScope, we provide a method `g()` defined in `Session`.

First, we create a session, then a graph instance inside that session.

.. code:: python

    sess = graphscope.session()
    graph = sess.g()

The class `Graph` has several methods:

.. code:: python

    def add_vertices(self, vertices, label="_", properties=[], vid_field=0):
        pass

    def add_edges(self, edges, label="_", properties=[], src_label=None, dst_label=None, src_field=0, dst_field=1):
        pass

These methods helps users to construct the schema of the property graph iteratively.

We can add a kind of vertices to graph.

The parameters contain:

- A loader for data source, which can be a file location, or a numpy, etc. See more details in :ref:`Loader Object`.
- The label name of the vertex.
- A list of properties, the names should consistent to the header_row of the data source file or pandas. This list is optional. When use default value, all columns except the vertex_id column will be added as properties.
- The column used as vertex_id. The value in this column of the data source will be used for src/dst when loading edges.

Let's see an example.

.. code:: python

    graph = graph.add_vertices(
        "file:///home/admin/student.v",  # The data source.
        label="student",  # Label name
        properties=["name", "lesson_number", "avg_score"],  # Columns loaded as property
        vid_field="student_id"  # Columns used for vertex_id
    )


We can also omit certain parameters for vertices.

- properties can be empty, which means that all columns are selected as properties;
- vid can be represented by a number of index. Default is 0, which is the first column.

In the simplest case, the configuration can only contains a loader. In this case, the first column
is used as vid, and the rest columns are used as properties.


.. code:: python

    graph.add_vertices("file:///home/admin/student.v", label="student")


Then we can add a kind of edges to graph.

The parameter contains:

- a :ref:`Loader Object` for data source, it tells `graphscope` where to find the data for this label, it can be a file location, or a numpy, etc.
- The label name of the edge.
- a list of properties, the names should consistent to the header_row of the data source file or pandas. This list is optional. When it omitted or empty, all columns except the src/dst columns will be added as properties.
- The label name of the source vertex.
- The label name of the destination vertex.
- The column use for source vertex id.
- The column used for destination vertex id.

Let's see an example.

.. code:: python

    graph = graph.add_edges(
        "file:///home/admin/group.e",  # The data source
        label="group",  # Label name
        properties=["group_id", "member_size"],  # Selected column names in group.e, will load as properties
        src_label="student",  # Label name of the source vertex
        dst_label="student",  # Label name of the destination vertex
        src_field="leader_student_id",  # Use `leader_student_id` column as src id
        dst_field="member_student_id",  # Use `member_student_id` column as dst id
    )


In some cases, an edge label may connect two kinds of vertices. For example, in a
graph, two kinds of edges are labeled with `group` but represents two relations.
i.e., `teacher` -> `group` <- `student` and `student` <- `group` <- `student`. 
In this case, we can simple add the relation again with the same edge label,
but with different source and destination label.


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

.. It is worth noting that for several configurations in the side `Label`, 
.. the attributes should be the same in number and type, and preferably 
.. have the same name, because the data of the same `Label` will be put into one Table, 
.. and the attribute names will uses the names specified by the first configuration.

Some parameters can omitted for edges.
e.g., properties can be empty, which means to select all columns

.. code:: python

    graph = graph.add_edges(
        "file:///home/admin/group.e",
        label="group",
        src_label="student", dst_label="student",
        src_field="leader_student_id", dst_field="member_student_id"
    )

Src and dst fields can be assigned by number, which represents the column index
in the data source.

The following statement means the first column is used as src_id and the second column is used as dst_id:

.. code:: python

    graph = graph.add_edges(
    "file:///home/admin/group.e",
    label="group",
    src_label="student", dst_label="student",
    src_field=0, dst_field=1,
    )

The default value of `src_field` is `0`, and default value of `dst_field` is `1`.
So if your edges use the first column as source vid, and second column as destination vid,
you can just use the default value for the parameter.

.. code:: python

    graph = graph.add_edges(
    "file:///home/admin/group.e",
    label="group",
    src_label="student", dst_label="student",
    )

If there is only one vertex label in the graph, the label of vertices can be omitted.
GraphScope will infer the source and destination vertex label is that very label.

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices("file:///home/admin/student.v", label="student")
    graph = graph.add_edges("file:///home/admin/group.e", label="group")
    # GraphScope will assign `src_label` and `dst_label` to `student` automatically.


Moreover, the vertices can be totally omitted.
`graphscope` will extract vertices ids from edges, and a default label `_` will assigned 
to all vertices in this case.

Note this have some constraints that there cannot be any manually added vertex in graphs.
It only serve the most simple cases.

.. code:: python

    graph = sess.g()
    graph.add_edges("file:///home/admin/group.e", label="group")
    # After loaded, the graph will have an vertex label called `_`, and an edge label called `group`.


The class `Graph` has three meta options, which are:

- `oid_type`, can be `int64_t` or `string`. Default to `int64_t` cause it's more faster and costs less memory.
- `directed`, bool, default to `True`. Controls load an directed or undirected Graph.
- `generate_eid`, bool, default to `True`. Whether to automatically generate an unique id for all edges.


Let's make the example complete:

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

A more complex example to load LDBC snb graph can be find `here <https://github.com/alibaba/GraphScope/blob/main/python/graphscope/dataset/ldbc.py>`_.


Graphs from Numpy and Pandas
----------------------------

The datasource aforementioned is an object of :ref:`Loader`. A loader wraps
a location or the data itself. `graphscope` supports load a graph
from pandas dataframes or numpy ndarrays.

.. code:: python

    import pandas as pd

    df_e = pd.read_csv('group.e', sep=',',
                     usecols=['leader_student_id', 'member_student_id', 'member_size'])

    df_v = pd.read_csv('student.v', sep=',', usecols=['student_id', 'lesson_nums', 'avg_score'])

    # use a dataframe as datasource, properties omitted, col_0/col_1 will be used as src/dst by default.
    # (for vertices, col_0 will be used as vertex_id by default)
    graph = sess.g().add_vertices(df_v).add_edges(df_e)


Or load from numpy ndarrays

.. code:: python

    import numpy

    array_e = [df_e[col].values for col in ['leader_student_id', 'member_student_id', 'member_size']]
    array_v = [df_v[col].values for col in ['student_id', 'lesson_nums', 'avg_score']]

    graph = sess.g().add_vertices(array_v).add_edges(array_e)


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

    from graphscope.framework.loader import Loader

    ds1 = Loader("file:///var/datafiles/group.e")
    ds2 = Loader("oss://graphscope_bucket/datafiles/group.e", key='access-id', secret='secret-access-key', endpoint='oss-cn-hangzhou.aliyuncs.com')
    ds3 = Loader("hdfs:///datafiles/group.e", host='localhost', port='9000', extra_conf={'conf1': 'value1'})
    d34 = Loader("s3://datafiles/group.e", key='access-id', secret='secret-access-key', client_kwargs={'region_name': 'us-east-1'})

User can implement customized driver to support additional data sources. Take `ossfs <https://github.com/alibaba/libvineyard/blob/main/modules/io/adaptors/ossfs.py>`_ as an example, User need to subclass `AbstractFileSystem`, which
is used as resolve to specific protocol scheme, and `AbstractBufferFile` to do read and write.
The only methods user need to override is ``_upload_chunk``,
``_initiate_upload`` and ``_fetch_range``. In the end user need to use ``fsspec.register_implementation('protocol_name', 'protocol_file_system')`` to register corresponding resolver.