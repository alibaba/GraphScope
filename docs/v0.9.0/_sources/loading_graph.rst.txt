.. _loading_graphs:

Loading Graphs
==============

GraphScope models graph data as 
`Property Graph <https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model>`_,
in which the edges/vertices are labeled and each label may have many properties.


Load Built-in Datasets
----------------------

GraphScope comes with a set of popular datasets, and utility functions to load them into memory,
makes it easy for user to get started.
Here's an example:

.. code:: python

    from graphscope.dataset import load_ogbn_mag
    graph = load_ogbn_mag()

In standalone mode, it will automatically download the data to :file:`~/.graphscope/dataset`, and it will remain in there for future usage.

However, in kubernetes mode, it's not trivial to download the data to the pod's local storage, so we provide an option to mount a OSS bucket, which contains all available datasets. For example, we load the same graph as above, but this time graphscope is running in a Kubernetes cluster:

.. code:: python

    import graphscope
    from graphscope.dataset import load_ogbn_mag
    sess = graphscope.session(cluster_type='k8s', mount_dataset='/dataset')
    graph = load_ogbn_mag(sess, '/dataset/ogbn_mag_small')

Here, we first created a `Session` in a Kubernetes cluster, and mount the dataset bucket to :file:`/dataset`, this path is relative to Pods. Then we pass that session as first parameter, and :file:`/dataset/ogbn_mag_small` as second parameter. The :file:`/dataset` is the root path of datasets which we have assigned by `mount_dataset`, the `ogbn_mag_small` is the sub folder name of the dataset.

You can view all avaiable datasets in `here <https://github.com/alibaba/GraphScope/tree/main/python/graphscope/dataset>`_ , and get details description and usage in those source files.



Loading Your Own Datasets
--------------------------

However, it's more common that user need to load there own data and do some analysis.
To load a property graph to GraphScope, we provide a method `g()` defined in `Session`.

To build a property graph on GraphScope, we firstly create an empty graph using ``g()``.

.. code:: python

    import graphscope
    sess = graphscope.session()
    # Use `sess = graphscope.session(cluster_type='hosts')` if you are in standalone mode.
    graph = sess.g()

The class `Graph` has several methods:

.. code:: python

    def add_vertices(self, vertices, label="_", properties=None, vid_field=0):
        pass

    def add_edges(self, edges, label="_e", properties=None, src_label=None, dst_label=None, src_field=0, dst_field=1):
        pass

These methods helps users to construct the schema of the property graph iteratively.

We will use files in :file:`ldbc_sample` through this tutorial. You can get the files in `here <https://github.com/GraphScope/gstest/tree/master/ldbc_sample>`_. And you can inspect the graph schema by using `print(graph.schema)`.

Adding Vertices
^^^^^^^^^^^^^^^

We can add a kind of vertices to graph, the method has the following parameters:

``vertices``: A location for the vertex data source, which can be a file location, or a numpy, etc. See more details in :ref:`Loader Object`.

A simple example:

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv')

It will read data from the the location :file:`/home/ldbc_sample/person_0_0.csv`. Since we didn't give additional arguments, these vertices will be labeled ``_`` by default, using the first column in the file as their ID, and other columns as their properties. Both the names and data types of properties will be deduced.

Another commonly used parameter is label:

``label``: The label name of the vertex, default to `_`.

Since a property graph allows many kinds of vertices, it is suggested for users to give each kind of vertices a meaningful label name. For example:


.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')

Then we have a graph with one kind of vertices, its label name is person.

In addition, each kind of labeled vertices have their own properties. Here is the third parameter:

``properties``: A list of properties, Optional, default to ``None``.

This parameter selects the corresponding columns from the source data file or pandas DataFrames as properties. Please note that 
the values of this parameter should exist in the file/DataFrame. By default( values ``None``), all columns except the ``vid_field`` column 
will be added as properties. If it equals to a empty list ``[]``, then no properties will be added. 

For example:

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


``vid_field`` determines which column used as vertex ID. (as well as the source ID or destination ID when loading edges.) 

It can be a ``str``, the name of columns, or ``int``, representing the index of the columns.

By default, the value is 0, hence the first column will be used as vertex ID.

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', vid_field='firstName')

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', vid_field=0)


Adding Edges
^^^^^^^^^^^^

Next, let's take a look on the parameters for loading edges.

``edges``: The location indicating where to read the data. e.g.,

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    # Note we already added a vertex label named 'person'.
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', src_label='person', dst_label='person')

This will load an edge which label is ``_e`` (the default value), its source vertex and destination vertex will be ``person``, using the **first column** as the source vertex ID, the **second column** as the destination vertex ID, the others as properties.

Similar to vertices, we can use parameter `label` to assign label name and `properties` to select properties.

``label``: The label name of the edges, default to ``_e``. (It's recommended to use a meaningful label name.)
``properties``: A list of properties, default to ``None``(add all columns as properties). 

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', label='knows', src_label='person', dst_label='person')


Differ to vertices, edges have some additional parameters.

```src_label```: The label name of the source vertex. 
```dst_label```: The label name of the destination vertex, it can be different to the ``src_label``, 
```src_field``` and ```dst_field```: The columns used for source(destination) vertex id. Default to 0 and 1, respectively.

e.g.,

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    graph = graph.add_vertices('/home/ldbc_sample/comment_0_0.csv', label='comment')
    # Note we already added a vertex label named 'person'.
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment')


The value and behavior is similar to `vid_field` in Vertex, except for it takes two columns as edges is constituted by source vertex id and destination vertex id. Here's an example:

Examples of ``src_field`` and ``dst_field``:

.. code:: python

    # Steps to init a graph and add vertices are omitted
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment', src_field='Person.id', dst_field='Comment.id')
    # Or use the index.
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment', src_field=0, dst_field=1)


Advanced Usage
--------------

Here are some advanced usages to deal with homogeneous graphs or very complex graphs.

Deduce vertex labels when not ambiguous
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If there is only one kind of vertices in a graph, the vertex label can be omitted.
GraphScope will infer the source and destination vertex label to that very label.

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    # GraphScope will assign `src_label` and `dst_label` to `person` automatically.
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv')


Deduce vertex from edges
^^^^^^^^^^^^^^^^^^^^^^^^

If user add edges with unseen ``src_label`` or ``dst_label``, graphscope will extract an vertex table from the given labels from the edge data.

.. code:: python

    graph = sess.g()
    # Deduce vertex label `person` from the source and destination endpoints of edges.
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', src_label='person', dst_label='person')

    graph = sess.g()
    # Deduce the vertex label `person` from the source endpoint,
    # and vertex label `comment` from the destination endpoint of edges.
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment')


Multiple relations
^^^^^^^^^^^^^^^^^^

In some cases, an edge label may connect two kinds of vertices. For example, in a
graph, two kinds of edges are labeled with `likes` but represents two relations.
i.e., `person` -> `likes` <- `comment` and `person` -> `likes` <- `post`. 

In this case, we can simply add the relation again with the same edge label,
but with different source and destination labels.

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

   1. This feature(multiple relations using same edge label) is only avaiable in `lazy` mode yet.
   2. It is worth noting that for several configurations in the side `Label`, 
      the attributes should be the same in number and type, and preferably 
      have the same name, because the data of the same `Label` will be put into one Table, 
      and the attribute names will uses the names specified by the first configuration.


Specify data types of properties manually
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

GraphScope will deduce data types from input files, and it works as expected in most cases.
However, sometimes user may want to determine the data types as well, e.g.

.. code:: python

    graph = sess.g()
    graph = graph.add_vertices('/home/ldbc_sample/post_0_0.csv', label='post', properties=['content', ('length', 'int'), ])

It forces the property to be (casted and) loaded as specified data type. The format of this parameter is tuple(s) with the name and the type.
e.g., in this case, the property ``length`` will have type ``int`` rather than the default ``int64_t``. The options of the types are ``int``, ``int64``, ``float``, ``double``, or ``str``.


Other parameters of graph
^^^^^^^^^^^^^^^^^^^^^^^^^

The class ``Graph`` has three meta options, which are:

- ``oid_type``, can be ``int64_t`` or ``string``. Default to ``int64_t`` in consideration of efficiency. But if the ID column can't be represented by ``int64_t``, then we should use ``string``.
- ``directed``, boolean value and default to ``True``. Controls to load an directed or undirected graph.
- ``generate_eid``, bool, default to ``True``, whether to generate an unique id for all edges automatically.


Putting them Together
^^^^^^^^^^^^^^^^^^^^^

Let's make this example complete. 
A more complex example to load LDBC snb graph can be find [here](https://github.com/alibaba/GraphScope/blob/main/python/graphscope/dataset/ldbc.py).

.. code:: python

    graph = sess.g(oid_type='int64_t', directed=True, generate_eid=True)
    graph = graph.add_vertices('/home/ldbc_sample/person_0_0.csv', label='person')
    graph = graph.add_vertices('/home/ldbc_sample/comment_0_0.csv', label='comment')
    graph = graph.add_vertices('/home/ldbc_sample/post_0_0.csv', label='post')
    
    graph = graph.add_edges('/home/ldbc_sample/person_knows_person_0_0.csv', label='knows', src_label='person', dst_label='person')
    graph = graph.add_edges('/home/ldbc_sample/person_likes_comment_0_0.csv', label='likes', src_label='person', dst_label='comment')
    graph = graph.add_edges('/home/ldbc_sample/person_likes_post_0_0.csv', label='likes', src_label='person', dst_label='post')

    print(graph.schema)

A more complex example to load LDBC snb graph can be find `here <https://github.com/alibaba/GraphScope/blob/main/python/graphscope/dataset/ldbc.py>`_.

Loading From Pandas or Numpy
-----------------------------

The data source aforementioned is an object of :ref:`Loader`. A loader wraps a location or the data itself. 
GraphScope supports load a graph from pandas dataframes or numpy ndarrays, making it easy to construct a graph right in the python console.

Apart from the loader, the other fields like properties, label, etc. are the same as examples above.


From Pandas
^^^^^^^^^^^

.. code:: python

    import pandas as pd

    df_v = pd.read_csv('/home/ldbc_sample/comment_0_0.csv', sep='|')
    df_e = pd.read_csv('/home/ldbc_sample/comment_replyOf_comment_0_0.csv', sep='|')

    # use a dataframe as datasource, properties omitted,
    # for edges, col_0/col_1 will be used as src/dst by default.
    # for vertices, col_0 will be used as vertex_id by default.
    graph = sess.g().add_vertices(df_v).add_edges(df_e)


From Numpy
^^^^^^^^^^

Note that each array is a column, we pass it like as COO matrix format to the loader.

.. code:: python

    import numpy

    array_v = [df_v[col].values for col in df_v.columns.values]
    array_e = [df_e[col].values for col in df_e.columns.values]

    graph = sess.g().add_vertices(array_v).add_edges(array_e)


Loader Variants
---------------

When a loader wraps a location, it may only contains a str.
The string follows the standard of URI. When receiving a request for loading graph from a location, 
``graphscope`` will parse the URI and invoke corresponding loader according to the schema.

Currently, ``graphscope`` supports loaders for ``local``, ``s3``, ``oss``, ``hdfs``.
Under the hood, data is loaded distributedly by `v6d <https://github.com/v6d-io/v6d>`_, ``v6d`` takes advantage
of `fsspec <https://github.com/intake/filesystem_spec>`_ to resolve specific scheme and formats.
Any additional configurations can be passed in kwargs of ``Loader``, which will be parsed 
directly by the specific class. e.g., ``host`` and ``port`` to ``hdfs``, or ``access-id``, ``secret-access-key`` to ``oss`` or ``s3``.


.. code:: python

    from graphscope.framework.loader import Loader

    ds1 = Loader("file:///var/datafiles/group.e")
    ds2 = Loader("oss://graphscope_bucket/datafiles/group.e", key='access-id', secret='secret-access-key', endpoint='oss-cn-hangzhou.aliyuncs.com')
    ds3 = Loader("hdfs:///datafiles/group.e", host='localhost', port='9000', extra_conf={'conf1': 'value1'})
    d34 = Loader("s3://datafiles/group.e", key='access-id', secret='secret-access-key', client_kwargs={'region_name': 'us-east-1'})

User can implement customized driver to support additional data sources. Take `ossfs <https://github.com/v6d-io/v6d/blob/main/modules/io/adaptors/ossfs.py>`_ as an example, User need to subclass `AbstractFileSystem`, which
is used as resolve to specific protocol scheme, and `AbstractBufferFile` to do read and write.
The only methods user need to override is ``_upload_chunk``,
``_initiate_upload`` and ``_fetch_range``. In the end user need to use ``fsspec.register_implementation('protocol_name', 'protocol_file_system')`` to register corresponding resolver.