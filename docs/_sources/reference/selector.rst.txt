.. _selector:


Selector
========

Selectors are strs, or key-value pairs that used for :ref:`Graph`, or :ref:`Context`. Selectors are intended to be used to specify a subset of data of objects, they can not only retrieve the results of algorithms, but also elements on
the graph itself. The syntax of selectors slightly differ from each other For different kinds of graphs or contexts. Since we have 5 kinds of context, let's talk about them one by one.

Selectors in different context
------------------------------

The basic components of selector is `v`, `e`, and `r`, means vertex, edge and result, respectively.

.. warning::

    Selectors on edges is not supported yet, but will be supported in next releases.

TensorContext
+++++++++++++

:class:`TensorContext` holds a tensor inside the engine. In this case, selectors doesn't matter,
instead, we have an `axis` keyword that used to retrieve datas in the tensor respect to axis.


VertexDataContext
+++++++++++++++++

:class:`VertexDataContext` is used by algorithms that compatible graph is a simple graph,
i.e. no labels and property, and result of the algorithms has the form that
have exactly one data on every vertex, such as :func:`graphscope.sssp`, :func:`graphscope.pagerank`
Let's see what selectors of :class:`VertexDataContext` looks like:

    - The syntax of selector on vertex is:
        - `v.id`: Get the Id of vertices
        - `v.data`: Get the data of vertices (If there is any, means origin data on the graph, not results)

    - The syntax of selector of edge is:
        - `e.src`: Get the source Id of edges
        - `e.dst`: Get the destination Id of edges
        - `e.data`: Get the edge data on the edges (If there is any, means origin data on the graph)

    - The syntax of selector of results is:
        - `r`: Get quering results of algorithms. e.g. Rankings of vertices after doing PageRank. 


LabeledVertexDataContext
++++++++++++++++++++++++

:class:`LabeledVertexDataContext` is used by algorithms that compatible graphs is a property graph,
i.e. There are several labels in the graph, and each label has several properties, And results of
such algorithms would take the form that has one data on each vertex label.
Typical apps are UDF (User defined) Algorithms.

Selection are performed on labels first, then on properties.

We use `:` to filter labels, and `.` to select properties.
But the results has no property, only have labels.

    - The syntax of selector of vertex is:
        - `v:label_name.id`: Get Id that belongs to a specific vertex label.
        - `v:label_name.property_name`: Get data that on a specific property of a specific vertex label.

    - The syntax of selector of edge is:
        - `e:label_name.src`: Get source Id of a specific edge label.
        - `e:label_name.dst`: Get destination Id of a specific edge label.
        - `e:label_name.property_name`: Get data on a specific property of a specific edge label.

    - The syntax of selector of results is:
        - `r:label_name`: Get results data of a vertex label.


VertexPropertyContext
+++++++++++++++++++++

:class:`VertexPropertyContext` is used by algorithms that compatible graph is a simple graph,
i.e. no labels and property, and results of algorithms can have multiple datas (properties) on each vertex,
such as :func:`graphscope.hits`.

Note that selectors of :class:`VertexPropertyContext` only differed in `r` with :class:`VertexDataContext`.


    - The syntax of selector on vertex is:
        - `v.id`: Get the Id of vertices
        - `v.data`: Get the data of vertices (If there is any, means origin data on the graph, not results)

    - The syntax of selector of edge is:
        - `e.src`: Get the source Id of edges
        - `e.dst`: Get the destination Id of edges
        - `e.data`: Get the edge data on the edges (If there is any, means origin data on the graph)

    - The syntax of selector of results is:
        - `r.column_name`: Get the property named `column_name` in results. e.g. `r.hub` in :func:`graphscope.hits`.


LabelVertexPropertyContext
++++++++++++++++++++++++++

:class:`LabeledVertexPropertyContext` is used by algorithms that compatible graphs is a property graph.
i.e. There are several labels in the graph, and each label has several properties.
And results of the algorithms would has the form that each label can have multiple datas (a.k.a. property).

Selection are performed on labels first, then on properties.

We use `:` to filter labels, and `.` to select properties.

And the results can have several properties.

    - The syntax of selector of vertex is:
        - `v:label_name.id`: Get Id that belongs to a specific vertex label.
        - `v:label_name.property_name`: Get data that on a specific property of a specific vertex label.

    - The syntax of selector of edge is:
        - `e:label_name.src`: Get source Id of a specific edge label.
        - `e:label_name.dst`: Get destination Id of a specific edge label.
        - `e:label_name.property_name`: Get data on a specific property of a specific edge label.

    - The syntax of selector of results is:
        - `r:label_name.column_name`: Get the property named `column_name` of `label_name`.


Methods that use selectors
--------------------------

The form of the selector is slightly different in different methods, 

One-dimensional results
+++++++++++++++++++++++

Methods that return one-dimensional results are:

    - `to_numpy`
    - `to_vineyard_tensor`

Selectors is a str, such as:


.. code:: python

    context.to_numpy('v.id')  # VertexDataContext

    context.to_numpy('r')  # VertexDataContext

    context.to_numpy('r:person')  # LabeledVertexDataContext

    context.to_vineyard_tensor('v.person.name')  # VertexPropertyContext

    context.to_vineyard_tensor('r.person.rank')  # LabeledVertexPropertyContext


Multi-dimensional results
+++++++++++++++++++++++++

To retrieve multi-dimensional results, we must assign a key for each selector.
Also, Each Key must be unique for a given selector.

Methods that will return multi-dimensional results are:

    - `to_dataframe`
    - `to_vineyard_dataframe`
    - `output`
    - `output_to_client`

Selector is a dict, such as:

.. code:: python

    context.to_dataframe({'id': 'v.id', 'rank': 'r'})  # VertexDataContext

    context.to_vineyard_dataframe({'id': 'v.id', 'hub': 'r:person'})  # LabeledVertexDataContext

    context.output(path, {'person_id': 'v.person.id', 'age': 'v.hub'})  # VertexPropertyContext

    context.output_to_client(path, {'person_id': 'v.person.id', 'res': 'r.person.res'})  # LabeledVertexPropertyContext


Vertex Range
------------

Optional, user may also want to retrieve only a subset of data specified by selector.
Those method above can take a optional `vertex_range` argument, which works like slicing,
user can specify a `begin` and an `end` value, then only vertex inside the range will be selected.

Examples:

.. code:: python

    context.to_dataframe(selector={'id': 'v.id', 'rank': 'r'}, vertex_range={'begin': '200', 'end': '400'})


.. note::

    Values of vertex_range is compared with vertex ID, in alphabetical order, not numeric order.
