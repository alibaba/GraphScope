.. _graph_transformation:

Graph Transformations
=====================

We introduce a series of method that can append more labels to a existed grpah, and
do projection over existed graph. We will also show how to make a complex property graph
compatible with algorithms that can only run on simple graph. Finally, we show how to add
the query result of algorithm back to graph as a property on vertex.

More specically, :class:`Graph` provides two methods for append labels, and one method for
projection.


.. code:: python

    def add_vertices(self, vertices, label="_", properties=[], vid_field=0):
        pass

    def add_edges(self, edges, label="_", properties=[], src_label=None, dst_label=None, src_field=0, dst_field=1):
        pass

    def project(self, vertices, edges):
        pass


We have already seem `add_vertices` and `add_edges` in :ref:`loading graphs<loading_graphs>`, we use them
to build a graph iteratively.

Further, we can use them to attach more vertex labels and edge labels to a existed graph.
But this won't modify the source graph, instead, it will return a new graph, which is based
on the source graph.


Attach new labels
-----------------

Take LDBC-SNB Property Graph as an example，We now load a subset of labels, as the source graph.


.. code:: python

    import graphscope
    from pathlib import Path
    from graphscope.framework.loader import Loader

    sess = graphscope.session()

    graph = sess.g(directed=directed)
    graph = graph.add_vertices(Loader("person_0_0.csv", delimiter="|"), "person")
    graph = graph.add_edges(Loader("person_knows_person_0_0.csv", delimiter="|"),
                "knows", src_label="person", dst_label="person"
        )

    # graph has 1 vertex label "person"
    print(graph.schema)

Now we have an loaded graph, let's attach some new labels to it.

.. code:: python

    graph1 = graph.add_vertices(Loader("comment_0_0.csv", delimiter="|"), "comment")

    # Now graph1 has 2 vertex labels "person" and "comment"
    print(graph1.schema)

    graph2 = graph1.add_edges(Loader("comment_replyOf_comment_0_0.csv", delimiter="|"),
                "replyOf", src_label="comment", dst_label="comment"
        )

    # graph2 has 2 edge labels "knows" and "replyOf"
    print(graph2.schema)

We can see each operation of `add` will produce a new graph.
In implementation detail, their common labels will share the common memory, so it won't
copy the source graph.


Projection
----------

In some scenario, we need to extract a subgraph from a complex graph. We do that by `project`.


.. code:: python

    def project(
            self,
            vertices: Mapping[str, Union[List[str], None]],
            edges: Union[Mapping[str, Union[List[str], None]], None]
        ):
        pass


The parameter definition means it's a `dict`, the key is the label name, the value is a `list` of `str`, which is the name of properties. Specifically, if the value is `None`, it means select all properties.

A graph that produced by `project` should just like a normal property graph, and can be projected further.

Here's some examples.

.. code:: python

    sub_graph = graph2.project(vertices={"person": ["firstName", "lastName"]}, edges={"knows": None})

    # contains 1 vertex label "person", and 1 edge label "knows", with selected properties.
    print(sub_graph.schema)

    sub_graph2 = sub_graph.project(vertices={"person": []}, edges={"knows": ["creationDate"]})

    # No properties on the vertex, and 1 property on the edge.
    print(sub_graph2.schema)



Transform to simple graph implicitly
------------------------------------

When an algorithm that only works on simple graph query a property graph, the property graph will
be converted to a simple graph implicitly. If such transformation cannot be performed (the vertex label num and
edge label num is not one, or has more than 1 property on vertex/edge), an exception will be raised.

.. code:: python

    from graphscope import wcc

    ret = wcc(sub_graph2)

    # wcc(graph2)  # Error! More than 1 vertex label / edge label
    # wcc(sub_graph)  # Error! More than 1 property.


Add results back to graph as a property
---------------------------------------

The result `ret` produced in previous step can be add to a graph as a property of vertex.

Note the result can not only be added to the graph it directly queried on, but also the graph which produced
the queried graph by `project`, as long as the vertex label that will be mutated is the same between the two graphs.

.. code:: python

    new_graph = sub_graph2.add_column(ret, selector={'cc': 'r'})

    new_graph = sub_graph.add_column(ret, selector={'cc': 'r'})

    new_graph = graph.add_column(ret, selector={'cc': 'r'})



