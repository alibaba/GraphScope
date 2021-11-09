.. _transformation:

Graph Transformation
^^^^^^^^^^^^^^

In GraphScope, :class:`nx.Graph` or :class:`nx.DiGraph` can be created from GraphScope graph object.
We implement a copy-on-write transformation strategy for creating from GraphScope graph.
That is, when init a :class:`nx.Graph` or :class:`nx.DiGraph` object with GraphScope graph object,
we do not convert arrow property graph to dynamic property graph immediately，
but just host the arrow property graph in the :class:`nx.Graph` or :class:`nx.DiGraph` object.
And the graph can report graph info and run algorithms as expected.
When the graph comes to modification operation, the arrow property graph would convert
to dynamic property graph, that is the `copy-on-write` transformation strategy.
Here is an example to show the `copy-on-write` transformation.

Note: :class:`nx.Graph` and :class:`nx.DiGraph` not support to be created from a GraphScope multigraph
(which contains parallel edge). If input graph is multigraph, it would raise a
`NetworkXError`.

.. code:: python

    >>> # first create a labeled graphscope graph
    >>> graph = graphscope.g()
    >>> graph.add_vertices("v_0.csv", label="v0")
    >>> graph.add_vertices("v_1.csv", label="v1")
    >>> graph.add_edge("e.csv", label="e", src_label="v0", dst_label="v1")

    >>> # then we init a nx.Graph with graph, here we set v0 as default vertex label
    >>> G = nx.Graph(graph, default_label="v0")  # G actually hosts a arrow property graph

    >>> # we can report info of G or run algorithm without converting to dynamic property graph
    >>> print(G.nodes)
    [0, ("v1", 1)]
    >>> nx.builtin.pagerank(G)

    >>> # when comes to the modification, hosted arrow property graph convert to dynamic property graph
    >>> G.add_node(1)
    >>> print(G.nodes)
    [0, 1, ("v1", 1)]


.. autofunction:: graphscope.nx.Graph.__init__
.. autofunction:: graphscope.nx.Graph._init_with_arrow_property_graph
.. autofunction:: graphscope.nx.Graph._convert_arrow_to_dynamic

.. include:: cython_sdk.rst
