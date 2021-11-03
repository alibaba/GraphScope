.. _transformation:

Graph transformation
^^^^^^^^^^^^^^

In GraphScope, `nx.Graph` or `nx.DiGraph` can be created from immutable graphscope graph.
We implement a copy-on-write transformation strategy for creating from graphscope
graph. That is, when init `nx.Graph` or `nx.DiGraph` with `gs.Graph`, we do not convert
arrow property graph to dynamic property graph immediately，but host arrow property graph
object in the `nx.Graph` or `nx.DiGraph` object. When the Graph comes to modification
operation, the arrow property graph would convert to dynamic property graph, that is
we call `copy-on-write` transformation strategy. Here is an example to show the
`copy-on-write` transformation.

.. code:: python

    >>>
    >>> # first create a labeled graphscope graph
    >>> graph = graphscope.g()
    >>> graph.add_vertices("v_0.csv", label="v0")
    >>> graph.add_vertices("v_1.csv", label="v1")
    >>> graph.add_edge("e.csv", label="e", src_label="v0", dst_label="v1")
    >>> # then we init a nx.Graph with graph, here we set v0 as default vertex label
    >>> G = nx.Graph(graph, default_label="v0")  # G actually host a arrow property graph
    >>> # we can report meta of G or run algorithm without converting to dynamic property graph
    >>> print(G.nodes)
    >>> nx.builtin.pagerank(G)
    >>> # when comes to the modification, hosted arrow property graph convert to dynamic property graph
    >>> G.add_node(0)
    >>> print(G.nodes)


.. autofunction:: graphscope.nx.Graph.__init__
.. autofunction:: graphscope.nx.Graph._init_with_arrow_property_graph
.. autofunction:: graphscope.nx.Graph._convert_arrow_to_dynamic

.. include:: cython_sdk.rst
