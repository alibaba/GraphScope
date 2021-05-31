.. _graphs:

Graph types
------------------

+----------------+------------+--------------------+------------------------+
|    Class       | Type       | Self-loops allowed | Parallel edges allowed |
+================+============+====================+========================+
| Graph          | undirected | Yes                | No                     |
+----------------+------------+--------------------+------------------------+
| DiGraph        | directed   | Yes                | No                     |
+----------------+------------+--------------------+------------------------+

Notice that graphscope.nx not support MultiGraph and MultiDiGraph.

Graph
^^^^^^^
Undirected graphs with self loops

.. currentmodule:: graphscope.nx
.. autoclass:: Graph
   :special-members:
   :members:
   :exclude-members: __repr__, __copy__, __deepcopy__, __str__, __weakref__


DiGraph
^^^^^^^
Directed graphs with self loops

.. currentmodule:: graphscope.nx
.. autoclass:: DiGraph
   :special-members:
   :inherited-members:
   :members:
   :exclude-members: __repr__, __copy__, __deepcopy__, __str__, __weakref__


transformation
^^^^^^^^^^^^^^

In GraphScope, the immutable graphscope.Graph and the mutable graphscope.nx.Graph
can be transformed into each other. We define the transformation in each Graph class
constructors.

.. autofunction:: graphscope.framework.graph.Graph.__init__
.. autofunction:: graphscope.nx.Graph.__init__
.. autofunction:: graphscope.nx.convert.from_gs_graph




.. include:: cython_sdk.rst
