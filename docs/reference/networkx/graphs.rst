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

Graph
^^^^^^^
Undirected graphs with self loops

.. currentmodule:: graphscope.experimental.nx
.. autoclass:: Graph
   :special-members:
   :members:
   :exclude-members: __repr__, __copy__, __deepcopy__, __str__, __weakref__


DiGraph
^^^^^^^
Directed graphs with self loops

.. currentmodule:: graphscope.experimental.nx
.. autoclass:: DiGraph
   :special-members:
   :inherited-members:
   :members:
   :exclude-members: __repr__, __copy__, __deepcopy__, __str__, __weakref__


.. include:: cython_sdk.rst
