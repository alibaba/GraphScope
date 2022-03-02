.. _functions:

*********
Functions
*********

.. automodule:: graphscope.nx.classes.function

Graph
-----
.. autosummary::
   :toctree: generated/

   degree
   degree_histogram
   density
   info
   create_empty_copy
   is_directed
   to_directed
   to_undirected
   is_empty
   add_star
   add_path
   add_cycle
   subgraph
   induced_subgraph
   edge_subgraph


Nodes
-----
.. autosummary::
   :toctree: generated/

   nodes
   number_of_nodes
   neighbors
   all_neighbors
   non_neighbors
   common_neighbors


Edges
-----
.. autosummary::
   :toctree: generated/

   edges
   number_of_edges
   density
   non_edges

Self loops
----------
.. autosummary::
   :toctree: generated/

   selfloop_edges
   number_of_selfloops
   nodes_with_selfloops

Attributes
----------
.. autosummary::
   :toctree: generated/

   is_weighted
   is_negatively_weighted


Freezing graph structure
------------------------
.. autosummary::
   :toctree: generated/

   freeze
   is_frozen