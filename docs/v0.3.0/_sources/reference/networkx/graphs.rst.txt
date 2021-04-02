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

Adding and removing nodes and edges

.. autosummary::
   :toctree: generated/

   Graph.__init__
   Graph.add_node
   Graph.add_nodes_from
   Graph.remove_node
   Graph.remove_nodes_from
   Graph.add_edge
   Graph.add_edges_from
   Graph.add_weighted_edges_from
   Graph.remove_edge
   Graph.remove_edges_from
   Graph.update
   Graph.clear

Reporting nodes edges and neighbors

.. autosummary::
   :toctree: generated/

   Graph.nodes
   Graph.__iter__
   Graph.has_node
   Graph.get_node_data
   Graph.__contains__
   Graph.edges
   Graph.has_edge
   Graph.get_edge_data
   Graph.neighbors
   Graph.adj
   Graph.__getitem__
   Graph.adjacency
   Graph.nbunch_iter

Counting nodes edges and neighbors

.. autosummary::
   :toctree: generated/

   Graph.order
   Graph.number_of_nodes
   Graph.__len__
   Graph.degree
   Graph.size
   Graph.number_of_edges


Making copies and subgraphs

.. autosummary::
   :toctree: generated/

   Graph.copy
   Graph.to_undirected
   Graph.to_directed
   Graph.subgraph
   Graph.edge_subgraph

Project simple graph

.. autosummary::
    :toctree: generated/

    Graph.project_to_simple

DiGraph
^^^^^^^
Directed graphs with self loops

.. autoclass:: DiGraph

Adding and removing nodes and edges

.. autosummary::
   :toctree: generated/

   DiGraph.__init__
   DiGraph.add_node
   DiGraph.add_nodes_from
   DiGraph.remove_node
   DiGraph.remove_nodes_from
   DiGraph.add_edge
   DiGraph.add_edges_from
   DiGraph.add_weighted_edges_from
   DiGraph.remove_edge
   DiGraph.remove_edges_from
   DiGraph.update
   DiGraph.clear

Reporting nodes edges and neighbors

.. autosummary::
   :toctree: generated/

   DiGraph.nodes
   DiGraph.__iter__
   DiGraph.has_node
   DiGraph.__contains__
   DiGraph.edges
   DiGraph.out_edges
   DiGraph.in_edges
   DiGraph.has_edge
   DiGraph.get_edge_data
   DiGraph.neighbors
   DiGraph.adj
   DiGraph.__getitem__
   DiGraph.successors
   DiGraph.succ
   DiGraph.predecessors
   DiGraph.pred
   DiGraph.adjacency
   DiGraph.nbunch_iter

Counting nodes edges and neighbors

.. autosummary::
   :toctree: generated/

   DiGraph.order
   DiGraph.number_of_nodes
   DiGraph.__len__
   DiGraph.degree
   DiGraph.in_degree
   DiGraph.out_degree
   DiGraph.size
   DiGraph.number_of_edges

Making copies and subgraphs

.. autosummary::
   :toctree: generated/

   DiGraph.copy
   DiGraph.to_undirected
   DiGraph.to_directed
   DiGraph.subgraph
   DiGraph.edge_subgraph
   DiGraph.reverse

Project simple graph

.. autosummary::
    :toctree: generated/

    DiGraph.project_to_simple

Reading graphs
*************************

Edge List
^^^^^^^^^
.. autofunction:: graphscope.experimental.nx.read_edgelist

Adjacency List
^^^^^^^^^^^^^^
.. autofunction:: graphscope.experimental.nx.read_adjlist


Algorithms on mutable graphs
****************************

.. autofunction:: graphscope.experimental.nx.Graph.project_to_simple
.. autofunction:: graphscope.experimental.nx.builtin.average_neighbor_degree
.. autofunction:: graphscope.experimental.nx.builtin.pagerank
.. autofunction:: graphscope.experimental.nx.builtin.hits
.. autofunction:: graphscope.experimental.nx.builtin.degree_centrality
.. autofunction:: graphscope.experimental.nx.builtin.in_degree_centrality
.. autofunction:: graphscope.experimental.nx.builtin.out_degree_centrality
.. autofunction:: graphscope.experimental.nx.builtin.eigenvector_centrality
.. autofunction:: graphscope.experimental.nx.builtin.katz_centrality
.. autofunction:: graphscope.experimental.nx.builtin.has_path
.. autofunction:: graphscope.experimental.nx.builtin.shortest_path
.. autofunction:: graphscope.experimental.nx.builtin.single_source_dijkstra_path_length
.. autofunction:: graphscope.experimental.nx.builtin.average_shortest_path_length
.. autofunction:: graphscope.experimental.nx.builtin.bfs_edges
.. autofunction:: graphscope.experimental.nx.builtin.bfs_predecessors
.. autofunction:: graphscope.experimental.nx.builtin.bfs_successors
.. autofunction:: graphscope.experimental.nx.builtin.k_core
.. autofunction:: graphscope.experimental.nx.builtin.clustering
.. autofunction:: graphscope.experimental.nx.builtin.triangles
.. autofunction:: graphscope.experimental.nx.builtin.transitivity
.. autofunction:: graphscope.experimental.nx.builtin.average_clustering

.. include:: cython_sdk.rst
