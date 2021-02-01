.. graphscope documentation master file, created by
   sphinx-quickstart on Tue Aug 27 10:19:05 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GraphScope: A One-Stop Large-Scale Graph Computing System from Alibaba
==================================================

GraphScope is a unified distributed graph computing platform 
that provides a one-stop environment for performing diverse graph 
operations on a cluster of computers through a user-friendly 
Python interface. GraphScope makes multi-staged processing of 
large-scale graph data on compute clusters simple by combining 
several important pieces of Alibaba technology: including GRAPE, 
GraphCompute, and Graph-Learn (GL) for analytics, interactive, 
and graph neural networks (GNN) computation, respectively, 
and the vineyard store that offers efficient in-memory data transfers.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   installation
   getting_started
   deployment
   loading_graph
   interactive_engine
   analytics_engine
   learning_engine
   developer_guide

.. toctree::
   :maxdepth: 2
   :caption: Tutorials

   How to Create a Session <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/1_how_to_launch_a_session.ipynb>
   Loading Graphs <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/2_loading_graphs.ipynb>
   Built-in Analytical Algorithms <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/3_builtin_analytical_algorithms.ipynb>
   Writing Your Own Algorithms <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/4_writing_your_own_algorithms.ipynb>
   Interactive Query with Gremlin <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/5_interactive_query_with_gremlin.ipynb>
   Unsupervised Learning with GraphSage <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/6_unsupervised_learning_with_graphsage.ipynb>
   Supervised Learning with GCN <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/7_supervised_learning_with_gcn.ipynb>
   A Complex Workflow: Node Classification on Citation Network <https://nbviewer.jupyter.org/github/alibaba/GraphScope/blob/main/tutorials/8_node_classification_on_citation_network.ipynb>

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/python_index
   reference/analytical_engine_index

.. toctree::
   :maxdepth: 2
   :hidden:

   zh/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

