.. graphscope documentation master file, created by
   sphinx-quickstart on Tue Aug 27 10:19:05 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

GraphScope: A One-Stop Large-Scale Graph Computing System from Alibaba
======================================================================

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
   tutorials
   deployment
   loading_graph
   graph_transformation
   analytics_engine
   interactive_engine
   learning_engine
   frequently_asked_questions
   developer_guide

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

