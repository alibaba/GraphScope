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
   :caption: Overview
   
   graphscope_positioning
   overview/getting_started
   overview/graph_analytics_workloads
   overview/graph_interactive_workloads
   overview/graph_learning_workloads
   overview/design_of_graphscope
   overview/glossary
   release_notes
   frequently_asked_questions

.. toctree::
   :maxdepth: 2
   :caption: Installation & Deployment

   deploy_graphscope_on_local
   deploy_graphscope_on_self_managed_k8s
   deploy_graphscope_on_clouds
   deploy_graphscope_with_helm

.. toctree::
   :maxdepth: 2
   :caption: Graph Analytical Engine
   
   analytical_engine/getting_started
   analytical_engine/deployment
   analytical_engine/design_of_gae
   analytical_engine/guide_and_examples
   analytical_engine/apis
   analytical_engine/faqs
   

.. toctree::
   :maxdepth: 2
   :caption: Graph Interactive Engine

   getting_started_gie
   design_of_gie
   user_guide_and_examples_of_gie
   faqs_of_gie

.. toctree::
   :maxdepth: 2
   :caption: Graph Learning Engine

   getting_started_gle
   design_of_gle
   user_guide_and_examples_of_gie
   apis_of_gle
   faqs_of_gle

.. toctree::
   :maxdepth: 2
   :caption: Storage Engine

   storage_engine/getting_started
   storage_engine/grin
   storage_engine/graph_formats


.. toctree::
   :maxdepth: 2
   :caption: troubleshooting & utilities

   utilities/how_to_find_logs
   utilities/monitoring
   utilities/error_codes
   utilities/bulk_loader

.. toctree::
   :maxdepth: 2
   :caption: Development

   development/dev_environment
   development/code_style_guide
   development/how_to_test
   development/how_to_contribute

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/python_index
   reference/analytical_engine_index
   Analytical Engine Java API Reference <reference/gae_java/index>

.. toctree::
   :maxdepth: 2
   :hidden:

   zh/index

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

