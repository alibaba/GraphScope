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

   overview/intro
   overview/getting_started
   overview/graph_analytics_workloads
   overview/graph_interactive_workloads
   overview/graph_learning_workloads
   overview/design_of_graphscope
   overview/positioning
   overview/glossary
   release_notes
   frequently_asked_questions

.. toctree::
   :maxdepth: 2
   :caption: Installation & Deployment

   deployment/install_on_local
   deployment/deploy_graphscope_on_self_managed_k8s
   deployment/deploy_graphscope_on_existing_vineyard_cluster
   deployment/deploy_graphscope_on_clouds
   deployment/deploy_graphscope_with_helm

.. toctree::
   :maxdepth: 2
   :caption: Graph Analytical Engine

   analytical_engine/getting_started
   analytical_engine/deployment
   analytical_engine/design_of_gae
   analytical_engine/guide_and_examples
   analytical_engine/dev_and_test
   analytical_engine/apis
   analytical_engine/faqs


.. toctree::
   :maxdepth: 2
   :caption: Graph Interactive Engine

   interactive_engine/getting_started
   interactive_engine/deployment
   interactive_engine/guide_and_examples
   interactive_engine/design_of_gie
   interactive_engine/supported_gremlin_steps
   interactive_engine/faq.md

.. toctree::
   :maxdepth: 2
   :caption: Graph Learning Engine

   learning_engine/getting_started
   learning_engine/design_of_gle
   learning_engine/graph_sampling
   learning_engine/guide_and_examples
   learning_engine/dev_and_test

.. toctree::
   :maxdepth: 2
   :caption: Storage Engine

   storage_engine/getting_started
   storage_engine/grin
   storage_engine/groot
   storage_engine/vineyard
   storage_engine/graphar
   storage_engine/gart

.. toctree::
   :maxdepth: 2
   :caption: troubleshooting & utilities

   utilities/how_to_find_logs
   utilities/monitoring
   utilities/gs
   utilities/error_codes

.. toctree::
   :maxdepth: 2
   :caption: Development

   development/dev_guide
   development/code_style_guide
   development/how_to_test
   development/how_to_contribute
   development/how_to_debugging
   development/how_to_install_in_offline_mode

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

