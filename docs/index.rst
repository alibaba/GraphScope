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
   getting_started_overview
   graph_analytics_workloads
   graph_interactive_workloads
   graph_learning_workloads
   design_of_graphscope
   glossary
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
   :caption: Tutorials

   how_to_load_graphs
   try_graphscope_on_jupyter_notebook
   how_to_run_built_in_apps
   how_to_develop_customized_apps

.. toctree::
   :maxdepth: 2
   :caption: Graph Analytics Engine

   getting_started_gae
   design_of_gae
   user_guide_and_examples_of_gae
   apis_of_gae
   faqs_of_gae

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
   :caption: Graph Storage

   unified_interface
   graph_formats


.. toctree::
   :maxdepth: 2
   :caption: Monitoring & Debugging

   how_to_find_logs
   monitoring
   error_codes

.. toctree::
   :maxdepth: 2
   :caption: Development

   development_dnvironment_preparation
   code_style_guide
   algorithm_development
   how_to_test
   how_to_submit_pr
   how_to_report_bugs

.. toctree::
   :maxdepth: 2
   :caption: Utilities

   bulk_loader
   resources_estimator


.. toctree::
   :maxdepth: 2
   :caption: Community

   how_to_contribute
   contact_us

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

