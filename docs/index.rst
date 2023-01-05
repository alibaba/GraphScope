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
   :caption: What is GraphScope

   overview
   key_features
   graphscope_for_data_scientists
   graphscope_for_traversal_queries
   graphscope_for_graph_analytics
   graphscope_for_learning
   performance_and_benchmark
   graphscope_positioning
   glossary
   release_notes
   frequently_asked_questions

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   python_tutorials
   java_tutorials
   cpp_tutorials
   walkthrough_examples_acrossing_engines
   video_tutorials

.. toctree::
   :maxdepth: 2
   :caption: Deployment and Usage 

   installation
   deploy_on_self_managed_k8s
   deploy_on_clouds
   deploy_with_helm
   deploy_as_job_for_analytical_tasks
   deploy_as_service_with_groot

.. toctree::
   :maxdepth: 2
   :caption: Operations & Observability

   how_to_find_logs
   monitoring
   error_codes
   how_to_report_bug

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   dive_into_engines
   storages_and_unified_interface
   performance_tuning

.. toctree::
   :maxdepth: 2
   :caption: Development

   env_preparation
   code_style_guide
   algorithm_development
   test_code
   submit_pr

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

