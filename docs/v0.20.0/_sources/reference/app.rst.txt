.. _app:

Analytical App
==============

AppAssets
----------

.. currentmodule:: graphscope
.. autoclass:: graphscope.framework.app.AppAssets
   :special-members:
   :members:
   :exclude-members: __call__, __repr__

JavaApp
----------

.. currentmodule:: graphscope
.. autoclass:: graphscope.analytical.app.JavaApp
   :special-members:
   :members:
   :exclude-members: __repr__


App object
----------

.. currentmodule:: graphscope
.. autoclass:: graphscope.framework.app.AppDAGNode
   :members: unload

.. autoclass:: graphscope.framework.app.App
   :special-members:
   :members:
   :exclude-members: __repr__, __call__, _query, __weakref__

Functions
---------
.. autosummary::
   :toctree: generated/

   graphscope.framework.app.load_app


BuiltIn apps
------------

.. autofunction:: graphscope.attribute_assortativity_coefficient
.. autofunction:: graphscope.numeric_assortativity_coefficient
.. autofunction:: graphscope.average_degree_connectivity
.. autofunction:: graphscope.average_shortest_path_length
.. autofunction:: graphscope.bfs
.. autofunction:: graphscope.avg_clustering
.. autofunction:: graphscope.clustering
.. autofunction:: graphscope.degree_assortativity_coefficient
.. autofunction:: graphscope.degree_centrality
.. autofunction:: graphscope.eigenvector_centrality
.. autofunction:: graphscope.hits
.. autofunction:: graphscope.is_simple_path
.. autofunction:: graphscope.k_core
.. autofunction:: graphscope.k_shell
.. autofunction:: graphscope.katz_centrality
.. autofunction:: graphscope.louvain
.. autofunction:: graphscope.cdlp
.. autofunction:: graphscope.lpa
.. autofunction:: graphscope.lpa_u2i
.. autofunction:: graphscope.pagerank
.. autofunction:: graphscope.pagerank_nx
.. autofunction:: graphscope.sssp
.. autofunction:: graphscope.triangles
.. autofunction:: graphscope.voterank
.. autofunction:: graphscope.wcc
