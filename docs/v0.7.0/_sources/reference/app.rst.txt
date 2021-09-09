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

.. autofunction:: graphscope.bfs
.. autofunction:: graphscope.pagerank
.. autofunction:: graphscope.sssp
.. autofunction:: graphscope.wcc
.. autofunction:: graphscope.cdlp
.. autofunction:: graphscope.clustering
.. autofunction:: graphscope.degree_centrality
.. autofunction:: graphscope.eigenvector_centrality
.. autofunction:: graphscope.hits
.. autofunction:: graphscope.k_core
.. autofunction:: graphscope.katz_centrality
.. autofunction:: graphscope.lpa
.. autofunction:: graphscope.triangles
.. autofunction:: graphscope.louvain

