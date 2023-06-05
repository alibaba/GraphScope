.. _context:

Context
==============

Context object
--------------
.. currentmodule:: graphscope.framework.context

.. autoclass:: BaseContextDAGNode
   :special-members:
   :members: to_numpy, to_dataframe, to_vineyard_tensor, to_vineyard_dataframe

.. autoclass:: TensorContextDAGNode

.. autoclass:: VertexDataContextDAGNode

.. autoclass:: LabeledVertexDataContextDAGNode

.. autoclass:: VertexPropertyContextDAGNode

.. autoclass:: LabeledVertexPropertyContextDAGNode

.. autoclass:: Context

.. autoclass:: DynamicVertexDataContext

.. autoclass:: ResultDAGNode
