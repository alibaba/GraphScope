.. _operation:

Operation
=========

Operation object
----------------

.. currentmodule:: graphscope.framework.operation
.. autoclass:: Operation

.. autosummary::
   :toctree: generated/

   Operation.__init__
   Operation.as_op_def
   Operation.key
   Operation.signature
   Operation.eval
   Operation.set_output
   Operation.evaluated

BuiltIn operations
------------------

.. automodule:: graphscope.framework.dag_utils
.. autosummary::
   :toctree: generated/

   create_app
   create_graph
   dynamic_to_arrow
   arrow_to_dynamic
   modify_edges
   modify_vertices
   run_app
   report_graph
   project_arrow_property_graph
   project_dynamic_property_graph
   unload_app
   unload_graph
   context_to_numpy
   context_to_dataframe
   to_vineyard_tensor
   add_column
   graph_to_numpy
   graph_to_dataframe
