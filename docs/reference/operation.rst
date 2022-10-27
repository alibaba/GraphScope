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
   Operation.evaluated

BuiltIn operations
------------------

.. automodule:: graphscope.framework.dag_utils
.. autosummary::
   :toctree: generated/

   create_app
   bind_app
   run_app
   create_graph
   create_loader
   add_labels_to_graph
   dynamic_to_arrow
   arrow_to_dynamic
   modify_edges
   modify_vertices
   report_graph
   project_arrow_property_graph
   project_to_simple
   copy_graph
   to_directed
   to_undirected
   create_graph_view
   clear_graph
   clear_edges
   create_subgraph
   create_unload_op
   unload_app
   unload_graph
   unload_context
   context_to_numpy
   context_to_dataframe
   to_vineyard_tensor
   to_vineyard_dataframe
   to_data_sink
   output
   get_context_data
   add_column
   graph_to_numpy
   graph_to_dataframe
   gremlin_query
   gremlin_to_subgraph
   fetch_gremlin_result
