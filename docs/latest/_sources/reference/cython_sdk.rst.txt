.. _cython_sdk:

Cython SDK API
==============

Pregel
------

.. py:module:: graphscope.analytical.app.pregel
   :noindex:
.. py:class:: Vertex[VD_TYPE, MD_TYPE]
   :noindex:

    Class which holds vertex id, data and edges.

    VD_TYPE: vertex data type
    MD_TYPE: message type

    .. py:method:: Vertex.id() -> str
       :noindex:

        Get vertex id.

    .. py:method:: Vertex.label() -> str
       :noindex:

        Get vertex label.

    .. py:method:: Vertex.label_id() -> int
       :noindex:

        Get vertex label id.

    .. py:method:: Vertex.properties() -> vector[pair[str, str]]
       :noindex:

        Get list of vertex properties, which consisting of property name and property type.
        Note that property type is one of ("INT", "DOUBLE", "STRING")

    .. py:method:: Vertex.set_value(value: VD_TYPE)
       :noindex:

        Set the vertex data, immediately visible in current round.

    .. py:method:: Vertex.value() -> VD_TYPE
       :noindex:

        Get the vertex value, which data stored with vertex.

    .. py:method:: Vertex.get_str(property_id: int) -> str
       :noindex:

        Get vertex str data by property id.

    .. py:method:: Vertex.get_str(property_name: str) -> str
       :noindex:

        Get vertex str data by property name.

    .. py:method:: Vertex.get_double(property_id: int) -> double
       :noindex:

        Get vertex str data by property id.

    .. py:method:: Vertex.get_double(property_name: str) -> double
       :noindex:

        Get vertex double data by property_name.

    .. py:method:: Vertex.get_int(property_id: int) -> str
       :noindex:

        Get vertex int data by property id.

    .. py:method:: Vertex.get_int(property_name: str) -> str
       :noindex:

        Get vertex int data by property_name.

    .. py:method:: Vertex.outgoing_edges(edge_label_id: int) -> AdjList[VD_TYPE, MD_TYPE]
       :noindex:

        Get a iterable of outgoing edges by edge label id of this vertex.

    .. py:method:: Vertex.outgoing_edges(edge_label_name: str) -> AdjList[VD_TYPE, MD_TYPE]
       :noindex:

        Get a iterable of outgoing edges by edge label name of this vertex.

    .. py:method:: Vertex.incoming_edges(edge_label_id: int) -> AdjList[VD_TYPE, MD_TYPE]
       :noindex:

        Get a iterable of incoming edges by edge label id of this vertex.

    .. py:method:: Vertex.incoming_edges(edge_label_name: str) -> AdjList[VD_TYPE, MD_TYPE]
       :noindex:

        Get a iterable of incoming edges by edge label name of this vertex.

    .. py:method:: Vertex.send(v: Vertex[VD_TYPE, MD_TYPE], msg: MD_TYPE)
       :noindex:

        Send a message to target vertex.

    .. py:method:: Vertex.vote_to_halt()
       :noindex:

        After this method is called, the `compute()` code will no longer called with this
        vertex util receive messages. The application will finish only when all vertices
        vote to halt.


.. py:class:: Neighbor[VD_TYPE, MD_TYPE]
   :noindex:

    .. py:method:: Neighbor.vertex() -> Vertex[VD_TYPE, MD_TYPE]
       :noindex:

        Get neighbor vertex.


    .. py:method:: Neighbor.get_str(column: int) -> str
       :noindex:

        Get edge str data by column id.

    .. py:method:: Neighbor.get_int(column: int) -> str
       :noindex:

        Get edge int data by column id.

    .. py:method:: Neighbor.get_double(column: int) -> str
       :noindex:

        Get edge double data by column id.

.. py:class:: AdjList[VD_TYPE, MD_TYPE]
   :noindex:

    .. py:method:: AdjList.begin() -> Neighbor
       :noindex:

        Return begin addr of the iterator.

    .. py:method:: AdjList.end() -> Neighbor
       :noindex:

        Return end addr of the iterator.

    .. py:method:: AdjList.size() -> size_t
       :noindex:

        Get the size of adj list.


.. py:class:: Context[VD_TYPE, MD_TYPE]
   :noindex:

    Class which holds current step, aggregator info, query args and other util function.

    .. py:method:: Context.get_config(key: str) -> str
       :noindex:

        Get a value with specific key, or "" with key not exist.

    .. py:method:: Context.register_aggregator(name: str, type: PregelAggregatorType)
       :noindex:

        Register a aggregator with specific type, naming by `name`.

    .. py:method:: Context.aggregate[AGGR_TYPE](name: str, value: AGGR_TYPE)
       :noindex:

        Add a new value to aggregator.

    .. py:method:: Context.get_aggregated_value[AGGR_TYPE](name: str) -> AGGR_TYPE
       :noindex:

        Get value from a aggregator.

    .. py:method:: Context.superstep() -> str
       :noindex:

        Get current superstep, begin with 0.

    .. py:method:: Context.get_total_vertices_num() -> size_t
       :noindex:

        Get total vertex number.

    .. py:method:: Context.vertex_label_num() -> int
       :noindex:

        Get vertex label number.

    .. py:method:: Context.edge_label_num() -> int
       :noindex:

        Get edge label number.

    .. py:method:: Context.vertex_property_num(vertex_label_name: str) -> int
       :noindex:

        Get vertex property number by vertex label name.

    .. py:method:: Context.vertex_property_num(vertex_label_id: int) -> int
       :noindex:

        Get vertex property number by vertex label id.

    .. py:method:: Context.edge_property_num(edge_label_name: str) -> int
       :noindex:

        Get edge property number by edge label name.

    .. py:method:: Context.edge_property_num(edge_label_id: int) -> int
       :noindex:

        Get vertex property number by edge label id.

    .. py:method:: Context.vertex_labels() -> vector[str]
       :noindex:

        Get list of vertex label.

    .. py:method:: Context.edge_labels() -> vector[str]
       :noindex:

        Get list of edge label.

    .. py:method:: Context.get_vertex_label_by_id(vertex_label_id: int) -> str
       :noindex:

        Get vertex label name by label id.

    .. py:method:: Context.get_vertex_label_id_by_name(vertex_label_name: str) -> int
       :noindex:

        Get vertex label id by name.

    .. py:method:: Context.get_edge_label_by_id(edge_label_id: int) -> str
       :noindex:

        Get edge label name by label id.

    .. py:method:: Context.get_edge_label_id_by_name(edge_label_name: str) -> int
       :noindex:

        Get edge label id by name.

    .. py:method:: Context.vertex_properties(vertex_label_id: int) -> vector[pair[str, str]]
       :noindex:

        Get list of vertex properties by label id.

    .. py:method:: Context.vertex_properties(vertex_label_name: str) -> vector[pair[str, str]]
       :noindex:

        Get list of vertex properties by label name.

    .. py:method:: Context.edge_properties(edge_label_id: int) -> vector[pair[str, str]]
       :noindex:

        Get list of edge properties by label id.

    .. py:method:: Context.edge_properties(edge_label_name: str) -> vector[pair[str, str]]
       :noindex:

        Get list of edge properties by label name.

    .. py:method:: Context.get_vertex_property_id_by_name(vertex_label_name: str, vertex_property_name: str) -> int
       :noindex:

        Get vertex property id by property name.

    .. py:method:: Context.get_vertex_property_id_by_name(vertex_label_id: int, vertex_property_name: str) -> int
       :noindex:

        Get vertex property id by property name.

    .. py:method:: Context.get_vertex_property_by_id(vertex_label_name: str, vertex_property_id: int) -> str
       :noindex:

        Get vertex property name by property id.

    .. py:method:: Context.get_vertex_property_by_id(vertex_label_id: int, vertex_property_id: int) -> int
       :noindex:

        Get vertex property name by property id.

    .. py:method:: Context.get_edge_property_id_by_name(edge_label_name: str, edge_property_name: str) -> int
       :noindex:

        Get edge property id by property name.

    .. py:method:: Context.get_edge_property_id_by_name(edge_label_id: int, edge_property_name: str) -> int
       :noindex:

        Get edge property id by property name.

    .. py:method:: Context.get_edge_property_by_id(edge_label_name: str, edge_property_id: int) -> str
       :noindex:

        Get edge property name by property id.

    .. py:method:: Context.get_edge_property_by_id(edge_label_id: int, edge_property_id: int) -> int
       :noindex:

        Get edge property name by property id.


.. py:class:: MessageIterator[MD_TYPE]
   :noindex:

    .. py:method:: MessageIterator.empty() -> bool
       :noindex:

        Return True if there is no message in queue.

    .. py::method:: MessageIterator.begin() -> MD_TYPE
       :noindex:

        Return begin addr of message iterator.

    .. py::method:: MessageIterator.end() -> MD_TYPE
       :noindex:

        Return end addr of message iterator.


.. py:class:: PregelAggregatorType
   :noindex:

    .. py:data:: kBoolAndAggregator
       :noindex:

         Aggregator for calculating the AND function over boolean values.
         The default value when nothing is aggregated is true.

    .. py:data:: kBoolOrAggregator
       :noindex:

         Aggregator for calculating the OR function over boolean values.
         The default value when nothing is aggregated is false.

    .. py:data:: kBoolOverwriteAggregator
       :noindex:

         Aggregator that stores a value that is overwritten once another value is aggregated.
         Note that, in case multiple vertices write to this aggregator, the behavior is
         non-deterministic. The default value for this aggregator is false.

    .. py:data:: kDoubleMinAggregator
       :noindex:

         Aggregator for getting min double value.

    .. py:data:: kDoubleMaxAggregator
       :noindex:

         Aggregator for getting max double value.

    .. py:data:: kDoubleSumAggregator
       :noindex:

         Aggregator for summing up double values.

    .. py:data:: kDoubleProductAggregator
       :noindex:

         Aggregator for calculating products of double value.

    .. py:data:: kDoubleOverwriteAggregator
       :noindex:

         Aggregator that stores a value that is overwritten once another value is aggregated.
         Note that, in case multiple vertices write to this aggregator, the behavior is
         non-deterministic.

    .. py:data:: kInt64MinAggregator
       :noindex:

         Aggregator for getting min int64 value.

    .. py:data:: kInt64MaxAggregator
       :noindex:

         Aggregator for getting max int64 value.

    .. py:data:: kInt64SumAggregator
       :noindex:

         Aggregator for summing up int64 values.

    .. py:data:: kInt64ProductAggregator
       :noindex:

         Aggregator for calculating products of int64 value.
    .. py:data:: kInt64OverwriteAggregator
       :noindex:

         Aggregator that stores a value that is overwritten once another value is aggregated.
         Note that, in case multiple vertices write to this aggregator, the behavior is
         non-deterministic.

    .. py:data:: kTextAppendAggregator
       :noindex:

         Aggregator with string as its value which keeps appending text to it.


PIE
---

.. py:module:: graphscope.analytical.app.pie
   :noindex:
.. py:class:: MessageStrategy
   :noindex:

   .. py:data:: kAlongOutgoingEdgeToOuterVertex
      :noindex:

         For each of inner vertex, it will send messages to target vertex along outgoing edges.

   .. py:data:: kAlongIncomingEdgeToOuterVertex
      :noindex:

         For each of inner vertex, it will send messages to target vertex along incoming edges.

   .. py:data:: kAlongEdgeToOuterVertex
      :noindex:

         For each of inner vertex, it will send messages to target vertex along both incoming and outgoing edges.

   .. py:data:: kSyncOnOuterVertex
      :noindex:

         For each of outer vertex, it will send messages to fragment which it belongs to (sync message to itself).

.. py:class:: Vertex
   :noindex:

   .. py:method:: Vertex.Vertex()
      :noindex:

        Vertex in graph.


.. py:class:: VertexRange
   :noindex:

   .. py:method:: VertexRange.VertexRange()
      :noindex:

        A range list of vertex, which only contain vertex id.

   .. py:method:: VertexRange.begin() -> Vertex
      :noindex:

        The begin addr of vertex range list.

   .. py:method:: VertexRange.end() -> Vertex
      :noindex:

        The end addr of vertex range list.

   .. py:method:: VertexRange.size() -> int
      :noindex:

        The size of vertex range list.


.. py:class:: VertexArray[T]
   :noindex:

   .. py:method:: VertexArray.VertexArray()
      :noindex:

        A list of vertex, which also contain a vertex data.

   .. py:method:: VertexArray.Init(range: VertexRange)
      :noindex:

        Init VertexArray with default value.

   .. py:method:: VertexArray.Init(range: VertexRange, const T& value)
      :noindex:

        Init VertexArray with specify value.

   .. py:method:: VertexArray.operator[](v: Vertex) -> T
      :noindex:

       Get vertex data.


.. py:class:: Nbr
   :noindex:

   .. py:method:: Nbr.Nbr()
      :noindex:

   .. py:method:: Nbr.neighbor() -> Vertex
      :noindex:

        Get nerghbor vertex.

   .. py:method:: Nbr.get_str(column: int) -> str
      :noindex:

        Get edge str data by column id.

   .. py:method:: Nbr.get_int(column: int) -> str
      :noindex:

        Get edge int data by column id.

   .. py:method:: Nbr.get_double(column: int) -> str
      :noindex:

        Get edge double data by column id.


.. py:class:: AdjList
   :noindex:

   .. py:method:: AdjList.AdjList()
      :noindex:

   .. py:method:: AdjList.begin() -> Nbr
      :noindex:
        Return begin addr of the adj list.

   .. py:method:: AdjList.end() -> Nbr
      :noindex:

        Return end addr of the adj list.

   .. py:method:: AdjList.size() -> int
      :noindex:

        Get the size of adj list.


.. py:class:: Fragment
   :noindex:

   .. py:method:: Fragment.Fragment()
      :noindex:

   .. py:method:: Fragment.fid() -> int
      :noindex:

        Get fragment id.

   .. py:method:: Fragment.fnum() -> int
      :noindex:

        Get fragment number.

   .. py:method:: Fragment.vertex_label_num() -> int
      :noindex:

        Get vertex label number.

   .. py:method:: Fragment.edge_label_num() -> int
      :noindex:

        Get edge label number.

   .. py:method:: Fragment.get_total_nodes_num() -> size_t
      :noindex:

        Get total vertex number.

   .. py:method:: Fragment.get_nodes_num(vertex_label_id: int) -> size_t
      :noindex:

        Get vertex(inner + outer) number by label id.

   .. py:method:: Fragment.get_inner_nodes_num(vertex_label_id: int) -> size_t
      :noindex:

        Get inner vertex number by label id.

   .. py:method:: Fragment.get_outer_nodes_num(vertex_label_id: int) -> size_t
      :noindex:

        Get outer vertex number by label id.

   .. py:method:: Fragment.nodes(vertex_label_id: int) -> VertexRange
      :noindex:

        Get vertex range of this fragment by label id.

   .. py:method:: Fragment.inner_nodes(vertex_label_id: int) -> VertexRange
      :noindex:

        Get inner vertex range of this fragment by label id.

   .. py:method:: Fragment.outer_nodes(vertex_label_id: int) -> VertexRange
      :noindex:

        Get outer vertex range of this fragment by label id.

   .. py:method:: Fragment.get_node_fid(v: Vertex) -> int
      :noindex:

        Get vertex fragment id.

   .. py:method:: Fragment.is_inner_node(v: Vertex) -> bool
      :noindex:

        Return True if inner vertex of this fragment.

   .. py:method:: Fragment.is_outer_node(v: Vertex) -> bool
      :noindex:

        Return False if outer vertex of this fragment.

   .. py:method:: Fragment.get_node(label_id: int, oid: int64_t, v: Vertex&) -> bool
      :noindex:

        Return True if oid exist in this fragment.

   .. py:method:: Fragment.get_inner_node(label_id: int, oid: int64_t, v: Vertex&) -> bool
      :noindex:

        Return True if oid exist of inner vertex in this fragment.

   .. py:method:: Fragment.get_outer_node(label_id: int, oid: int64_t, v: Vertex&) -> bool
      :noindex:

        Return True if oid exist of outer vertex in this fragment.

   .. py:method:: Fragment.get_node_id(v: Vertex) -> int64_t
      :noindex:

        Get vertex oid.

   .. py:method:: Fragment.get_outgoing_edges(v: Vertex, edge_label_id: int) -> AdjList
      :noindex:

        Get a iterable of outgoing edges by label id of this vertex.

   .. py:method:: Fragment.get_incoming_edges(v: Vertex, edge_label_id: int) -> AdjList
      :noindex:

        Get a iterable of incoming edges by label id of this vertex.

   .. py:method:: Fragment.has_child(v: Vertex, edge_label_id: int) -> bool
      :noindex:

        Return True of vertex has child with connection of edge label id.

   .. py:method:: Fragment.has_parent(v: Vertex, edge_label_id: int) -> bool
      :noindex:

        Return True of vertex has parent with connection of edge label id.

   .. py:method:: Fragment.get_indegree(v: Vertex, edge_label_id: int) -> bool
      :noindex:

        Return the in-degree of edge with specified edge id.

   .. py:method:: Fragment.get_outdegree(v: Vertex, edge_label_id: int) -> bool
      :noindex:

        Return the out-degree of edge with specified edge id.

   .. py:method:: Fragment.get_str(v: Vertex, vertex_property_id: int) -> str
      :noindex:

        Get vertex str data by property id.

   .. py:method:: Fragment.get_int(v: Vertex, vertex_property_id: int) -> int
      :noindex:

        Get vertex int data by property id.

   .. py:method:: Fragment.get_double(v: Vertex, vertex_property_id: int) -> double
      :noindex:

        Get vertex double data by property id.

   .. py:method:: Fragment.vertex_labels() -> vector[str]
      :noindex:

       Get list of vertex label.

   .. py:method:: Fragment.edge_labels() -> vector[str]
      :noindex:

       Get list of edge label.

   .. py:method:: Fragment.get_vertex_label_by_id(vertex_label_id: int) -> str
      :noindex:

       Get vertex label name by label id.

   .. py:method:: Fragment.get_vertex_label_id_by_name(vertex_label_name: str) -> int
      :noindex:

       Get vertex label id by name.

   .. py:method:: Fragment.get_edge_label_by_id(edge_label_id: int) -> str
      :noindex:

       Get edge label name by label id.

   .. py:method:: Fragment.get_edge_label_id_by_name(edge_label_name: str) -> int
      :noindex:

       Get edge label id by name.

   .. py:method:: Fragment.vertex_properties(vertex_label_id: int) -> vector[pair[str, str]]
      :noindex:

       Get list of vertex properties by label id.

   .. py:method:: Fragment.vertex_properties(vertex_label_name: str) -> vector[pair[str, str]]
      :noindex:

       Get list of vertex properties by label name.

   .. py:method:: Fragment.edge_properties(edge_label_id: int) -> vector[pair[str, str]]
      :noindex:

       Get list of edge properties by label id.

   .. py:method:: Fragment.edge_properties(edge_label_name: str) -> vector[pair[str, str]]
     :noindex:

       Get list of edge properties by label name.

   .. py:method:: Fragment.get_vertex_property_id_by_name(vertex_label_name: str, vertex_property_name: str) -> int
      :noindex:

       Get vertex property id by property name.

   .. py:method:: Fragment.get_vertex_property_id_by_name(vertex_label_id: int, vertex_property_name: str) -> int
      :noindex:

       Get vertex property id by property name.

   .. py:method:: Fragment.get_vertex_property_by_id(vertex_label_name: str, vertex_property_id: int) -> str
      :noindex:

       Get vertex property name by property id.

   .. py:method:: Fragment.get_vertex_property_by_id(vertex_label_id: int, vertex_property_id: int) -> int
      :noindex:

       Get vertex property name by property id.

   .. py:method:: Fragment.get_edge_property_id_by_name(edge_label_name: str, edge_property_name: str) -> int
      :noindex:

       Get edge property id by property name.

   .. py:method:: Fragment.get_edge_property_id_by_name(edge_label_id: int, edge_property_name: str) -> int
      :noindex:

       Get edge property id by property name.

   .. py:method:: Fragment.get_edge_property_by_id(edge_label_name: str, edge_property_id: int) -> str
      :noindex:

       Get edge property name by property id.

   .. py:method:: Fragment.get_edge_property_by_id(edge_label_id: int, edge_property_id: int) -> int
      :noindex:

       Get edge property name by property id.


.. py:class:: Context[VD_TYPE, MD_TYPE]
   :noindex:

   .. py:method:: Context.Context()
      :noindex:

   .. py:method:: Context.superstep() -> int
      :noindex:

        Get current superstep.

   .. py:method:: Context.get_config(key: str) -> str
      :noindex:

        Get a value with specific key, or "" with key not exist.

   .. py:method:: Context.init_value(range: VertexRange, value: MD_TYPE, type: PIEAggregateType)
      :noindex:

        Init vertex range with value and type aggregator.

   .. py:method:: Context.register_sync_buffer(v_label_id: int, strategy: MessageStrategy)
      :noindex:

        Set auto parallel message strategy.

   .. py:method:: Context.set_node_value(v: Vertex, value: VD_TYPE)
      :noindex:

        Set the value of vertex.

   .. py:method:: Context.get_node_value(v: Vertex) -> VD_TYPE
      :noindex:

        Get the value of vertex.


.. py:class:: PIEAggregateType
   :noindex:

      After messages auto passing, each inner vertex will aggregate messages it received.

   .. py:data:: kMinAggregate
      :noindex:

         Aggregator for getting min value.

   .. py:data:: kMaxAggregate
      :noindex:

         Aggregator for getting max value.
   .. py:data:: kSumAggregate
      :noindex:

         Aggregator for summing up values.

   .. py:data:: kProductAggregate
      :noindex:

         Aggregator for calculating products of values.