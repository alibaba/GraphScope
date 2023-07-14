#[doc = "< incoming"]
pub const GRIN_DIRECTION_IN: GrinDirection = 0;
#[doc = "< outgoing"]
pub const GRIN_DIRECTION_OUT: GrinDirection = 1;
#[doc = "< incoming & outgoing"]
pub const GRIN_DIRECTION_BOTH: GrinDirection = 2;
#[doc = " Enumerates the directions of edges with respect to a certain vertex"]
pub type GrinDirection = u32;
#[doc = "< other unknown types"]
pub const GRIN_DATATYPE_UNDEFINED: GrinDatatype = 0;
#[doc = "< int"]
pub const GRIN_DATATYPE_INT32: GrinDatatype = 1;
#[doc = "< unsigned int"]
pub const GRIN_DATATYPE_UINT32: GrinDatatype = 2;
#[doc = "< long int"]
pub const GRIN_DATATYPE_INT64: GrinDatatype = 3;
#[doc = "< unsigned long int"]
pub const GRIN_DATATYPE_UINT64: GrinDatatype = 4;
#[doc = "< float"]
pub const GRIN_DATATYPE_FLOAT: GrinDatatype = 5;
#[doc = "< double"]
pub const GRIN_DATATYPE_DOUBLE: GrinDatatype = 6;
#[doc = "< string"]
pub const GRIN_DATATYPE_STRING: GrinDatatype = 7;
#[doc = "< date"]
pub const GRIN_DATATYPE_DATE32: GrinDatatype = 8;
#[doc = "< Time32"]
pub const GRIN_DATATYPE_TIME32: GrinDatatype = 9;
#[doc = "< Timestamp"]
pub const GRIN_DATATYPE_TIMESTAMP64: GrinDatatype = 10;
#[doc = " Enumerates the datatype supported in the storage"]
pub type GrinDatatype = u32;
#[doc = "< success"]
pub const GRIN_ERROR_CODE_NO_ERROR: GrinErrorCode = 0;
#[doc = "< unknown error"]
pub const GRIN_ERROR_CODE_UNKNOWN_ERROR: GrinErrorCode = 1;
#[doc = "< invalid value"]
pub const GRIN_ERROR_CODE_INVALID_VALUE: GrinErrorCode = 2;
#[doc = "< unknown datatype"]
pub const GRIN_ERROR_CODE_UNKNOWN_DATATYPE: GrinErrorCode = 3;
#[doc = " Enumerates the error codes of grin"]
pub type GrinErrorCode = u32;
cfg_if::cfg_if! {
    if #[cfg(feature = "grin_features_enable_v6d")]{
        pub type GrinGraph = *mut ::std::os::raw::c_void;
        pub type GrinVertex = u64;
        #[repr(C)]
        #[derive(Debug, Copy, Clone, PartialEq)]
        pub struct GrinEdge {
            pub src: GrinVertex,
            pub dst: GrinVertex,
            pub dir: GrinDirection,
            pub etype: u32,
            pub eid: u64,
        }
        pub type GrinVertexList = *mut ::std::os::raw::c_void;
        pub type GrinVertexListIterator = *mut ::std::os::raw::c_void;
        #[repr(C)]
        #[derive(Debug, Copy, Clone, PartialEq)]
        pub struct GrinAdjacentList {
            pub begin: *const ::std::os::raw::c_void,
            pub end: *const ::std::os::raw::c_void,
            pub vid: GrinVertex,
            pub dir: GrinDirection,
            pub etype: u32,
        }
        pub type GrinAdjacentListIterator = *mut ::std::os::raw::c_void;
        pub type GrinPartitionedGraph = *mut ::std::os::raw::c_void;
        pub type GrinPartition = u32;
        pub type GrinPartitionList = *mut ::std::os::raw::c_void;
        pub type GrinPartitionId = u32;
        pub type GrinVertexRef = i64;
        pub type GrinVertexType = u32;
        pub type GrinVertexTypeList = *mut ::std::os::raw::c_void;
        pub type GrinVertexProperty = u64;
        pub type GrinVertexPropertyList = *mut ::std::os::raw::c_void;
        pub type GrinVertexTypeId = u32;
        pub type GrinVertexPropertyId = u32;
        pub type GrinEdgeType = u32;
        pub type GrinEdgeTypeList = *mut ::std::os::raw::c_void;
        pub type GrinVevType = *mut ::std::os::raw::c_void;
        pub type GrinVevTypeList = *mut ::std::os::raw::c_void;
        pub type GrinEdgeProperty = u64;
        pub type GrinEdgePropertyList = *mut ::std::os::raw::c_void;
        pub type GrinEdgeTypeId = u32;
        pub type GrinEdgePropertyId = u32;
        pub type GrinRow = *mut ::std::os::raw::c_void;
        pub const GRIN_NULL_DATATYPE: GrinDatatype = GRIN_DATATYPE_UNDEFINED;
        pub const GRIN_NULL_GRAPH: GrinGraph = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX: GrinVertex = u64::MAX;
        pub const GRIN_NULL_EDGE: GrinEdge = GrinEdge{src: u64::MAX, dst: u64::MAX, dir: GRIN_DIRECTION_BOTH, etype: u32::MAX, eid: u64::MAX};
        pub const GRIN_NULL_VERTEX_LIST: GrinVertexList = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_LIST_ITERATOR: GrinVertexListIterator = std::ptr::null_mut();
        pub const GRIN_NULL_ADJACENT_LIST: GrinAdjacentList = GrinAdjacentList{begin: std::ptr::null(), end: std::ptr::null(), vid: u64::MAX, dir: GRIN_DIRECTION_BOTH, etype: u32::MAX};
        pub const GRIN_NULL_ADJACENT_LIST_ITERATOR: GrinAdjacentListIterator = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITIONED_GRAPH: GrinPartitionedGraph = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITION: GrinPartition = u32::MAX;
        pub const GRIN_NULL_PARTITION_LIST: GrinPartitionList = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITION_ID: GrinPartitionId = u32::MAX;
        pub const GRIN_NULL_VERTEX_REF: GrinVertexRef = -1;
        pub const GRIN_NULL_VERTEX_TYPE: GrinVertexType = u32::MAX;
        pub const GRIN_NULL_VERTEX_TYPE_LIST: GrinVertexTypeList = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_PROPERTY: GrinVertexProperty = u64::MAX;
        pub const GRIN_NULL_VERTEX_PROPERTY_LIST: GrinVertexPropertyList = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_TYPE_ID: GrinVertexTypeId = u32::MAX;
        pub const GRIN_NULL_VERTEX_PROPERTY_ID: GrinVertexPropertyId = u32::MAX;
        pub const GRIN_NULL_EDGE_TYPE: GrinEdgeType = u32::MAX;
        pub const GRIN_NULL_EDGE_TYPE_LIST: GrinEdgeTypeList = std::ptr::null_mut();
        pub const GRIN_NULL_VEV_TYPE: GrinVevType = std::ptr::null_mut();
        pub const GRIN_NULL_VEV_TYPE_LIST: GrinVevTypeList = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_PROPERTY: GrinEdgeProperty = u64::MAX;
        pub const GRIN_NULL_EDGE_PROPERTY_LIST: GrinEdgePropertyList = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_TYPE_ID: GrinEdgeTypeId = u32::MAX;
        pub const GRIN_NULL_EDGE_PROPERTY_ID: GrinEdgePropertyId = u32::MAX;
        pub const GRIN_NULL_ROW: GrinRow = std::ptr::null_mut();
        pub const GRIN_NULL_SIZE: u32 = u32::MAX;
    } else {
        pub type GrinGraph = *mut ::std::os::raw::c_void;
        pub type GrinVertex = *mut ::std::os::raw::c_void;
        pub type GrinEdge = *mut ::std::os::raw::c_void;
        pub type GrinVertexData = *mut ::std::os::raw::c_void;
        pub type GrinVertexList = *mut ::std::os::raw::c_void;
        pub type GrinVertexListIterator = *mut ::std::os::raw::c_void;
        pub type GrinAdjacentList = *mut ::std::os::raw::c_void;
        pub type GrinAdjacentListIterator = *mut ::std::os::raw::c_void;
        pub type GrinEdgeData = *mut ::std::os::raw::c_void;
        pub type GrinEdgeList = *mut ::std::os::raw::c_void;
        pub type GrinEdgeListIterator = *mut ::std::os::raw::c_void;
        pub type GrinPartitionedGraph = *mut ::std::os::raw::c_void;
        pub type GrinPartition = *mut ::std::os::raw::c_void;
        pub type GrinPartitionList = *mut ::std::os::raw::c_void;
        pub type GrinPartitionId = u32;
        pub type GrinVertexRef = *mut ::std::os::raw::c_void;
        pub type GrinEdgeRef = *mut ::std::os::raw::c_void;
        pub type GrinVertexType = *mut ::std::os::raw::c_void;
        pub type GrinVertexTypeList = *mut ::std::os::raw::c_void;
        pub type GrinVertexProperty = *mut ::std::os::raw::c_void;
        pub type GrinVertexPropertyList = *mut ::std::os::raw::c_void;
        pub type GrinVertexTypeId = u32;
        pub type GrinVertexPropertyId = u32;
        pub type GrinEdgeType = *mut ::std::os::raw::c_void;
        pub type GrinEdgeTypeList = *mut ::std::os::raw::c_void;
        pub type GrinVevType = *mut ::std::os::raw::c_void;
        pub type GrinVevTypeList = *mut ::std::os::raw::c_void;
        pub type GrinEdgeProperty = *mut ::std::os::raw::c_void;
        pub type GrinEdgePropertyList = *mut ::std::os::raw::c_void;
        pub type GrinEdgeTypeId = u32;
        pub type GrinEdgePropertyId = u32;
        pub type GrinRow = *mut ::std::os::raw::c_void;
        pub type GrinLabel = *mut ::std::os::raw::c_void;
        pub type GrinLabelList = *mut ::std::os::raw::c_void;
        pub const GRIN_NULL_DATATYPE: GrinDatatype = GRIN_DATATYPE_UNDEFINED;
        pub const GRIN_NULL_GRAPH: GrinGraph = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX: GrinVertex = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE: GrinEdge = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_DATA: GrinVertexData = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_LIST: GrinVertexList = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_LIST_ITERATOR: GrinVertexListIterator = std::ptr::null_mut();
        pub const GRIN_NULL_ADJACENT_LIST: GrinAdjacentList = std::ptr::null_mut();
        pub const GRIN_NULL_ADJACENT_LIST_ITERATOR: GrinAdjacentListIterator = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_DATA: GrinEdgeData = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_LIST: GrinEdgeList = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_LIST_ITERATOR: GrinEdgeListIterator = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITIONED_GRAPH: GrinPartitionedGraph = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITION: GrinPartition = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITION_LIST: GrinPartitionList = std::ptr::null_mut();
        pub const GRIN_NULL_PARTITION_ID: GrinPartitionId = u32::MAX;
        pub const GRIN_NULL_VERTEX_REF: GrinVertexRef = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_REF: GrinEdgeRef = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_TYPE: GrinVertexType = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_TYPE_LIST: GrinVertexTypeList = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_PROPERTY: GrinVertexProperty = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_PROPERTY_LIST: GrinVertexPropertyList = std::ptr::null_mut();
        pub const GRIN_NULL_VERTEX_TYPE_ID: GrinVertexTypeId = u32::MAX;
        pub const GRIN_NULL_VERTEX_PROPERTY_ID: GrinVertexPropertyId = u32::MAX;
        pub const GRIN_NULL_EDGE_TYPE: GrinEdgeType = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_TYPE_LIST: GrinEdgeTypeList = std::ptr::null_mut();
        pub const GRIN_NULL_VEV_TYPE: GrinVevType = std::ptr::null_mut();
        pub const GRIN_NULL_VEV_TYPE_LIST: GrinVevTypeList = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_PROPERTY: GrinEdgeProperty = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_PROPERTY_LIST: GrinEdgePropertyList = std::ptr::null_mut();
        pub const GRIN_NULL_EDGE_TYPE_ID: GrinEdgeTypeId = u32::MAX;
        pub const GRIN_NULL_EDGE_PROPERTY_ID: GrinEdgePropertyId = u32::MAX;
        pub const GRIN_NULL_ROW: GrinRow = std::ptr::null_mut();
        pub const GRIN_NULL_LABEL: GrinLabel = std::ptr::null_mut();
        pub const GRIN_NULL_LABEL_LIST: GrinLabelList = std::ptr::null_mut();
        pub const GRIN_NULL_SIZE: u32 = u32::MAX;
    }
}
extern "C" {
    #[cfg(feature = "grin_enable_adjacent_list")]
    #[allow(unused)]
    pub fn grin_destroy_adjacent_list(arg1: GrinGraph, arg2: GrinAdjacentList);

    #[cfg(feature = "grin_enable_adjacent_list_array")]
    #[allow(unused)]
    pub fn grin_get_adjacent_list_size(arg1: GrinGraph, arg2: GrinAdjacentList) -> usize;

    #[doc = " @brief Get the neighbor vertex from the adjacent list.\n @param GrinGraph The graph.\n @param GrinAdjacentList The adjacent list.\n @param index The index of the edge to/from the neighbor in the adjacent list.\n @return The neighbor vertex."]
    #[cfg(feature = "grin_enable_adjacent_list_array")]
    #[allow(unused)]
    pub fn grin_get_neighbor_from_adjacent_list(
        arg1: GrinGraph,
        arg2: GrinAdjacentList,
        index: usize,
    ) -> GrinVertex;

    #[doc = " @brief Get the edge from the adjacent list.\n @param GrinGraph The graph.\n @param GrinAdjacentList The adjacent list.\n @param index The index of the edge in the adjacent list.\n @return The edge. Note that when the direction is OUT, the destination vertex\n of the edge is the neighbor vertex. While the direction is IN, the source\n vertex of the edge is the neighbor vertex."]
    #[cfg(feature = "grin_enable_adjacent_list_array")]
    #[allow(unused)]
    pub fn grin_get_edge_from_adjacent_list(
        arg1: GrinGraph,
        arg2: GrinAdjacentList,
        arg3: usize,
    ) -> GrinEdge;

    #[doc = " @brief Get the begin iterator of the adjacent list.\n @param GrinGraph The graph.\n @param GrinAdjacentList The adjacent list.\n @return The begin iterator of the adjacent list."]
    #[cfg(feature = "grin_enable_adjacent_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_adjacent_list_begin(
        arg1: GrinGraph,
        arg2: GrinAdjacentList,
    ) -> GrinAdjacentListIterator;

    #[cfg(feature = "grin_enable_adjacent_list_iterator")]
    #[allow(unused)]
    pub fn grin_destroy_adjacent_list_iter(arg1: GrinGraph, arg2: GrinAdjacentListIterator);

    #[doc = " @brief Update the iterator to the next of the adjacent list.\n @param GrinGraph The graph.\n @param GrinAdjacentListIterator The adjacent list iterator to be updated."]
    #[cfg(feature = "grin_enable_adjacent_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_next_adjacent_list_iter(arg1: GrinGraph, arg2: GrinAdjacentListIterator);

    #[doc = " @brief Check if the adjacent list iterator is at the end.\n Note that we may get an end iterator when calling ``grin_get_adjacent_list_begin``\n if the adjacent list is empty.\n Users should check if the iterator is at the end before using it.\n @param GrinGraph The graph.\n @param GrinAdjacentListIterator The adjacent list iterator.\n @return True if the iterator is at the end, otherwise false."]
    #[cfg(feature = "grin_enable_adjacent_list_iterator")]
    #[allow(unused)]
    pub fn grin_is_adjacent_list_end(arg1: GrinGraph, arg2: GrinAdjacentListIterator) -> bool;

    #[doc = " @brief Get the neighbor vertex from the adjacent list iterator.\n @param GrinGraph The graph.\n @param GrinAdjacentListIterator The adjacent list iterator.\n @return The neighbor vertex."]
    #[cfg(feature = "grin_enable_adjacent_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_neighbor_from_adjacent_list_iter(
        arg1: GrinGraph,
        arg2: GrinAdjacentListIterator,
    ) -> GrinVertex;

    #[doc = " @brief Get the edge from the adjacent list iterator.\n @param GrinGraph The graph.\n @param GrinAdjacentListIterator The adjacent list iterator.\n @return The edge. Note that when the direction is OUT, the destination vertex\n of the edge is the neighbor vertex. While the direction is IN, the source\n vertex of the edge is the neighbor vertex."]
    #[cfg(feature = "grin_enable_adjacent_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_edge_from_adjacent_list_iter(
        arg1: GrinGraph,
        arg2: GrinAdjacentListIterator,
    ) -> GrinEdge;

    #[cfg(feature = "grin_enable_edge_list")]
    #[allow(unused)]
    pub fn grin_destroy_edge_list(arg1: GrinGraph, arg2: GrinEdgeList);

    #[cfg(feature = "grin_enable_edge_list_array")]
    #[allow(unused)]
    pub fn grin_get_edge_list_size(arg1: GrinGraph, arg2: GrinEdgeList) -> usize;

    #[cfg(feature = "grin_enable_edge_list_array")]
    #[allow(unused)]
    pub fn grin_get_edge_from_list(
        arg1: GrinGraph,
        arg2: GrinEdgeList,
        arg3: usize,
    ) -> GrinEdge;

    #[cfg(feature = "grin_enable_edge_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_edge_list_begin(
        arg1: GrinGraph,
        arg2: GrinEdgeList,
    ) -> GrinEdgeListIterator;

    #[cfg(feature = "grin_enable_edge_list_iterator")]
    #[allow(unused)]
    pub fn grin_destroy_edge_list_iter(arg1: GrinGraph, arg2: GrinEdgeListIterator);

    #[cfg(feature = "grin_enable_edge_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_next_edge_list_iter(arg1: GrinGraph, arg2: GrinEdgeListIterator);

    #[cfg(feature = "grin_enable_edge_list_iterator")]
    #[allow(unused)]
    pub fn grin_is_edge_list_end(arg1: GrinGraph, arg2: GrinEdgeListIterator) -> bool;

    #[cfg(feature = "grin_enable_edge_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_edge_from_iter(arg1: GrinGraph, arg2: GrinEdgeListIterator) -> GrinEdge;

    #[doc = " @brief Get a (non-partitioned) graph from storage\n @param uri The URI of the graph.\n Current URI for supported storage includes:\n 1. gart://{etcd_endpoint}?prefix={etcd_prefix}&version={version}\n 2. graphar://{yaml_path}?partition_num={partition_num}&strategy={strategy}\n 3. v6d://{object_id}?ipc_socket={ipc_socket} where ipc_socket is optional.\n @return A graph handle."]
    #[allow(unused)]
    pub fn grin_get_graph_from_storage(arg1: *const ::std::os::raw::c_char) -> GrinGraph;

    #[allow(unused)]
    pub fn grin_destroy_graph(arg1: GrinGraph);

    #[doc = " @brief Check if the graph is directed.\n This API is only available when the storage supports both directed and\n undirected graph. Otherwise, check which of ``GRIN_ASSUME_HAS_DIRECTED_GRAPH``\n and ``GRIN_ASSUME_HAS_UNDIRECTED_GRAPH`` is defined.\n @param GrinGraph The graph.\n @return True if the graph is directed, otherwise false."]
    #[cfg(all(feature = "grin_assume_has_directed_graph", feature = "grin_assume_has_undirected_graph"))]
    #[allow(unused)]
    pub fn grin_is_directed(arg1: GrinGraph) -> bool;

    #[doc = " @brief Check if the graph is a multigraph.\n This API is only available when the storage supports multigraph.\n @param GrinGraph The graph.\n @return True if the graph is a multigraph, otherwise false."]
    #[cfg(feature = "grin_assume_has_multi_edge_graph")]
    #[allow(unused)]
    pub fn grin_is_multigraph(arg1: GrinGraph) -> bool;

    #[allow(unused)]
    pub fn grin_destroy_vertex(arg1: GrinGraph, arg2: GrinVertex);

    #[allow(unused)]
    pub fn grin_equal_vertex(arg1: GrinGraph, arg2: GrinVertex, arg3: GrinVertex) -> bool;

    #[cfg(feature = "grin_with_vertex_data")]
    #[allow(unused)]
    pub fn grin_get_vertex_data_datatype(arg1: GrinGraph, arg2: GrinVertex) -> GrinDatatype;

    #[cfg(feature = "grin_with_vertex_data")]
    #[allow(unused)]
    pub fn grin_get_vertex_data_value(
        arg1: GrinGraph,
        arg2: GrinVertex,
    ) -> *const ::std::os::raw::c_void;

    #[allow(unused)]
    pub fn grin_destroy_edge(arg1: GrinGraph, arg2: GrinEdge);

    #[doc = " @brief Get the source vertex of an edge.\n @param GrinGraph The graph.\n @param GrinEdge The edge.\n @return The source vertex of the edge."]
    #[allow(unused)]
    pub fn grin_get_src_vertex_from_edge(arg1: GrinGraph, arg2: GrinEdge) -> GrinVertex;

    #[doc = " @brief Get the destination vertex of an edge.\n @param GrinGraph The graph.\n @param GrinEdge The edge.\n @return The destination vertex of the edge."]
    #[allow(unused)]
    pub fn grin_get_dst_vertex_from_edge(arg1: GrinGraph, arg2: GrinEdge) -> GrinVertex;

    #[cfg(feature = "grin_with_edge_data")]
    #[allow(unused)]
    pub fn grin_get_edge_data_datatype(arg1: GrinGraph, arg2: GrinEdge) -> GrinDatatype;

    #[cfg(feature = "grin_with_edge_data")]
    #[allow(unused)]
    pub fn grin_get_edge_data_value(
        arg1: GrinGraph,
        arg2: GrinEdge,
    ) -> *const ::std::os::raw::c_void;

    #[cfg(feature = "grin_enable_vertex_list")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_list(arg1: GrinGraph, arg2: GrinVertexList);

    #[cfg(feature = "grin_enable_vertex_list_array")]
    #[allow(unused)]
    pub fn grin_get_vertex_list_size(arg1: GrinGraph, arg2: GrinVertexList) -> usize;

    #[doc = " @brief Get the vertex from the vertex list.\n @param GrinGraph The graph.\n @param GrinVertexList The vertex list.\n @param index The index of the vertex in the vertex list.\n @return The vertex."]
    #[cfg(feature = "grin_enable_vertex_list_array")]
    #[allow(unused)]
    pub fn grin_get_vertex_from_list(
        arg1: GrinGraph,
        arg2: GrinVertexList,
        index: usize,
    ) -> GrinVertex;

    #[doc = " @brief Get the begin iterator of the vertex list.\n @param GrinGraph The graph.\n @param GrinVertexList The vertex list.\n @return The begin iterator."]
    #[cfg(feature = "grin_enable_vertex_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_vertex_list_begin(
        arg1: GrinGraph,
        arg2: GrinVertexList,
    ) -> GrinVertexListIterator;

    #[cfg(feature = "grin_enable_vertex_list_iterator")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_list_iter(arg1: GrinGraph, arg2: GrinVertexListIterator);

    #[doc = " @brief Update the iterator to the next of the vertex list.\n @param GrinGraph The graph.\n @param GrinVertexListIterator The iterator to be updated."]
    #[cfg(feature = "grin_enable_vertex_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_next_vertex_list_iter(arg1: GrinGraph, arg2: GrinVertexListIterator);

    #[doc = " @brief Check whether the iterator reaches the end of the vertex list.\n Note that we may get an end iterator when calling ``grin_get_vertex_list_begin``\n if the vertex list is empty.\n Users should check if the iterator is at the end before using it.\n @param GrinGraph The graph.\n @param GrinVertexListIterator The iterator.\n @return True if the iterator reaches the end of the vertex list."]
    #[cfg(feature = "grin_enable_vertex_list_iterator")]
    #[allow(unused)]
    pub fn grin_is_vertex_list_end(arg1: GrinGraph, arg2: GrinVertexListIterator) -> bool;

    #[doc = " @brief Get the vertex from the iterator.\n @param GrinGraph The graph.\n @param GrinVertexListIterator The iterator.\n @return The vertex."]
    #[cfg(feature = "grin_enable_vertex_list_iterator")]
    #[allow(unused)]
    pub fn grin_get_vertex_from_iter(
        arg1: GrinGraph,
        arg2: GrinVertexListIterator,
    ) -> GrinVertex;

    #[doc = " @brief Get a partitioned graph from a storage.\n @param uri The URI of the graph.\n Current URI for supported storage includes:\n 1. gart://{etcd_endpoint}?prefix={etcd_prefix}&version={version}\n 2. graphar://{yaml_path}?partition_num={partition_num}&strategy={strategy}\n 3. v6d://{object_id}?ipc_socket={ipc_socket} where ipc_socket is optional.\n @return A partitioned graph handle."]
    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_partitioned_graph_from_storage(
        uri: *const ::std::os::raw::c_char,
    ) -> GrinPartitionedGraph;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_destroy_partitioned_graph(arg1: GrinPartitionedGraph);

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_total_partitions_number(arg1: GrinPartitionedGraph) -> usize;

    #[doc = " @brief Get the local partition list of the partitioned graph.\n For example, a graph may be partitioned into 6 partitions and located in\n 2 machines, then each machine may contain a local partition list of size 3.\n @param GrinPartitionedGraph The partitioned graph.\n @return A partition list of local partitions."]
    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_local_partition_list(arg1: GrinPartitionedGraph) -> GrinPartitionList;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_destroy_partition_list(arg1: GrinPartitionedGraph, arg2: GrinPartitionList);

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_create_partition_list(arg1: GrinPartitionedGraph) -> GrinPartitionList;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_insert_partition_to_list(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartitionList,
        arg3: GrinPartition,
    ) -> bool;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_partition_list_size(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartitionList,
    ) -> usize;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_partition_from_list(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartitionList,
        arg3: usize,
    ) -> GrinPartition;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_equal_partition(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartition,
        arg3: GrinPartition,
    ) -> bool;

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_destroy_partition(arg1: GrinPartitionedGraph, arg2: GrinPartition);

    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_partition_info(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartition,
    ) -> *const ::std::os::raw::c_void;

    #[doc = " @brief Get a local graph of the partitioned graph.\n @param GrinPartitionedGraph The partitioned graph.\n @param GrinPartition The partition of the graph.\n @return A local graph."]
    #[cfg(feature = "grin_enable_graph_partition")]
    #[allow(unused)]
    pub fn grin_get_local_graph_by_partition(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartition,
    ) -> GrinGraph;

    #[cfg(feature = "grin_trait_natural_id_for_partition")]
    #[allow(unused)]
    pub fn grin_get_partition_by_id(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartitionId,
    ) -> GrinPartition;

    #[cfg(feature = "grin_trait_natural_id_for_partition")]
    #[allow(unused)]
    pub fn grin_get_partition_id(
        arg1: GrinPartitionedGraph,
        arg2: GrinPartition,
    ) -> GrinPartitionId;

    #[doc = " @brief Get the vertex ref of a vertex.\n A vertex ref is a reference for a \"local\" vertex, and the reference can\n be recognized by other partitions.\n To transfer the vertex ref handle between partitions, users should\n first call serialization methods to serialize the vertex ref handle\n into string or int64 based on the storage's features;\n then send the messages to remote partitions and deserialize the string or\n int64 remotely to get the vertex ref handle on the remote partition;\n finally use ``grin_get_vertex_by_vertex_ref`` to get the vertex handle\n on the remote partition.\n These two vertices should represent the same vertex in the partitioned graph.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @return The vertex ref"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_get_vertex_ref_by_vertex(arg1: GrinGraph, arg2: GrinVertex) -> GrinVertexRef;

    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_ref(arg1: GrinGraph, arg2: GrinVertexRef);

    #[doc = " @brief get the local vertex handle from the vertex ref handle\n if the vertex ref handle is not recognized, a null vertex is returned\n @param GrinGraph The graph\n @param GrinVertexRef The vertex ref\n @return The vertex handle"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_get_vertex_from_vertex_ref(arg1: GrinGraph, arg2: GrinVertexRef) -> GrinVertex;

    #[doc = " @brief get the master partition of a vertex ref.\n Some storage can still provide the master partition of the vertex ref,\n even if the vertex ref can NOT be recognized locally.\n @param GrinGraph The graph\n @param GrinVertexRef The vertex ref"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_get_master_partition_from_vertex_ref(
        arg1: GrinGraph,
        arg2: GrinVertexRef,
    ) -> GrinPartition;

    #[doc = " @brief serialize the vertex ref handle to string\n The returned string should be freed by ``grin_destroy_serialized_vertex_ref``\n @param GrinGraph The graph\n @param GrinVertexRef The vertex ref"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_serialize_vertex_ref(
        arg1: GrinGraph,
        arg2: GrinVertexRef,
    ) -> *const ::std::os::raw::c_char;

    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_destroy_serialized_vertex_ref(
        arg1: GrinGraph,
        arg2: *const ::std::os::raw::c_char,
    );

    #[doc = " @brief deserialize the string to vertex ref handle\n If the string is invalid, a null vertex ref is returned\n @param GrinGraph The graph\n @param msg The string message to be deserialized"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_deserialize_to_vertex_ref(
        arg1: GrinGraph,
        msg: *const ::std::os::raw::c_char,
    ) -> GrinVertexRef;

    #[doc = " @brief check if the vertex is a master vertex\n @param GrinGraph The graph\n @param GrinVertex The vertex"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_is_master_vertex(arg1: GrinGraph, arg2: GrinVertex) -> bool;

    #[doc = " @brief check if the vertex is a mirror vertex\n @param GrinGraph The graph\n @param GrinVertex The vertex"]
    #[cfg(feature = "grin_enable_vertex_ref")]
    #[allow(unused)]
    pub fn grin_is_mirror_vertex(arg1: GrinGraph, arg2: GrinVertex) -> bool;

    #[doc = " @brief serialize the vertex ref handle to int64\n This API is enabled by ``GRIN_TRAIT_FAST_VERTEX_REF``, meaning the vertex ref\n can be serialized into int64 instead of string.\n Obviously transferring and serializing int64 is faster than string.\n @param GrinGraph The graph\n @param GrinVertexRef The vertex ref"]
    #[cfg(feature = "grin_trait_fast_vertex_ref")]
    #[allow(unused)]
    pub fn grin_serialize_vertex_ref_as_int64(
        arg1: GrinGraph,
        arg2: GrinVertexRef,
    ) -> i64;

    #[doc = " @brief deserialize the int64 to vertex ref handle\n @param GrinGraph The graph\n @param msg The int64 message to be deserialized"]
    #[cfg(feature = "grin_trait_fast_vertex_ref")]
    #[allow(unused)]
    pub fn grin_deserialize_int64_to_vertex_ref(
        arg1: GrinGraph,
        msg: i64,
    ) -> GrinVertexRef;

    #[cfg(feature = "grin_trait_master_vertex_mirror_partition_list")]
    #[allow(unused)]
    pub fn grin_get_master_vertex_mirror_partition_list(
        arg1: GrinGraph,
        arg2: GrinVertex,
    ) -> GrinPartitionList;

    #[cfg(feature = "grin_trait_mirror_vertex_mirror_partition_list")]
    #[allow(unused)]
    pub fn grin_get_mirror_vertex_mirror_partition_list(
        arg1: GrinGraph,
        arg2: GrinVertex,
    ) -> GrinPartitionList;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_get_edge_ref_by_edge(arg1: GrinGraph, arg2: GrinEdge) -> GrinEdgeRef;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_destroy_edge_ref(arg1: GrinGraph, arg2: GrinEdgeRef);

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_get_edge_from_edge_ref(arg1: GrinGraph, arg2: GrinEdgeRef) -> GrinEdge;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_get_master_partition_from_edge_ref(
        arg1: GrinGraph,
        arg2: GrinEdgeRef,
    ) -> GrinPartition;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_serialize_edge_ref(
        arg1: GrinGraph,
        arg2: GrinEdgeRef,
    ) -> *const ::std::os::raw::c_char;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_destroy_serialized_edge_ref(arg1: GrinGraph, arg2: *const ::std::os::raw::c_char);

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_deserialize_to_edge_ref(
        arg1: GrinGraph,
        arg2: *const ::std::os::raw::c_char,
    ) -> GrinEdgeRef;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_is_master_edge(arg1: GrinGraph, arg2: GrinEdge) -> bool;

    #[cfg(feature = "grin_enable_edge_ref")]
    #[allow(unused)]
    pub fn grin_is_mirror_edge(arg1: GrinGraph, arg2: GrinEdge) -> bool;

    #[cfg(feature = "grin_trait_master_edge_mirror_partition_list")]
    #[allow(unused)]
    pub fn grin_get_master_edge_mirror_partition_list(
        arg1: GrinGraph,
        arg2: GrinEdge,
    ) -> GrinPartitionList;

    #[cfg(feature = "grin_trait_mirror_edge_mirror_partition_list")]
    #[allow(unused)]
    pub fn grin_get_mirror_edge_mirror_partition_list(
        arg1: GrinGraph,
        arg2: GrinEdge,
    ) -> GrinPartitionList;

    #[doc = " @brief Get the vertex list of a given type with master vertices only.\n This API is only available for property graph.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @return The vertex list of master vertices only."]
    #[cfg(all(feature = "grin_trait_select_master_for_vertex_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_list_by_type_select_master(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> GrinVertexList;

    #[doc = " @brief Get the vertex list of a given type with mirror vertices only.\n This API is only available for property graph.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @return The vertex list of mirror vertices only."]
    #[cfg(all(feature = "grin_trait_select_master_for_vertex_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_list_by_type_select_mirror(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> GrinVertexList;

    #[cfg(all(feature = "grin_trait_select_partition_for_vertex_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_list_by_type_select_partition(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinPartition,
    ) -> GrinVertexList;

    #[cfg(all(feature = "grin_trait_select_master_for_edge_list", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_edge_list_by_type_select_master(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> GrinEdgeList;

    #[cfg(all(feature = "grin_trait_select_master_for_edge_list", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_edge_list_by_type_select_mirror(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> GrinEdgeList;

    #[cfg(all(feature = "grin_trait_select_partition_for_edge_list", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_edge_list_by_type_select_partition(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        arg3: GrinPartition,
    ) -> GrinEdgeList;

    #[cfg(all(feature = "grin_trait_select_master_neighbor_for_adjacent_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_adjacent_list_by_edge_type_select_master_neighbor(
        arg1: GrinGraph,
        arg2: GrinDirection,
        arg3: GrinVertex,
        arg4: GrinEdgeType,
    ) -> GrinAdjacentList;

    #[cfg(all(feature = "grin_trait_select_master_neighbor_for_adjacent_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_adjacent_list_by_edge_type_select_mirror_neighbor(
        arg1: GrinGraph,
        arg2: GrinDirection,
        arg3: GrinVertex,
        arg4: GrinEdgeType,
    ) -> GrinAdjacentList;

    #[cfg(all(feature = "grin_trait_select_neighbor_partition_for_adjacent_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_adjacent_list_by_edge_type_select_partition_neighbor(
        arg1: GrinGraph,
        arg2: GrinDirection,
        arg3: GrinVertex,
        arg4: GrinEdgeType,
        arg5: GrinPartition,
    ) -> GrinAdjacentList;

    #[cfg(feature = "grin_trait_specific_vev_relation")]
    #[allow(unused)]
    pub fn grin_get_one_to_one_vev_types(arg1: GrinGraph) -> GrinVevTypeList;

    #[cfg(feature = "grin_trait_specific_vev_relation")]
    #[allow(unused)]
    pub fn grin_get_one_to_many_vev_types(arg1: GrinGraph) -> GrinVevTypeList;

    #[cfg(feature = "grin_trait_specific_vev_relation")]
    #[allow(unused)]
    pub fn grin_get_many_to_one_vev_types(arg1: GrinGraph) -> GrinVevTypeList;

    #[cfg(feature = "grin_trait_specific_vev_relation")]
    #[allow(unused)]
    pub fn grin_get_many_to_many_vev_types(arg1: GrinGraph) -> GrinVevTypeList;

    #[doc = " @brief Get the vertex types that have primary keys\n In some graph, not every vertex type has primary keys.\n @param GrinGraph The graph\n @return The vertex type list of types that have primary keys"]
    #[cfg(feature = "grin_enable_vertex_primary_keys")]
    #[allow(unused)]
    pub fn grin_get_vertex_types_with_primary_keys(arg1: GrinGraph) -> GrinVertexTypeList;

    #[doc = " @brief Get the primary keys properties of a vertex type\n The primary keys properties are the properties that can be used to identify a vertex.\n They are a subset of the properties of a vertex type.\n @param GrinGraph The graph\n @param GrinVertexType The vertex type\n @return The primary keys properties list"]
    #[cfg(feature = "grin_enable_vertex_primary_keys")]
    #[allow(unused)]
    pub fn grin_get_primary_keys_by_vertex_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> GrinVertexPropertyList;

    #[doc = " @brief Get the primary keys values row of a vertex\n The values in the row are in the same order as the primary keys properties.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @return The primary keys values row"]
    #[cfg(feature = "grin_enable_vertex_primary_keys")]
    #[allow(unused)]
    pub fn grin_get_vertex_primary_keys_row(arg1: GrinGraph, arg2: GrinVertex) -> GrinRow;

    #[cfg(feature = "grin_enable_edge_primary_keys")]
    #[allow(unused)]
    pub fn grin_get_edge_types_with_primary_keys(arg1: GrinGraph) -> GrinEdgeTypeList;

    #[cfg(feature = "grin_enable_edge_primary_keys")]
    #[allow(unused)]
    pub fn grin_get_primary_keys_by_edge_type(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> GrinEdgePropertyList;

    #[cfg(feature = "grin_enable_edge_primary_keys")]
    #[allow(unused)]
    pub fn grin_get_edge_primary_keys_row(arg1: GrinGraph, arg2: GrinEdge) -> GrinRow;

    #[allow(unused)]
    pub fn grin_destroy_string_value(arg1: GrinGraph, arg2: *const ::std::os::raw::c_char);

    #[doc = " @brief Get the vertex property name\n @param GrinGraph The graph\n @param GrinVertexType The vertex type that the property belongs to\n @param GrinVertexProperty The vertex property\n @return The property's name as string"]
    #[cfg(feature = "grin_with_vertex_property_name")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_name(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinVertexProperty,
    ) -> *const ::std::os::raw::c_char;

    #[doc = " @brief Get the vertex property with a given name under a specific vertex type\n @param GrinGraph The graph\n @param GrinVertexType The specific vertex type\n @param name The name\n @return The vertex property"]
    #[cfg(feature = "grin_with_vertex_property_name")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_by_name(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        name: *const ::std::os::raw::c_char,
    ) -> GrinVertexProperty;

    #[doc = " @brief Get properties under all types with a given name.\n For example, vertex type \"person\" and \"company\" both have a property\n called \"name\". When this API is called given \"name\", it will return a list\n of \"name\" properties under both types.\n @param GrinGraph The graph\n @param name The name\n @return The vertex property list of properties with the given name"]
    #[cfg(feature = "grin_with_vertex_property_name")]
    #[allow(unused)]
    pub fn grin_get_vertex_properties_by_name(
        arg1: GrinGraph,
        name: *const ::std::os::raw::c_char,
    ) -> GrinVertexPropertyList;

    #[cfg(feature = "grin_with_edge_property_name")]
    #[allow(unused)]
    pub fn grin_get_edge_property_name(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        arg3: GrinEdgeProperty,
    ) -> *const ::std::os::raw::c_char;

    #[cfg(feature = "grin_with_edge_property_name")]
    #[allow(unused)]
    pub fn grin_get_edge_property_by_name(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        name: *const ::std::os::raw::c_char,
    ) -> GrinEdgeProperty;

    #[cfg(feature = "grin_with_edge_property_name")]
    #[allow(unused)]
    pub fn grin_get_edge_properties_by_name(
        arg1: GrinGraph,
        name: *const ::std::os::raw::c_char,
    ) -> GrinEdgePropertyList;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_equal_vertex_property(
        arg1: GrinGraph,
        arg2: GrinVertexProperty,
        arg3: GrinVertexProperty,
    ) -> bool;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_property(arg1: GrinGraph, arg2: GrinVertexProperty);

    #[doc = " @brief Get the datatype of the vertex property\n @param GrinGraph The graph\n @param GrinVertexProperty The vertex property\n @return The datatype of the vertex property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_datatype(
        arg1: GrinGraph,
        arg2: GrinVertexProperty,
    ) -> GrinDatatype;

    #[doc = " @brief Get the value of int32, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype int32.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_int32(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> i32;

    #[doc = " @brief Get the value of uint32, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype uint32.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_uint32(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> u32;

    #[doc = " @brief Get the value of int64, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype int64.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_int64(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> i64;

    #[doc = " @brief Get the value of uint64, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype uint64.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_uint64(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> u64;

    #[doc = " @brief Get the value of float, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype float.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_float(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> f32;

    #[doc = " @brief Get the value of double, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype double.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_double(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> f64;

    #[doc = " @brief Get the value of string, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype string.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n Note that the returned string should be explicitly freed by the user,\n by calling API ``grin_destroy_string_value``.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_string(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> *const ::std::os::raw::c_char;

    #[doc = " @brief Get the value of int32, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype date32.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_date32(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> i32;

    #[doc = " @brief Get the value of int32, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype time32.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n Note that the returned string should be explicitly freed by the user,\n by calling API ``grin_destroy_string_value``.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_time32(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> i32;

    #[doc = " @brief Get the value of int64, given a vertex and a vertex property.\n The user should make sure the vertex property is of datatype timestamp64.\n The return int has no predefined invalid value.\n User should use ``grin_get_last_error_code()`` to check if the API call\n is successful.\n Note that the returned string should be explicitly freed by the user,\n by calling API ``grin_destroy_string_value``.\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @param GrinVertexProperty The vertex property\n @return The value of the property"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value_of_timestamp64(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> i64;

    #[doc = " @brief Get the vertex type that a given vertex property belongs to.\n @param GrinGraph The graph\n @param GrinVertexProperty The vertex property\n @return The vertex type"]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_from_property(
        arg1: GrinGraph,
        arg2: GrinVertexProperty,
    ) -> GrinVertexType;

    #[cfg(all(feature = "grin_with_vertex_property", feature = "grin_trait_const_value_ptr"))]
    #[allow(unused)]
    pub fn grin_get_vertex_property_value(
        arg1: GrinGraph,
        arg2: GrinVertex,
        arg3: GrinVertexProperty,
    ) -> *const ::std::os::raw::c_void;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_equal_edge_property(
        arg1: GrinGraph,
        arg2: GrinEdgeProperty,
        arg3: GrinEdgeProperty,
    ) -> bool;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_destroy_edge_property(arg1: GrinGraph, arg2: GrinEdgeProperty);

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_datatype(
        arg1: GrinGraph,
        arg2: GrinEdgeProperty,
    ) -> GrinDatatype;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_int32(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> i32;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_uint32(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> u32;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_int64(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> i64;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_uint64(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> u64;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_float(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> f32;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_double(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> f64;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_string(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> *const ::std::os::raw::c_char;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_date32(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> i32;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_time32(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> i32;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_value_of_timestamp64(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> i64;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_type_from_property(
        arg1: GrinGraph,
        arg2: GrinEdgeProperty,
    ) -> GrinEdgeType;

    #[cfg(all(feature = "grin_with_edge_property", feature = "grin_trait_const_value_ptr"))]
    #[allow(unused)]
    pub fn grin_get_edge_property_value(
        arg1: GrinGraph,
        arg2: GrinEdge,
        arg3: GrinEdgeProperty,
    ) -> *const ::std::os::raw::c_void;

    #[doc = " @brief Get the vertex property list of the graph.\n This API is only available for property graph.\n @param GrinGraph The graph.\n @return The vertex property list."]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_list_by_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> GrinVertexPropertyList;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_list_size(
        arg1: GrinGraph,
        arg2: GrinVertexPropertyList,
    ) -> usize;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_from_list(
        arg1: GrinGraph,
        arg2: GrinVertexPropertyList,
        arg3: usize,
    ) -> GrinVertexProperty;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_create_vertex_property_list(arg1: GrinGraph) -> GrinVertexPropertyList;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_property_list(arg1: GrinGraph, arg2: GrinVertexPropertyList);

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_insert_vertex_property_to_list(
        arg1: GrinGraph,
        arg2: GrinVertexPropertyList,
        arg3: GrinVertexProperty,
    ) -> bool;

    #[doc = " @brief Get the vertex property handle by id.\n This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY``,\n meaning that the storage has naturally increasing ids for vertex properties\n under a certain vertex type.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @param GrinVertexPropertyId The vertex property id.\n @return The vertex property handle."]
    #[cfg(feature = "grin_trait_natural_id_for_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_by_id(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinVertexPropertyId,
    ) -> GrinVertexProperty;

    #[doc = " @brief Get the vertex property's natural id.\n This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_PROPERTY``,\n meaning that the storage has naturally increasing ids for vertex properties\n under a certain vertex type.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @param GrinVertexProperty The vertex property handle.\n @return The vertex property id."]
    #[cfg(feature = "grin_trait_natural_id_for_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_property_id(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinVertexProperty,
    ) -> GrinVertexPropertyId;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_list_by_type(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> GrinEdgePropertyList;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_list_size(
        arg1: GrinGraph,
        arg2: GrinEdgePropertyList,
    ) -> usize;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_from_list(
        arg1: GrinGraph,
        arg2: GrinEdgePropertyList,
        arg3: usize,
    ) -> GrinEdgeProperty;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_create_edge_property_list(arg1: GrinGraph) -> GrinEdgePropertyList;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_destroy_edge_property_list(arg1: GrinGraph, arg2: GrinEdgePropertyList);

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_insert_edge_property_to_list(
        arg1: GrinGraph,
        arg2: GrinEdgePropertyList,
        arg3: GrinEdgeProperty,
    ) -> bool;

    #[cfg(feature = "grin_trait_natural_id_for_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_by_id(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        arg3: GrinEdgePropertyId,
    ) -> GrinEdgeProperty;

    #[doc = " We must specify the edge type here, because the edge property id is unique only under a specific edge type"]
    #[cfg(feature = "grin_trait_natural_id_for_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_property_id(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        arg3: GrinEdgeProperty,
    ) -> GrinEdgePropertyId;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_destroy_row(arg1: GrinGraph, arg2: GrinRow);

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_int32_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> i32;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_uint32_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> u32;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_int64_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> i64;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_uint64_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> u64;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_float_from_row(arg1: GrinGraph, arg2: GrinRow, arg3: usize) -> f32;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_double_from_row(arg1: GrinGraph, arg2: GrinRow, arg3: usize) -> f64;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_string_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> *const ::std::os::raw::c_char;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_date32_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> i32;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_time32_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> i32;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_get_timestamp64_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: usize,
    ) -> i64;

    #[doc = " @brief Create a row.\n Row works as carrier of property values in GRIN.\n It is a pure value array, and users can only get the value by the array index.\n That means users should understand the property that each value is\n representing when using the row.\n Currently rows are used in two scenarios:\n 1. Users can create a row of values for primary keys properties,\n and then query the vertex/edge using the row if pk indexing is enabled.\n 2. Users can get the row of values for the entire property list of\n a vertex/edge in one API ``grin_get_vertex_row`` or ``grin_get_edge_row``.\n However this API is not recommended if the user only wants to get the\n properties values, in which case, the user can get property values\n one-by-one using the APIs like ``grin_get_vertex_property_value_of_int32``."]
    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_create_row(arg1: GrinGraph) -> GrinRow;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_int32_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: i32,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_uint32_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: u32,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_int64_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: i64,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_uint64_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: u64,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_float_to_row(arg1: GrinGraph, arg2: GrinRow, arg3: f32) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_double_to_row(arg1: GrinGraph, arg2: GrinRow, arg3: f64) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_string_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: *const ::std::os::raw::c_char,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_date32_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: i32,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_time32_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: i32,
    ) -> bool;

    #[cfg(feature = "grin_enable_row")]
    #[allow(unused)]
    pub fn grin_insert_timestamp64_to_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: i64,
    ) -> bool;

    #[cfg(all(feature = "grin_enable_row", feature = "grin_trait_const_value_ptr"))]
    #[allow(unused)]
    pub fn grin_get_value_from_row(
        arg1: GrinGraph,
        arg2: GrinRow,
        arg3: GrinDatatype,
        arg4: usize,
    ) -> *const ::std::os::raw::c_void;

    #[doc = " @brief Get row of values for the entire property list of a vertex.\n Later users can get property values from the row using APIs like\n ``grin_get_int32_from_row``.\n However this two-step value getting is not recommended if the user\n only wants to get the value of one property, in which case, the user\n should use APIs like ``grin_get_vertex_property_value_of_int32``.\n @param GrinGraph The graph\n @param GrinVertex The vertex"]
    #[cfg(all(feature = "grin_with_vertex_property", feature = "grin_enable_row"))]
    #[allow(unused)]
    pub fn grin_get_vertex_row(arg1: GrinGraph, arg2: GrinVertex) -> GrinRow;

    #[cfg(all(feature = "grin_with_edge_property", feature = "grin_enable_row"))]
    #[allow(unused)]
    pub fn grin_get_edge_row(arg1: GrinGraph, arg2: GrinEdge) -> GrinRow;

    #[doc = " @brief Get the vertex number of a given type in the graph.\n This API is only available for property graph.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @return The vertex number."]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_num_by_type(arg1: GrinGraph, arg2: GrinVertexType) -> usize;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_num_by_type(arg1: GrinGraph, arg2: GrinEdgeType) -> usize;

    #[doc = " @brief Get the vertex list of a given type.\n This API is only available for property graph.\n To get a vertex list chain of all types, using APIs in GRIN extension.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @return The vertex list of the given type."]
    #[cfg(all(feature = "grin_enable_vertex_list", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_list_by_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> GrinVertexList;

    #[cfg(all(feature = "grin_enable_edge_list", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_edge_list_by_type(arg1: GrinGraph, arg2: GrinEdgeType) -> GrinEdgeList;

    #[doc = " @brief Get the adjacent list of given direction, vertex and edge type.\n This API is only available for property graph.\n To get a adjacent list chain of all types, using APIs in GRIN extension.\n @param GrinGraph The graph.\n @param GrinDirection The direction of the adjacent list.\n @param GrinVertex The vertex.\n @return The adjacent list."]
    #[cfg(all(feature = "grin_enable_adjacent_list", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_adjacent_list_by_edge_type(
        arg1: GrinGraph,
        arg2: GrinDirection,
        arg3: GrinVertex,
        arg4: GrinEdgeType,
    ) -> GrinAdjacentList;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_equal_vertex_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinVertexType,
    ) -> bool;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_type(arg1: GrinGraph, arg2: GrinVertex) -> GrinVertexType;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_type(arg1: GrinGraph, arg2: GrinVertexType);

    #[doc = " @brief Get the vertex type list of the graph\n This API is only available for property graph.\n It lists all the vertex types in the graph.\n @param GrinGraph The graph.\n @return The vertex type list."]
    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_list(arg1: GrinGraph) -> GrinVertexTypeList;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_destroy_vertex_type_list(arg1: GrinGraph, arg2: GrinVertexTypeList);

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_create_vertex_type_list(arg1: GrinGraph) -> GrinVertexTypeList;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_insert_vertex_type_to_list(
        arg1: GrinGraph,
        arg2: GrinVertexTypeList,
        arg3: GrinVertexType,
    ) -> bool;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_list_size(arg1: GrinGraph, arg2: GrinVertexTypeList) -> usize;

    #[cfg(feature = "grin_with_vertex_property")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_from_list(
        arg1: GrinGraph,
        arg2: GrinVertexTypeList,
        arg3: usize,
    ) -> GrinVertexType;

    #[doc = " @brief Get the vertex type name.\n This API is enabled by ``GRIN_WITH_VERTEX_TYPE_NAME``,\n meaning that the graph has a unique name for each vertex type.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @return The vertex type name of string."]
    #[cfg(feature = "grin_with_vertex_type_name")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_name(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> *const ::std::os::raw::c_char;

    #[doc = " @brief Get the vertex type by name.\n This API is enabled by ``GRIN_WITH_VERTEX_TYPE_NAME``,\n meaning that the graph has a unique name for each vertex type.\n @param GrinGraph The graph.\n @param name The vertex type name.\n @return The vertex type."]
    #[cfg(feature = "grin_with_vertex_type_name")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_by_name(
        arg1: GrinGraph,
        name: *const ::std::os::raw::c_char,
    ) -> GrinVertexType;

    #[doc = " @brief Get the vertex type id.\n This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE``,\n meaning that the graph has naturally increasing ids for vertex types.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @return The vertex type id."]
    #[cfg(feature = "grin_trait_natural_id_for_vertex_type")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_id(arg1: GrinGraph, arg2: GrinVertexType)
        -> GrinVertexTypeId;

    #[doc = " @brief Get the vertex type by id.\n This API is enabled by ``GRIN_TRAIT_NATURAL_ID_FOR_VERTEX_TYPE``,\n meaning that the graph has naturally increasing ids for vertex types.\n @param GrinGraph The graph.\n @param GrinVertexTypeId The vertex type id.\n @return The vertex type."]
    #[cfg(feature = "grin_trait_natural_id_for_vertex_type")]
    #[allow(unused)]
    pub fn grin_get_vertex_type_by_id(
        arg1: GrinGraph,
        arg2: GrinVertexTypeId,
    ) -> GrinVertexType;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_equal_edge_type(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        arg3: GrinEdgeType,
    ) -> bool;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_type(arg1: GrinGraph, arg2: GrinEdge) -> GrinEdgeType;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_destroy_edge_type(arg1: GrinGraph, arg2: GrinEdgeType);

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_type_list(arg1: GrinGraph) -> GrinEdgeTypeList;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_destroy_edge_type_list(arg1: GrinGraph, arg2: GrinEdgeTypeList);

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_create_edge_type_list(arg1: GrinGraph) -> GrinEdgeTypeList;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_insert_edge_type_to_list(
        arg1: GrinGraph,
        arg2: GrinEdgeTypeList,
        arg3: GrinEdgeType,
    ) -> bool;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_type_list_size(arg1: GrinGraph, arg2: GrinEdgeTypeList) -> usize;

    #[cfg(feature = "grin_with_edge_property")]
    #[allow(unused)]
    pub fn grin_get_edge_type_from_list(
        arg1: GrinGraph,
        arg2: GrinEdgeTypeList,
        arg3: usize,
    ) -> GrinEdgeType;

    #[cfg(feature = "grin_with_edge_type_name")]
    #[allow(unused)]
    pub fn grin_get_edge_type_name(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> *const ::std::os::raw::c_char;

    #[cfg(feature = "grin_with_edge_type_name")]
    #[allow(unused)]
    pub fn grin_get_edge_type_by_name(
        arg1: GrinGraph,
        arg2: *const ::std::os::raw::c_char,
    ) -> GrinEdgeType;

    #[cfg(feature = "grin_trait_natural_id_for_edge_type")]
    #[allow(unused)]
    pub fn grin_get_edge_type_id(arg1: GrinGraph, arg2: GrinEdgeType) -> GrinEdgeTypeId;

    #[cfg(feature = "grin_trait_natural_id_for_edge_type")]
    #[allow(unused)]
    pub fn grin_get_edge_type_by_id(arg1: GrinGraph, arg2: GrinEdgeTypeId) -> GrinEdgeType;

    #[doc = " @brief Get source vertex types related to an edge type.\n GRIN assumes the relation between edge type and pairs of vertex types is\n many-to-many.\n To return the related pairs of vertex types, GRIN provides two APIs to get\n the src and dst vertex types respectively.\n The returned vertex type lists are of the same size,\n and the src/dst vertex types are aligned with their positions in the lists.\n @param GrinGraph The graph.\n @param GrinEdgeType The edge type.\n @return The vertex type list of source."]
    #[cfg(all(feature = "grin_with_vertex_property", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_src_types_by_edge_type(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> GrinVertexTypeList;

    #[doc = " @brief Get destination vertex types related to an edge type.\n GRIN assumes the relation between edge type and pairs of vertex types is\n many-to-many.\n To return the related pairs of vertex types, GRIN provides two APIs to get\n the src and dst vertex types respectively.\n The returned vertex type lists are of the same size,\n and the src/dst vertex types are aligned with their positions in the lists.\n @param GrinGraph The graph.\n @param GrinEdgeType The edge type.\n @return The vertex type list of destination."]
    #[cfg(all(feature = "grin_with_vertex_property", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_dst_types_by_edge_type(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
    ) -> GrinVertexTypeList;

    #[doc = " @brief Get edge types related to a pair of vertex types.\n GRIN assumes the relation between edge type and pairs of vertex types is\n many-to-many.\n @param GrinGraph The graph.\n @param GrinVertexType The source vertex type.\n @param GrinVertexType The destination vertex type.\n @return The related edge type list."]
    #[cfg(all(feature = "grin_with_vertex_property", feature = "grin_with_edge_property"))]
    #[allow(unused)]
    pub fn grin_get_edge_types_by_vertex_type_pair(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinVertexType,
    ) -> GrinEdgeTypeList;

    #[cfg(any(feature = "grin_with_vertex_label", feature = "grin_with_edge_label"))]
    #[allow(unused)]
    pub fn grin_get_label_by_name(
        arg1: GrinGraph,
        arg2: *const ::std::os::raw::c_char,
    ) -> GrinLabel;

    #[cfg(any(feature = "grin_with_vertex_label", feature = "grin_with_edge_label"))]
    #[allow(unused)]
    pub fn grin_destroy_label(arg1: GrinGraph, arg2: GrinLabel);

    #[cfg(any(feature = "grin_with_vertex_label", feature = "grin_with_edge_label"))]
    #[allow(unused)]
    pub fn grin_get_label_name(arg1: GrinGraph, arg2: GrinLabel)
        -> *const ::std::os::raw::c_char;

    #[cfg(any(feature = "grin_with_vertex_label", feature = "grin_with_edge_label"))]
    #[allow(unused)]
    pub fn grin_destroy_label_list(arg1: GrinGraph, arg2: GrinLabelList);

    #[cfg(any(feature = "grin_with_vertex_label", feature = "grin_with_edge_label"))]
    #[allow(unused)]
    pub fn grin_get_label_list_size(arg1: GrinGraph, arg2: GrinLabelList) -> usize;

    #[cfg(any(feature = "grin_with_vertex_label", feature = "grin_with_edge_label"))]
    #[allow(unused)]
    pub fn grin_get_label_from_list(
        arg1: GrinGraph,
        arg2: GrinLabelList,
        arg3: usize,
    ) -> GrinLabel;

    #[doc = " @brief assign a label to a vertex\n @param GrinGraph the graph\n @param GrinLabel the label\n @param GrinVertex the vertex\n @return whether succeed"]
    #[cfg(feature = "grin_with_vertex_label")]
    #[allow(unused)]
    pub fn grin_assign_label_to_vertex(
        arg1: GrinGraph,
        arg2: GrinLabel,
        arg3: GrinVertex,
    ) -> bool;

    #[doc = " @brief get the label list of a vertex\n @param GrinGraph the graph\n @param GrinVertex the vertex"]
    #[cfg(feature = "grin_with_vertex_label")]
    #[allow(unused)]
    pub fn grin_get_vertex_label_list(arg1: GrinGraph, arg2: GrinVertex) -> GrinLabelList;

    #[doc = " @brief get the vertex list by label\n @param GrinGraph the graph\n @param GrinLabel the label"]
    #[cfg(feature = "grin_with_vertex_label")]
    #[allow(unused)]
    pub fn grin_get_vertex_list_by_label(arg1: GrinGraph, arg2: GrinLabel) -> GrinVertexList;

    #[doc = " @brief filtering an existing vertex list by label\n @param GrinVertexList the existing vertex list\n @param GrinLabel the label"]
    #[cfg(feature = "grin_with_vertex_label")]
    #[allow(unused)]
    pub fn grin_select_label_for_vertex_list(
        arg1: GrinGraph,
        arg2: GrinLabel,
        arg3: GrinVertexList,
    ) -> GrinVertexList;

    #[doc = " @brief assign a label to a edge\n @param GrinGraph the graph\n @param GrinLabel the label\n @param GrinEdge the edge\n @return whether succeed"]
    #[cfg(feature = "grin_with_edge_label")]
    #[allow(unused)]
    pub fn grin_assign_label_to_edge(arg1: GrinGraph, arg2: GrinLabel, arg3: GrinEdge) -> bool;

    #[doc = " @brief get the label list of a edge\n @param GrinGraph the graph\n @param GrinEdge the edge"]
    #[cfg(feature = "grin_with_edge_label")]
    #[allow(unused)]
    pub fn grin_get_edge_label_list(arg1: GrinGraph, arg2: GrinEdge) -> GrinLabelList;

    #[doc = " @brief get the edge list by label\n @param GrinGraph the graph\n @param GrinLabel the label"]
    #[cfg(feature = "grin_with_edge_label")]
    #[allow(unused)]
    pub fn grin_get_edge_list_by_label(arg1: GrinGraph, arg2: GrinLabel) -> GrinEdgeList;

    #[doc = " @brief filtering an existing edge list by label\n @param GrinEdgeList the existing edge list\n @param GrinLabel the label"]
    #[cfg(feature = "grin_with_edge_label")]
    #[allow(unused)]
    pub fn grin_select_label_for_edge_list(
        arg1: GrinGraph,
        arg2: GrinLabel,
        arg3: GrinEdgeList,
    ) -> GrinEdgeList;

    #[cfg(feature = "grin_assume_all_vertex_list_sorted")]
    #[allow(unused)]
    pub fn grin_smaller_vertex(arg1: GrinGraph, arg2: GrinVertex, arg3: GrinVertex) -> bool;

    #[doc = " @brief Get the position of a vertex in a sorted list\n caller must guarantee the input vertex list is sorted to get the correct result\n @param GrinGraph The graph\n @param GrinVertexList The sorted vertex list\n @param GrinVertex The vertex to find\n @return The position of the vertex"]
    #[cfg(all(feature = "grin_assume_all_vertex_list_sorted", feature = "grin_enable_vertex_list_array"))]
    #[allow(unused)]
    pub fn grin_get_position_of_vertex_from_sorted_list(
        arg1: GrinGraph,
        arg2: GrinVertexList,
        arg3: GrinVertex,
    ) -> usize;

    #[doc = " @brief Get the int64 internal id of a vertex\n @param GrinGraph The graph\n @param GrinVertex The vertex\n @return The int64 internal id of the vertex"]
    #[cfg(all(feature = "grin_enable_vertex_internal_id_index", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_internal_id_by_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinVertex,
    ) -> i64;

    #[doc = " @brief Get the vertex by internal id under type\n @param GrinGraph The graph\n @param GrinVertexType The vertex type\n @param id The internal id of the vertex under type\n @return The vertex"]
    #[cfg(all(feature = "grin_enable_vertex_internal_id_index", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_by_internal_id_by_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        id: i64,
    ) -> GrinVertex;

    #[doc = " @brief Get the upper bound of internal id under type.\n @param GrinGraph The graph\n @param GrinVertexType The vertex type\n @return The upper bound of internal id under type"]
    #[cfg(all(feature = "grin_enable_vertex_internal_id_index", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_internal_id_upper_bound_by_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> i64;

    #[doc = " @brief Get the lower bound internal id under type.\n @param GrinGraph The graph\n @param GrinVertexType The vertex type\n @return The lower bound internal id under type"]
    #[cfg(all(feature = "grin_enable_vertex_internal_id_index", feature = "grin_with_vertex_property"))]
    #[allow(unused)]
    pub fn grin_get_vertex_internal_id_lower_bound_by_type(
        arg1: GrinGraph,
        arg2: GrinVertexType,
    ) -> i64;

    #[doc = " @brief Get the vertex by primary keys row.\n The values in the row must be in the same order as the primary keys\n properties, which can be obtained by ``grin_get_primary_keys_by_vertex_type``.\n @param GrinGraph The graph.\n @param GrinVertexType The vertex type.\n @param GrinRow The values row of primary keys properties.\n @return The vertex."]
    #[cfg(all(feature = "grin_enable_vertex_pk_index", feature = "grin_enable_vertex_primary_keys"))]
    #[allow(unused)]
    pub fn grin_get_vertex_by_primary_keys_row(
        arg1: GrinGraph,
        arg2: GrinVertexType,
        arg3: GrinRow,
    ) -> GrinVertex;

    #[cfg(all(feature = "grin_enable_edge_pk_index", feature = "grin_enable_edge_primary_keys"))]
    #[allow(unused)]
    pub fn grin_get_edge_by_primary_keys_row(
        arg1: GrinGraph,
        arg2: GrinEdgeType,
        arg3: GrinRow,
    ) -> GrinEdge;

    pub static mut grin_error_code: GrinErrorCode;

    #[doc = " @brief Get the last error code.\n The error code is thread local.\n Currently users only need to check the error code when using\n getting-value APIs whose return has no predefined invalid value."]
    #[allow(unused)]
    pub fn grin_get_last_error_code() -> GrinErrorCode;

    #[doc = " @brief Get the static feature prototype message of the storage.\n This proto describes the features of the storage, such as whether\n it supports property graph or partitioned graph.\n @return The serialized proto message."]
    #[allow(unused)]
    pub fn grin_get_static_storage_feature_msg() -> *const ::std::os::raw::c_char;
}
