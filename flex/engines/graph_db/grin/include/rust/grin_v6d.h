#define GRIN_FEATURES_ENABLE_V6D
#define GRIN_RUST_CODEGEN

#include "storage/v6d/predefine.h"

#ifdef GRIN_RUST_CODEGEN
/// GRIN_FEATURES_ENABLE_V6D
/// RUST_KEEP pub const GRIN_NULL_DATATYPE: GrinDatatype = GRIN_DATATYPE_UNDEFINED;
/// RUST_KEEP pub const GRIN_NULL_GRAPH: GrinGraph = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_VERTEX: GrinVertex = u64::MAX;
/// RUST_KEEP pub const GRIN_NULL_EDGE: GrinEdge = GrinEdge{src: u64::MAX, dst: u64::MAX, dir: GRIN_DIRECTION_BOTH, etype: u32::MAX, eid: u64::MAX};
/// RUST_KEEP pub const GRIN_NULL_VERTEX_LIST: GrinVertexList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_VERTEX_LIST_ITERATOR: GrinVertexListIterator = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_ADJACENT_LIST: GrinAdjacentList = GrinAdjacentList{begin: std::ptr::null(), end: std::ptr::null(), vid: u64::MAX, dir: GRIN_DIRECTION_BOTH, etype: u32::MAX};
/// RUST_KEEP pub const GRIN_NULL_ADJACENT_LIST_ITERATOR: GrinAdjacentListIterator = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_PARTITIONED_GRAPH: GrinPartitionedGraph = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_PARTITION: GrinPartition = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_PARTITION_LIST: GrinPartitionList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_PARTITION_ID: GrinPartitionId = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_VERTEX_REF: GrinVertexRef = -1;
/// RUST_KEEP pub const GRIN_NULL_VERTEX_TYPE: GrinVertexType = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_VERTEX_TYPE_LIST: GrinVertexTypeList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_VERTEX_PROPERTY: GrinVertexProperty = u64::MAX;
/// RUST_KEEP pub const GRIN_NULL_VERTEX_PROPERTY_LIST: GrinVertexPropertyList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_VERTEX_TYPE_ID: GrinVertexTypeId = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_VERTEX_PROPERTY_ID: GrinVertexPropertyId = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_EDGE_TYPE: GrinEdgeType = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_EDGE_TYPE_LIST: GrinEdgeTypeList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_VEV_TYPE: GrinVevType = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_VEV_TYPE_LIST: GrinVevTypeList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_EDGE_PROPERTY: GrinEdgeProperty = u64::MAX;
/// RUST_KEEP pub const GRIN_NULL_EDGE_PROPERTY_LIST: GrinEdgePropertyList = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_EDGE_TYPE_ID: GrinEdgeTypeId = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_EDGE_PROPERTY_ID: GrinEdgePropertyId = u32::MAX;
/// RUST_KEEP pub const GRIN_NULL_ROW: GrinRow = std::ptr::null_mut();
/// RUST_KEEP pub const GRIN_NULL_SIZE: u32 = u32::MAX;
int __rust_keep_grin_null;
#endif
