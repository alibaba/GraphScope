use std::ffi::CString;
use std::mem;
use std::sync::Arc;

use ir_common::generated::schema as schema_pb;

use crate::ffi::{
    get_schema, GraphHandle, GraphId, PropertyId, PropertyType, SchemaHandle, WriteNativeProperty,
};

// TODO: `VertexId`/`EdgeId` should be identical to `ID`, and `LabelId` should be identical to `KeyId` in Runtime
type VertexId = u64;
type EdgeId = u64;
type LabelId = i32;

type GraphBuilder = *const ::libc::c_void;
type VertexTypeBuilder = *const ::libc::c_void;
type EdgeTypeBuilder = *const ::libc::c_void;

extern "C" {
    /// APIs for writing a vineyard graph
    pub fn get_graph_builder(graph_name: *const ::libc::c_char, index: i32) -> GraphBuilder;
    pub fn initialize_graph_builder(builder: GraphBuilder, schema: SchemaHandle);
    pub fn add_vertex(
        graph_builder: GraphBuilder, id: VertexId, label_id: LabelId, property_size: usize,
        properties: *const WriteNativeProperty,
    );
    pub fn add_edge(
        graph_builder: GraphBuilder, edge_id: EdgeId, src_id: VertexId, dst_id: VertexId, label: LabelId,
        src_label: LabelId, dst_label: LabelId, property_size: usize,
        properties: *const WriteNativeProperty,
    );
    pub fn add_vertices(
        graph_builder: GraphBuilder, vertex_size: usize, ids: *const VertexId, label_ids: *const LabelId,
        property_sizes: *const usize, properties: *const WriteNativeProperty,
    );
    pub fn add_edges(
        graph_builder: GraphBuilder, edge_size: usize, edge_ids: *const EdgeId, src_ids: *const VertexId,
        dst_ids: *const VertexId, labels: *const LabelId, src_labels: *const LabelId,
        dst_labels: *const LabelId, property_sizes: *const usize, properties: *const WriteNativeProperty,
    );
    pub fn build_vertices(builder: GraphBuilder);
    pub fn build_edges(builder: GraphBuilder);
    pub fn destroy(builder: GraphBuilder);

    /// APIs for building a vineyard graph schema
    fn create_schema_builder() -> SchemaHandle;
    fn build_vertex_type(
        schema: SchemaHandle, label: LabelId, name: *const ::libc::c_char,
    ) -> VertexTypeBuilder;
    fn build_edge_type(
        schema: SchemaHandle, label: LabelId, name: *const ::libc::c_char,
    ) -> EdgeTypeBuilder;

    fn build_vertex_property(
        vertex: VertexTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType,
    );
    fn build_edge_property(
        edge: EdgeTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType,
    );
    fn build_vertex_primary_keys(
        vertex: VertexTypeBuilder, key_count: usize, key_name_list: *const *const ::libc::c_char,
    );
    fn build_edge_relation(edge: EdgeTypeBuilder, src: *const ::libc::c_char, dst: *const ::libc::c_char);
    fn finish_build_vertex(vertex: VertexTypeBuilder);
    fn finish_build_edge(edge: EdgeTypeBuilder);
    fn finish_build_schema(schema: SchemaHandle);
}

// TODO: defined in extern?
pub fn build_vineyard_schema(_schema: &schema_pb::Schema) -> SchemaHandle {
    todo!()
}
