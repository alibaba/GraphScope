use crate::store_impl::v6d::read_ffi::{
    FFIState, FfiLabelId as LabelId, FfiVertexId as VertexId, PropertyId, PropertyType, SchemaHandle,
    WriteNativeProperty,
};

pub type GraphBuilder = *const ::libc::c_void;
type VertexTypeBuilder = *const ::libc::c_void;
type EdgeTypeBuilder = *const ::libc::c_void;

#[allow(dead_code)]
extern "C" {
    /// APIs for writing a vineyard graph
    pub fn v6d_get_graph_builder(graph_name: *const ::libc::c_char, index: i32) -> GraphBuilder;
    pub fn v6d_initialize_graph_builder(builder: GraphBuilder, schema: SchemaHandle) -> FFIState;
    pub fn v6d_add_vertex(
        graph_builder: GraphBuilder, id: VertexId, label_id: LabelId, property_size: usize,
        properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn v6d_add_edge(
        graph_builder: GraphBuilder, src_id: VertexId, dst_id: VertexId, label: LabelId,
        src_label: LabelId, dst_label: LabelId, property_size: usize,
        properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn v6d_add_vertices(
        graph_builder: GraphBuilder, vertex_size: usize, ids: *const VertexId, label_ids: *const LabelId,
        property_sizes: *const usize, properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn v6d_add_edges(
        graph_builder: GraphBuilder, edge_size: usize, src_ids: *const VertexId, dst_ids: *const VertexId,
        labels: *const LabelId, src_labels: *const LabelId, dst_labels: *const LabelId,
        property_sizes: *const usize, properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn v6d_build_vertices(builder: GraphBuilder) -> FFIState;
    pub fn v6d_build_edges(builder: GraphBuilder) -> FFIState;
    pub fn v6d_destroy(builder: GraphBuilder);

    /// APIs for building a vineyard graph schema
    pub fn v6d_create_schema_builder() -> SchemaHandle;
    pub fn v6d_build_vertex_type(
        schema: SchemaHandle, label: LabelId, name: *const ::libc::c_char,
    ) -> VertexTypeBuilder;
    pub fn v6d_build_edge_type(
        schema: SchemaHandle, label: LabelId, name: *const ::libc::c_char,
    ) -> EdgeTypeBuilder;

    pub fn v6d_build_vertex_property(
        vertex: VertexTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType,
    ) -> FFIState;
    pub fn v6d_build_edge_property(
        edge: EdgeTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType,
    ) -> FFIState;
    pub fn v6d_build_vertex_primary_keys(
        vertex: VertexTypeBuilder, key_count: usize, key_name_list: *const *const ::libc::c_char,
    ) -> FFIState;
    pub fn v6d_build_edge_relation(
        edge: EdgeTypeBuilder, src: *const ::libc::c_char, dst: *const ::libc::c_char,
    ) -> FFIState;
    pub fn v6d_finish_build_vertex(vertex: VertexTypeBuilder) -> FFIState;
    pub fn v6d_finish_build_edge(edge: EdgeTypeBuilder) -> FFIState;
    pub fn v6d_finish_build_schema(schema: SchemaHandle);
}
