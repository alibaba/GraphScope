use std::ffi::CString;
use std::mem;
use std::sync::Arc;

use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;

use crate::ffi::{
    get_schema, FFIState, GraphHandle, GraphId, PropertyId, PropertyType, SchemaHandle, WriteNativeProperty,
};

pub type VertexId = u64;
pub type EdgeId = u64;
pub type LabelId = i32;

type GraphBuilder = *const ::libc::c_void;
type VertexTypeBuilder = *const ::libc::c_void;
type EdgeTypeBuilder = *const ::libc::c_void;

extern "C" {
    /// APIs for writing a vineyard graph
    pub fn get_graph_builder(graph_name: *const ::libc::c_char, index: i32) -> GraphBuilder;
    pub fn initialize_graph_builder(builder: GraphBuilder, schema: SchemaHandle) -> FFIState;
    pub fn add_vertex(
        graph_builder: GraphBuilder, id: VertexId, label_id: LabelId, property_size: usize,
        properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn add_edge(
        graph_builder: GraphBuilder, edge_id: EdgeId, src_id: VertexId, dst_id: VertexId, label: LabelId,
        src_label: LabelId, dst_label: LabelId, property_size: usize,
        properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn add_vertices(
        graph_builder: GraphBuilder, vertex_size: usize, ids: *const VertexId, label_ids: *const LabelId,
        property_sizes: *const usize, properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn add_edges(
        graph_builder: GraphBuilder, edge_size: usize, edge_ids: *const EdgeId, src_ids: *const VertexId,
        dst_ids: *const VertexId, labels: *const LabelId, src_labels: *const LabelId,
        dst_labels: *const LabelId, property_sizes: *const usize, properties: *const WriteNativeProperty,
    ) -> FFIState;
    pub fn build_vertices(builder: GraphBuilder) -> FFIState;
    pub fn build_edges(builder: GraphBuilder) -> FFIState;
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
    ) -> FFIState;
    fn build_edge_property(
        edge: EdgeTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType,
    ) -> FFIState;
    fn build_vertex_primary_keys(
        vertex: VertexTypeBuilder, key_count: usize, key_name_list: *const *const ::libc::c_char,
    ) -> FFIState;
    fn build_edge_relation(
        edge: EdgeTypeBuilder, src: *const ::libc::c_char, dst: *const ::libc::c_char,
    ) -> FFIState;
    fn finish_build_vertex(vertex: VertexTypeBuilder) -> FFIState;
    fn finish_build_edge(edge: EdgeTypeBuilder) -> FFIState;
    fn finish_build_schema(schema: SchemaHandle);
}

pub fn build_vineyard_schema(schema: &schema_pb::Schema) -> SchemaHandle {
    let builder = unsafe { create_schema_builder() };
    for vertex in &schema.entities {
        let label = &vertex.label.as_ref().unwrap();
        let name = CString::new(label.name.to_owned()).unwrap();
        let vbuilder = unsafe { build_vertex_type(builder, label.id, name.as_ptr()) };

        let mut primary_key_list = vec![];
        let mut primary_key_ptr_list = vec![];

        for column in &vertex.columns {
            let key = &column.key.as_ref().unwrap();
            let prop_id = key.id;
            let prop_name = CString::new(key.name.to_owned()).unwrap();
            let prop_type =
                PropertyType::from_data_type(common_pb::DataType::from_i32(column.data_type).unwrap());
            unsafe { build_vertex_property(vbuilder, prop_id, prop_name.as_ptr(), prop_type) };

            if column.is_primary_key {
                primary_key_ptr_list.push(prop_name.as_ptr());
                primary_key_list.push(prop_name);
            }
        }
        unsafe {
            build_vertex_primary_keys(vbuilder, primary_key_list.len(), primary_key_ptr_list.as_ptr());
            finish_build_vertex(vbuilder);
        };
    }

    for edge in &schema.relations {
        let label = &edge.label.as_ref().unwrap();
        let name = CString::new(label.name.to_owned()).unwrap();
        let ebuilder = unsafe { build_edge_type(builder, label.id, name.as_ptr()) };

        for column in &edge.columns {
            let key = &column.key.as_ref().unwrap();
            let prop_id = key.id;
            let prop_name = CString::new(key.name.to_owned()).unwrap();
            let prop_type =
                PropertyType::from_data_type(common_pb::DataType::from_i32(column.data_type).unwrap());
            unsafe { build_edge_property(ebuilder, prop_id, prop_name.as_ptr(), prop_type) };
        }

        for relation in &edge.entity_pairs {
            let src = &relation.src.as_ref().unwrap();
            let src_name = CString::new(src.name.to_owned()).unwrap();
            let dst = &relation.dst.as_ref().unwrap();
            let dst_name = CString::new(dst.name.to_owned()).unwrap();
            unsafe { build_edge_relation(ebuilder, src_name.as_ptr(), dst_name.as_ptr()) };
        }

        unsafe { finish_build_edge(ebuilder) };
    }

    unsafe { finish_build_schema(builder) };
    return builder;
}
