//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use maxgraph_store::api::{VertexId, LabelId, EdgeId, Vertex};
use super::ffi::{WriteNativeProperty, GraphId, GraphHandle, SchemaHandle, PropertyId, get_schema};
use store::ffi::PropertyType;
use std::sync::Arc;
use std::mem;
use maxgraph_store::schema::Schema;
use maxgraph_store::schema::prelude::{DataType, Type};
use std::ffi::CString;
use dataflow::operator::unarystep::vineyard::VineyardStreamOperator;
use maxgraph_store::api::prelude::Property;
use maxgraph_common::proto::query_flow::RuntimeGraphSchemaProto;

type GraphBuilder = *const ::libc::c_void;
type VertexTypeBuilder = *const ::libc::c_void;
type EdgeTypeBuilder = *const ::libc::c_void;
type InstanceId = u64;

extern {
    fn create_graph_builder(graph_name: *const ::libc::c_char, schema: SchemaHandle, index: i32) -> GraphBuilder;
    fn get_builder_id(graph_builder: GraphBuilder, graph_id: *mut GraphId, instance_id: *mut InstanceId);

    fn build_global_graph_stream(graph_name: *const ::libc::c_char, size: usize, object_ids: *const GraphId, instance_ids: *const InstanceId) -> GraphId;
    fn get_graph_builder(graph_name: *const ::libc::c_char, index: i32) -> GraphBuilder;

    fn add_vertex(graph_builder: GraphBuilder, id: VertexId, labelId: LabelId,
                  property_size: usize, properties: *const WriteNativeProperty);

    fn add_edge(graph_builder: GraphBuilder,
                edge_id: EdgeId, src_id: VertexId, dst_id: VertexId,
                label: LabelId, src_label: LabelId, dst_label: LabelId,
                property_size: usize, properties: *const WriteNativeProperty);

    fn add_vertices(graph_builder: GraphBuilder,
                    vertex_size: usize, ids: *const VertexId, label_ids: *const LabelId,
                    property_sizes: *const usize, properties: *const WriteNativeProperty);

    fn add_edges(graph_builder: GraphBuilder,
                 edge_size: usize,
                 edgeids: *const EdgeId, src_id: *const VertexId, dst_id: *const VertexId,
                 labels: *const LabelId, src_labels: *const LabelId, dst_labels: *const LabelId,
                 property_sizes: *const usize, properties: *const WriteNativeProperty);

    fn build_vertice(builder: GraphBuilder);
    fn build_edges(builder: GraphBuilder);
    fn destroy(builder: GraphBuilder);

    fn create_schema_builder() -> SchemaHandle;
    fn build_vertex_type(schema: SchemaHandle, label: LabelId, name: *const ::libc::c_char) -> VertexTypeBuilder;
    fn build_edge_type(schema: SchemaHandle, label: LabelId, name: *const ::libc::c_char) -> EdgeTypeBuilder;

    fn build_vertex_property(vertex: VertexTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType);
    fn build_edge_property(edge: EdgeTypeBuilder, id: PropertyId, name: *const ::libc::c_char, prop_type: PropertyType);
    fn build_vertex_primary_keys(vertex: VertexTypeBuilder, key_count: usize, key_name_list: *const *const ::libc::c_char);
    fn build_edge_relation(edge: EdgeTypeBuilder, src: *const ::libc::c_char, dst: *const ::libc::c_char);
    fn finish_build_vertex(vertex: VertexTypeBuilder);
    fn finish_build_edge(edge: EdgeTypeBuilder);
    fn finish_build_schema(schema: SchemaHandle);
}

pub struct VineyardGraphBuilder {
    graph: GraphHandle,
}

fn build_vineyard_schema(schema: &RuntimeGraphSchemaProto) -> SchemaHandle {
    let schema_handle = unsafe { create_schema_builder() };
    for vertex_type in schema.get_vertex_types() {
        let label = vertex_type.get_label_id() as LabelId;
        let name = CString::new(vertex_type.get_label_name().to_owned()).unwrap();
        let vertex_builder = unsafe { build_vertex_type(schema_handle, label, name.as_ptr()) };
        for property in vertex_type.get_properties() {
            let prop_name = property.get_name();
            let prop_id = property.get_id();
            let data_type = DataType::from(property.get_data_type());
            let prop_type = PropertyType::from_data_type(&data_type);
            let prop_name_cs = CString::new(prop_name).unwrap();
            unsafe { build_vertex_property(vertex_builder, prop_id, prop_name_cs.as_ptr(), prop_type) };
        }
        let mut primary_key_list = vec![];
        let mut primary_key_ptr_list = vec![];
        for primary_key in vertex_type.get_primary_keys() {
            let ckey = CString::new(primary_key.to_owned()).unwrap();
            primary_key_ptr_list.push(ckey.as_ptr());
            primary_key_list.push(ckey);
        }
        unsafe { build_vertex_primary_keys(vertex_builder, primary_key_list.len(), primary_key_ptr_list.as_ptr()) };
        unsafe { finish_build_vertex(vertex_builder) };
    }
    for edge_type in schema.get_edge_types() {
        let label = edge_type.get_label_id() as LabelId;
        let name = CString::new(edge_type.get_label_name().to_owned()).unwrap();
        let edge_builder = unsafe { build_edge_type(schema_handle, label, name.as_ptr()) };
        for property in edge_type.get_properties() {
            let prop_name = property.get_name();
            let prop_id = property.get_id();
            let data_type = DataType::from(property.get_data_type());
            let prop_type = PropertyType::from_data_type(&data_type);
            let prop_name_cs = CString::new(prop_name).unwrap();
            unsafe { build_edge_property(edge_builder, prop_id, prop_name_cs.as_ptr(), prop_type) };
        }
        for relation in edge_type.get_relations() {
            let src = CString::new(relation.get_source_label().to_owned()).unwrap();
            let dst = CString::new(relation.get_target_label().to_owned()).unwrap();
            unsafe { build_edge_relation(edge_builder, src.as_ptr(), dst.as_ptr()) };
        }
        unsafe { finish_build_edge(edge_builder) };
    }

    unsafe { finish_build_schema(schema_handle) };

    return schema_handle;
}

impl VineyardGraphBuilder {
    pub fn new(graph_name: String,
               schema: &RuntimeGraphSchemaProto,
               index: i32) -> Self {
        let schema_handle = build_vineyard_schema(schema);
        let c_name = CString::new(graph_name).unwrap();
        info!("Start to create vineyard graph builder");
        let graph = unsafe { create_graph_builder(c_name.as_ptr(), schema_handle, index) };
        info!("Create vineyard graph builder finish");
        VineyardGraphBuilder {
            graph,
        }
    }

    pub fn get_graph_instance_id(&self) -> (GraphId, InstanceId) {
        let mut graph_id = 0;
        let mut instance_id = 0;
        unsafe { get_builder_id(self.graph, &mut graph_id, &mut instance_id) };
        return (graph_id, instance_id);
    }
}

impl Drop for VineyardGraphBuilder {
    fn drop(&mut self) {
        unsafe { destroy(self.graph); }
    }
}

pub struct VineyardGraphStream {}

impl VineyardGraphStream {
    pub fn new() -> Self {
        VineyardGraphStream {}
    }

    pub fn build_global_stream(&self,
                               graph_name: String,
                               graph_instance_list: &Vec<(GraphId, InstanceId)>) -> i64 {
        let name = CString::new(graph_name).unwrap();
        let mut graph_id_list = Vec::new();
        let mut instance_id_list = Vec::new();
        for (graph_id, instance_id) in graph_instance_list.iter() {
            graph_id_list.push(*graph_id);
            instance_id_list.push(*instance_id);
        }
        let count = graph_instance_list.len();
        info!("start build global count = {}, graph_id_list = {:?}, instance_id_list = {:?}",
            count, graph_id_list, instance_id_list);
        let global_id = unsafe {
            build_global_graph_stream(name.as_ptr(),
                                      count,
                                      graph_id_list.as_ptr(),
                                      instance_id_list.as_ptr())
        };
        info!("build global graph id {}", global_id);
        return global_id;
    }
}

pub struct VineyardGraphWriter {
    graph: GraphHandle,
}

impl VineyardGraphWriter {
    pub fn new(graph_name: String, index: i32) -> Self {
        let name = CString::new(graph_name).unwrap();
        let graph = unsafe { get_graph_builder(name.as_ptr(), index) };
        VineyardGraphWriter {
            graph
        }
    }

    pub fn add_vertex(&self, vertex_id: VertexId, label_id: LabelId, properties: Vec<WriteNativeProperty>) {
        unsafe {
            add_vertex(self.graph, vertex_id, label_id, properties.len(), properties.as_ptr());
        }
    }

    pub fn add_edge(&self, edge_id: EdgeId, src_id: VertexId, dst_id: VertexId,
                    edge_label: LabelId, src_label: LabelId, dst_label: LabelId, properties: Vec<WriteNativeProperty>) {
        unsafe {
            add_edge(self.graph,
                     edge_id,
                     src_id,
                     dst_id,
                     edge_label,
                     src_label,
                     dst_label,
                     properties.len(),
                     properties.as_ptr());
        }
    }

    pub fn add_vertices(&self, vertex_ids: Vec<VertexId>, vertex_labels_ids: Vec<LabelId>, properties: Vec<Vec<WriteNativeProperty>>) {
        let mut merge_properties: Vec<WriteNativeProperty> = vec![];
        let mut property_sizes: Vec<usize> = vec![];
        for mut prop_vec in properties {
            property_sizes.push(prop_vec.len());
            merge_properties.append(&mut prop_vec);
        }
        _info!("Before add vertex to vineyard with vertex ids {:?} vertex label ids {:?} property size list {:?}",
            &vertex_ids,
            &vertex_labels_ids,
            &property_sizes);
        unsafe {
            add_vertices(self.graph,
                         vertex_ids.len(),
                         vertex_ids.as_ptr(),
                         vertex_labels_ids.as_ptr(),
                         property_sizes.as_ptr(),
                         merge_properties.as_ptr());
        }
    }

    pub fn add_edges(&self, edge_ids: Vec<EdgeId>,
                     src_ids: Vec<VertexId>, dst_ids: Vec<VertexId>,
                     edge_labels: Vec<LabelId>, src_labels: Vec<LabelId>, dst_labels: Vec<LabelId>,
                     properties: Vec<Vec<WriteNativeProperty>>) {
        let mut merge_properties: Vec<WriteNativeProperty> = vec![];
        let mut property_sizes: Vec<usize> = vec![];
        for mut prop_vec in properties {
            property_sizes.push(prop_vec.len());
            merge_properties.append(&mut prop_vec);
        }

        unsafe {
            add_edges(self.graph,
                      edge_ids.len(),
                      edge_ids.as_ptr(),
                      src_ids.as_ptr(),
                      dst_ids.as_ptr(),
                      edge_labels.as_ptr(),
                      src_labels.as_ptr(),
                      dst_labels.as_ptr(),
                      property_sizes.as_ptr(),
                      merge_properties.as_ptr());
        }
    }

    pub fn finish_vertice(&self) {
        info!("finish vertice");
        unsafe { build_vertice(self.graph); }
    }

    pub fn finish_edge(&self) {
        info!("finish edge");
        unsafe { build_edges(self.graph); }
    }
}

impl Drop for VineyardGraphWriter {
    fn drop(&mut self) {
        unsafe { destroy(self.graph); }
    }
}

unsafe impl Send for VineyardGraphWriter {}

unsafe impl Sync for VineyardGraphWriter {}

#[test]
fn test_add_vertices() {
    let vertex_builder = std::ptr::null();
    let id = CString::new("id".to_owned()).unwrap();
    let name = CString::new("name".to_owned()).unwrap();
    let primary_key_ptr_list = vec![id.as_ptr(), name.as_ptr()];
    let primary_key_list = vec![id, name];
    unsafe { build_vertex_primary_keys(vertex_builder, primary_key_list.len(), primary_key_ptr_list.as_ptr()) };
}
