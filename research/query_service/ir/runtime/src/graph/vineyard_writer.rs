//
//! Copyright 2022 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::ffi::CString;

use ffi::ffi::{GraphHandle, WriteNativeProperty};
use ffi::graph_builder_ffi::{build_vineyard_schema, destroy, get_graph_builder, initialize_graph_builder};
use ir_common::generated::schema as schema_pb;
use pegasus::api::function::FnResult;

use crate::graph::element::{Edge, Vertex};
use crate::graph::WriteGraphProxy;

pub struct VineyardGraphWriter {
    graph: GraphHandle,
}

impl VineyardGraphWriter {
    pub fn new(graph_name: String, schema: &schema_pb::Schema, index: i32) -> Self {
        let name = CString::new(graph_name).unwrap();
        let graph = unsafe { get_graph_builder(name.as_ptr(), index) };
        let schema_handle = build_vineyard_schema(schema);
        unsafe { initialize_graph_builder(graph, schema_handle) };
        VineyardGraphWriter { graph }
    }

    // pub fn add_vertex(&self, vertex_id: VertexId, label_id: LabelId, properties: Vec<WriteNativeProperty>) {
    //     unsafe {
    //         add_vertex(self.graph, vertex_id, label_id, properties.len(), properties.as_ptr());
    //     }
    // }
    //
    // pub fn add_edge(
    //     &self, edge_id: EdgeId, src_id: VertexId, dst_id: VertexId, edge_label: LabelId,
    //     src_label: LabelId, dst_label: LabelId, properties: Vec<WriteNativeProperty>,
    // ) {
    //     unsafe {
    //         add_edge(
    //             self.graph,
    //             edge_id,
    //             src_id,
    //             dst_id,
    //             edge_label,
    //             src_label,
    //             dst_label,
    //             properties.len(),
    //             properties.as_ptr(),
    //         );
    //     }
    // }
    //
    // pub fn add_vertices(
    //     &self, vertex_ids: Vec<VertexId>, vertex_labels_ids: Vec<LabelId>,
    //     properties: Vec<Vec<WriteNativeProperty>>,
    // ) {
    //     let mut merge_properties: Vec<WriteNativeProperty> = vec![];
    //     let mut property_sizes: Vec<usize> = vec![];
    //     for mut prop_vec in properties {
    //         property_sizes.push(prop_vec.len());
    //         merge_properties.append(&mut prop_vec);
    //     }
    //     info!("Before add vertex to vineyard with vertex ids {:?} vertex label ids {:?} property size list {:?}",
    //           &vertex_ids,
    //           &vertex_labels_ids,
    //           &property_sizes);
    //     unsafe {
    //         add_vertices(
    //             self.graph,
    //             vertex_ids.len(),
    //             vertex_ids.as_ptr(),
    //             vertex_labels_ids.as_ptr(),
    //             property_sizes.as_ptr(),
    //             merge_properties.as_ptr(),
    //         );
    //     }
    // }
    //
    // pub fn add_edges(
    //     &self, edge_ids: Vec<EdgeId>, src_ids: Vec<VertexId>, dst_ids: Vec<VertexId>,
    //     edge_labels: Vec<LabelId>, src_labels: Vec<LabelId>, dst_labels: Vec<LabelId>,
    //     properties: Vec<Vec<WriteNativeProperty>>,
    // ) {
    //     let mut merge_properties: Vec<WriteNativeProperty> = vec![];
    //     let mut property_sizes: Vec<usize> = vec![];
    //     for mut prop_vec in properties {
    //         property_sizes.push(prop_vec.len());
    //         merge_properties.append(&mut prop_vec);
    //     }
    //
    //     unsafe {
    //         add_edges(
    //             self.graph,
    //             edge_ids.len(),
    //             edge_ids.as_ptr(),
    //             src_ids.as_ptr(),
    //             dst_ids.as_ptr(),
    //             edge_labels.as_ptr(),
    //             src_labels.as_ptr(),
    //             dst_labels.as_ptr(),
    //             property_sizes.as_ptr(),
    //             merge_properties.as_ptr(),
    //         );
    //     }
    // }
    //
    // pub fn finish_vertice(&self) {
    //     info!("finish vertice");
    //     unsafe {
    //         build_vertice(self.graph);
    //     }
    // }
    //
    // pub fn finish_edge(&self) {
    //     info!("finish edge");
    //     unsafe {
    //         build_edges(self.graph);
    //     }
    // }
}

impl WriteGraphProxy for VineyardGraphWriter {
    fn add_vertex(&mut self, vertex: Vertex) -> FnResult<()> {
        unimplemented!()
    }

    fn add_vertices(&mut self, vertices: Vec<Vertex>) -> FnResult<()> {
        unimplemented!()
    }

    fn add_edge(&mut self, edge: Edge) -> FnResult<()> {
        unimplemented!()
    }

    fn add_edges(&mut self, edges: Vec<Edge>) -> FnResult<()> {
        unimplemented!()
    }

    fn finish(&mut self) -> FnResult<()> {
        unimplemented!()
    }
}

impl Drop for VineyardGraphWriter {
    fn drop(&mut self) {
        unsafe {
            destroy(self.graph);
        }
    }
}

unsafe impl Send for VineyardGraphWriter {}

unsafe impl Sync for VineyardGraphWriter {}
