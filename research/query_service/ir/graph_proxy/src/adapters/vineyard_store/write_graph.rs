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

use std::collections::HashMap;
use std::ffi::CString;

use dyn_type::Object;
use ffi::ffi::{GraphHandle, WriteNativeProperty};
use ffi::graph_builder_ffi::{
    add_edge, add_edges, add_vertex, add_vertices, build_edges, build_vertices, build_vineyard_schema,
    destroy, get_graph_builder, initialize_graph_builder,
};
use ir_common::generated::schema as schema_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::api::function::FnResult;

use crate::error::FnExecError;
use crate::graph::element::{Edge, Element, GraphElement, Vertex};
use crate::graph::property::{Details, DynDetails};
use crate::graph::WriteGraphProxy;

#[derive(Clone, Debug)]
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

    fn encode_key(&self, key: Option<&NameOrId>) -> FnResult<KeyId> {
        match key {
            Some(NameOrId::Str(s)) => Err(FnExecError::write_store_error(&format!(
                "do not support string key when writing vineyard {:?}",
                s
            )))?,
            Some(NameOrId::Id(id)) => Ok(*id),
            None => Err(FnExecError::write_store_error("do not support empty key when writing vineyard"))?,
        }
    }

    fn encode_details(&self, details: Option<&DynDetails>) -> FnResult<Vec<WriteNativeProperty>> {
        let properties = details
            .map(|details| details.get_all_properties())
            .unwrap_or(None);
        self.encode_properties(properties)
    }

    fn encode_properties(
        &self, properties: Option<HashMap<NameOrId, Object>>,
    ) -> FnResult<Vec<WriteNativeProperty>> {
        let mut native_properties: Vec<WriteNativeProperty> = vec![];
        if let Some(mut properties) = properties {
            for (prop_key, prop_val) in properties.drain() {
                let prop_id = self.encode_key(Some(&prop_key))?;
                let native_property = WriteNativeProperty::from_object(prop_id, prop_val);
                native_properties.push(native_property);
            }
        }
        Ok(native_properties)
    }
}

impl WriteGraphProxy for VineyardGraphWriter {
    fn add_vertex(&mut self, vertex: Vertex) -> FnResult<()> {
        let vertex_id = vertex.id();
        let label_id = self.encode_key(vertex.label())?;
        let native_properties = self.encode_details(vertex.details())?;
        unsafe {
            add_vertex(
                self.graph,
                vertex_id,
                label_id,
                native_properties.len(),
                native_properties.as_ptr(),
            );
        }
        Ok(())
    }

    fn add_vertices(&mut self, vertices: Vec<Vertex>) -> FnResult<()> {
        let vertex_size = vertices.len();
        let mut vertex_ids = Vec::with_capacity(vertex_size);
        let mut vertex_label_ids = Vec::with_capacity(vertex_size);
        let mut merge_properties: Vec<WriteNativeProperty> = Vec::with_capacity(vertex_size);
        let mut property_sizes = Vec::with_capacity(vertex_size);
        for vertex in vertices {
            vertex_ids.push(vertex.id());
            vertex_label_ids.push(self.encode_key(vertex.label())?);
            let mut properties = self.encode_details(vertex.details())?;
            property_sizes.push(properties.len());
            merge_properties.append(&mut properties);
        }

        unsafe {
            add_vertices(
                self.graph,
                vertex_size,
                vertex_ids.as_ptr(),
                vertex_label_ids.as_ptr(),
                property_sizes.as_ptr(),
                merge_properties.as_ptr(),
            );
        }

        Ok(())
    }

    fn add_edge(&mut self, edge: Edge) -> FnResult<()> {
        let edge_label = self.encode_key(edge.label())?;
        let src_label = self.encode_key(edge.get_src_label())?;
        let dst_label = self.encode_key(edge.get_dst_label())?;
        let native_properties = self.encode_details(edge.details())?;
        unsafe {
            add_edge(
                self.graph,
                edge.id(),
                edge.src_id,
                edge.dst_id,
                edge_label,
                src_label,
                dst_label,
                native_properties.len(),
                native_properties.as_ptr(),
            );
        }

        Ok(())
    }

    fn add_edges(&mut self, edges: Vec<Edge>) -> FnResult<()> {
        let edge_size = edges.len();
        let mut edge_ids = Vec::with_capacity(edge_size);
        let mut src_ids = Vec::with_capacity(edge_size);
        let mut dst_ids = Vec::with_capacity(edge_size);
        let mut edge_label_ids = Vec::with_capacity(edge_size);
        let mut src_label_ids = Vec::with_capacity(edge_size);
        let mut dst_label_ids = Vec::with_capacity(edge_size);
        let mut merge_properties: Vec<WriteNativeProperty> = Vec::with_capacity(edge_size);
        let mut property_sizes = Vec::with_capacity(edge_size);
        for edge in edges {
            edge_ids.push(edge.id());
            src_ids.push(edge.src_id);
            dst_ids.push(edge.dst_id);
            edge_label_ids.push(self.encode_key(edge.label())?);
            src_label_ids.push(self.encode_key(edge.get_src_label())?);
            dst_label_ids.push(self.encode_key(edge.get_dst_label())?);
            let mut properties = self.encode_details(edge.details())?;
            property_sizes.push(properties.len());
            merge_properties.append(&mut properties);
        }

        unsafe {
            add_edges(
                self.graph,
                edge_size,
                edge_ids.as_ptr(),
                src_ids.as_ptr(),
                dst_ids.as_ptr(),
                edge_label_ids.as_ptr(),
                src_label_ids.as_ptr(),
                dst_label_ids.as_ptr(),
                property_sizes.as_ptr(),
                merge_properties.as_ptr(),
            );
        }

        Ok(())
    }

    fn finish(&mut self) -> FnResult<()> {
        unsafe {
            build_vertices(self.graph);
        }
        unsafe {
            build_edges(self.graph);
        }
        Ok(())
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
