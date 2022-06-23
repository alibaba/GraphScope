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

use ffi::read_ffi::*;
use ffi::write_ffi::*;
use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;
use ir_common::{KeyId, NameOrId};

use crate::apis::{
    get_graph, Details, DynDetails, Edge, Element, GraphElement, QueryParams, Vertex, WriteGraphProxy, ID,
};
use crate::{GraphProxyError, GraphProxyResult};

// TODO: use TRANSLATE_VERTEX_ID_FLAG in read_graph after separate groot/vineyard
const TRANSLATE_VERTEX_ID_FLAG: &str = "TRANSLATE_VERTEX_ID_FLAG";
const OUTER_ID_PROP_KEY: &str = "OUTER_ID_PROP_KEY";

#[derive(Clone, Debug)]
pub struct VineyardGraphWriter {
    graph: GraphBuilder,
}

impl VineyardGraphWriter {
    pub fn new(graph_name: String, schema: &schema_pb::Schema, index: i32) -> GraphProxyResult<Self> {
        let name = CString::new(graph_name).unwrap();
        let graph = unsafe { get_graph_builder(name.as_ptr(), index) };
        let schema_handle = build_vineyard_schema(schema)?;
        unsafe { initialize_graph_builder(graph, schema_handle) };
        Ok(VineyardGraphWriter { graph })
    }

    fn encode_ffi_id(&self, vertex_id: ID) -> GraphProxyResult<FfiVertexId> {
        let read_graph = get_graph()
            .ok_or(GraphProxyError::query_store_error("get graph failed when writing subgraph"))?;
        let mut extra_param = HashMap::new();
        extra_param.insert(TRANSLATE_VERTEX_ID_FLAG.to_string(), "".to_string());
        let mut query_param = QueryParams::default();
        query_param.extra_params = Some(extra_param);
        if let Some(vertex_with_outer_id) = read_graph
            .get_vertex(&vec![vertex_id], &query_param)?
            .next()
        {
            let outer_id = vertex_with_outer_id
                .details()
                .map(|details| details.get_property(&NameOrId::from(OUTER_ID_PROP_KEY)))
                .unwrap_or(None)
                .ok_or(GraphProxyError::query_store_error(
                    "get_property of outer_id failed when writing subgraph",
                ))?;
            outer_id.as_u64().map_err(|e| {
                GraphProxyError::query_store_error(&format!(
                    "cast outer_id as u64 failed {:?}",
                    e.to_string()
                ))
            })
        } else {
            Err(GraphProxyError::query_store_error("get_vertex with TRANSLATE_VERTEX_ID_FLAG failed"))
        }
    }

    fn encode_ffi_label(&self, key: Option<&NameOrId>) -> GraphProxyResult<FfiLabelId> {
        let key_id = self.encode_key(key)?;
        Ok(key_id as FfiLabelId)
    }

    fn encode_key(&self, key: Option<&NameOrId>) -> GraphProxyResult<KeyId> {
        match key {
            Some(NameOrId::Str(s)) => Err(GraphProxyError::write_graph_error(&format!(
                "do not support string key when writing vineyard {:?}",
                s
            )))?,
            Some(NameOrId::Id(id)) => Ok(*id),
            None => {
                Err(GraphProxyError::write_graph_error("do not support empty key when writing vineyard"))?
            }
        }
    }

    fn encode_details(&self, details: Option<&DynDetails>) -> GraphProxyResult<Vec<WriteNativeProperty>> {
        let properties = details
            .map(|details| details.get_all_properties())
            .unwrap_or(None);
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
    fn add_vertex(&mut self, vertex: Vertex) -> GraphProxyResult<()> {
        let vertex_id = self.encode_ffi_id(vertex.id())?;
        let label_id = self.encode_ffi_label(vertex.label())?;
        let native_properties = self.encode_details(vertex.details())?;
        let state = unsafe {
            add_vertex(self.graph, vertex_id, label_id, native_properties.len(), native_properties.as_ptr())
        };
        check_ffi_state(state, "add_vertex")
    }

    fn add_vertices(&mut self, vertices: Vec<Vertex>) -> GraphProxyResult<()> {
        let vertex_size = vertices.len();
        let mut vertex_ids = Vec::with_capacity(vertex_size);
        let mut vertex_label_ids = Vec::with_capacity(vertex_size);
        let mut merge_properties: Vec<WriteNativeProperty> = Vec::with_capacity(vertex_size);
        let mut property_sizes = Vec::with_capacity(vertex_size);
        for vertex in vertices {
            vertex_ids.push(self.encode_ffi_id(vertex.id())?);
            vertex_label_ids.push(self.encode_ffi_label(vertex.label())?);
            let mut properties = self.encode_details(vertex.details())?;
            property_sizes.push(properties.len());
            merge_properties.append(&mut properties);
        }

        let state = unsafe {
            add_vertices(
                self.graph,
                vertex_size,
                vertex_ids.as_ptr(),
                vertex_label_ids.as_ptr(),
                property_sizes.as_ptr(),
                merge_properties.as_ptr(),
            )
        };
        check_ffi_state(state, "add_vertices")
    }

    fn add_edge(&mut self, edge: Edge) -> GraphProxyResult<()> {
        let edge_label = self.encode_ffi_label(edge.label())?;
        let src_label = self.encode_ffi_label(edge.get_src_label())?;
        let dst_label = self.encode_ffi_label(edge.get_dst_label())?;
        let native_properties = self.encode_details(edge.details())?;
        let state = unsafe {
            add_edge(
                self.graph,
                edge.id(),
                self.encode_ffi_id(edge.src_id)?,
                self.encode_ffi_id(edge.dst_id)?,
                edge_label,
                src_label,
                dst_label,
                native_properties.len(),
                native_properties.as_ptr(),
            )
        };

        check_ffi_state(state, "add_edge")
    }

    fn add_edges(&mut self, edges: Vec<Edge>) -> GraphProxyResult<()> {
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
            src_ids.push(self.encode_ffi_id(edge.src_id)?);
            dst_ids.push(self.encode_ffi_id(edge.dst_id)?);
            edge_label_ids.push(self.encode_ffi_label(edge.label())?);
            src_label_ids.push(self.encode_ffi_label(edge.get_src_label())?);
            dst_label_ids.push(self.encode_ffi_label(edge.get_dst_label())?);
            let mut properties = self.encode_details(edge.details())?;
            property_sizes.push(properties.len());
            merge_properties.append(&mut properties);
        }

        let state = unsafe {
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
            )
        };

        check_ffi_state(state, "add_edges")
    }

    fn finish(&mut self) -> GraphProxyResult<()> {
        let build_vertex_state = unsafe { build_vertices(self.graph) };
        let build_edge_state = unsafe { build_edges(self.graph) };
        check_ffi_state(build_vertex_state, "finish_vertex")?;
        check_ffi_state(build_edge_state, "finish_edge")
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

fn check_ffi_state(state: FFIState, msg: &str) -> GraphProxyResult<()> {
    if state.eq(&STATE_SUCCESS) {
        Ok(())
    } else if state.eq(&STATE_FAILED) {
        Err(GraphProxyError::write_graph_error(&format!("state error on vineyard when {:?}", msg)))
    } else {
        Err(GraphProxyError::write_graph_error(&format!("state unknown on vineyard when {:?}", msg)))
    }
}

fn build_vineyard_schema(schema: &schema_pb::Schema) -> GraphProxyResult<SchemaHandle> {
    let builder = unsafe { create_schema_builder() };
    for vertex in &schema.entities {
        let label = &vertex.label.as_ref().unwrap();
        let name = CString::new(label.name.to_owned()).unwrap();
        let v_type_builder = unsafe { build_vertex_type(builder, label.id, name.as_ptr()) };

        let mut primary_key_list = vec![];
        let mut primary_key_ptr_list = vec![];

        for column in &vertex.columns {
            let key = &column.key.as_ref().unwrap();
            let prop_id = key.id;
            let prop_name = CString::new(key.name.to_owned()).unwrap();
            let prop_type = PropertyType::from_common_data_type(
                common_pb::DataType::from_i32(column.data_type).unwrap(),
            );
            let state =
                unsafe { build_vertex_property(v_type_builder, prop_id, prop_name.as_ptr(), prop_type) };
            check_ffi_state(state, "build_vertex_property() for vineyard")?;

            if column.is_primary_key {
                primary_key_ptr_list.push(prop_name.as_ptr());
                primary_key_list.push(prop_name);
            }
        }
        unsafe {
            let state = build_vertex_primary_keys(
                v_type_builder,
                primary_key_list.len(),
                primary_key_ptr_list.as_ptr(),
            );
            check_ffi_state(state, "build_vertex_primary_keys() for vineyard")?;
            let state = finish_build_vertex(v_type_builder);
            check_ffi_state(state, "finish_build_vertex() for vineyard")?;
        };
    }

    for edge in &schema.relations {
        let label = &edge.label.as_ref().unwrap();
        let name = CString::new(label.name.to_owned()).unwrap();
        let e_type_builder = unsafe { build_edge_type(builder, label.id, name.as_ptr()) };

        for column in &edge.columns {
            let key = &column.key.as_ref().unwrap();
            let prop_id = key.id;
            let prop_name = CString::new(key.name.to_owned()).unwrap();
            let prop_type = PropertyType::from_common_data_type(
                common_pb::DataType::from_i32(column.data_type).unwrap(),
            );
            let state =
                unsafe { build_edge_property(e_type_builder, prop_id, prop_name.as_ptr(), prop_type) };
            check_ffi_state(state, "build_edge_property() for vineyard")?;
        }

        for relation in &edge.entity_pairs {
            let src = &relation.src.as_ref().unwrap();
            let src_name = CString::new(src.name.to_owned()).unwrap();
            let dst = &relation.dst.as_ref().unwrap();
            let dst_name = CString::new(dst.name.to_owned()).unwrap();
            let state =
                unsafe { build_edge_relation(e_type_builder, src_name.as_ptr(), dst_name.as_ptr()) };
            check_ffi_state(state, "build_edge_relation() for vineyard")?;
        }

        let state = unsafe { finish_build_edge(e_type_builder) };
        check_ffi_state(state, "finish_build_edge() for vineyard")?;
    }

    unsafe { finish_build_schema(builder) };

    return Ok(builder);
}
