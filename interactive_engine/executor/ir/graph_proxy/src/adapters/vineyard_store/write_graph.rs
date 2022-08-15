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

use global_query::store_impl::v6d::read_ffi::*;
use global_query::store_impl::v6d::write_ffi::*;
use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;
use ir_common::{KeyId, LabelId, NameOrId, OneOrMany};

use crate::apis::graph::PKV;
use crate::apis::{Details, DynDetails, WriteGraphProxy};
use crate::{GraphProxyError, GraphProxyResult};

#[derive(Clone, Debug)]
pub struct VineyardGraphWriter {
    graph: GraphBuilder,
}

impl VineyardGraphWriter {
    pub fn new(graph_name: String, schema: &schema_pb::Schema, index: i32) -> GraphProxyResult<Self> {
        let name = CString::new(graph_name).unwrap();
        let graph = unsafe { v6d_get_graph_builder(name.as_ptr(), index) };
        let schema_handle = build_vineyard_schema(schema)?;
        unsafe { v6d_initialize_graph_builder(graph, schema_handle) };
        Ok(VineyardGraphWriter { graph })
    }

    fn encode_ffi_id(&self, vertex_pk: PKV) -> GraphProxyResult<FfiVertexId> {
        match vertex_pk {
            OneOrMany::One(pkv) => {
                let pk_value = &pkv[0].1;
                pk_value.as_u64().map_err(|e| {
                    GraphProxyError::query_store_error(&format!(
                        "cast outer_id as u64 failed {:?}",
                        e.to_string()
                    ))
                })
            }
            OneOrMany::Many(_) => Err(GraphProxyError::write_graph_error(
                "encode_ffi_id failed as vineyard only support single-column pk",
            )),
        }
    }

    fn encode_ffi_label(&self, label: LabelId) -> GraphProxyResult<FfiLabelId> {
        Ok(label as FfiLabelId)
    }

    fn encode_key(&self, key: NameOrId) -> GraphProxyResult<KeyId> {
        match key {
            NameOrId::Str(s) => Err(GraphProxyError::write_graph_error(&format!(
                "do not support string key when writing vineyard {:?}",
                s
            )))?,
            NameOrId::Id(id) => Ok(id),
        }
    }

    fn encode_details(&self, details: Option<DynDetails>) -> GraphProxyResult<Vec<WriteNativeProperty>> {
        let properties = details
            .map(|details| details.get_all_properties())
            .unwrap_or(None);
        let mut native_properties: Vec<WriteNativeProperty> = vec![];
        if let Some(mut properties) = properties {
            for (prop_key, prop_val) in properties.drain() {
                let prop_id = self.encode_key(prop_key)?;
                let native_property = WriteNativeProperty::from_object(prop_id, prop_val);
                native_properties.push(native_property);
            }
        }
        Ok(native_properties)
    }
}

impl WriteGraphProxy for VineyardGraphWriter {
    fn add_vertex(
        &mut self, label: LabelId, vertex_pk: PKV, properties: Option<DynDetails>,
    ) -> GraphProxyResult<()> {
        let vertex_id = self.encode_ffi_id(vertex_pk)?;
        let label_id = self.encode_ffi_label(label)?;
        let native_properties = self.encode_details(properties)?;
        let state = unsafe {
            v6d_add_vertex(
                self.graph,
                vertex_id,
                label_id,
                native_properties.len(),
                native_properties.as_ptr(),
            )
        };
        check_ffi_state(state, "add_vertex")
    }

    fn add_edge(
        &mut self, label: LabelId, src_vertex_label: LabelId, src_vertex_pk: PKV,
        dst_vertex_label: LabelId, dst_vertex_pk: PKV, properties: Option<DynDetails>,
    ) -> GraphProxyResult<()> {
        let edge_label = self.encode_ffi_label(label)?;
        let src_id = self.encode_ffi_id(src_vertex_pk)?;
        let dst_id = self.encode_ffi_id(dst_vertex_pk)?;
        let src_label = self.encode_ffi_label(src_vertex_label)?;
        let dst_label = self.encode_ffi_label(dst_vertex_label)?;
        let native_properties = self.encode_details(properties)?;
        let state = unsafe {
            v6d_add_edge(
                self.graph,
                src_id,
                dst_id,
                edge_label,
                src_label,
                dst_label,
                native_properties.len(),
                native_properties.as_ptr(),
            )
        };

        check_ffi_state(state, "add_edge")
    }

    fn finish(&mut self) -> GraphProxyResult<()> {
        let build_vertex_state = unsafe { v6d_build_vertices(self.graph) };
        let build_edge_state = unsafe { v6d_build_edges(self.graph) };
        check_ffi_state(build_vertex_state, "finish_vertex")?;
        check_ffi_state(build_edge_state, "finish_edge")
    }
}

impl Drop for VineyardGraphWriter {
    fn drop(&mut self) {
        unsafe {
            v6d_destroy(self.graph);
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
    let builder = unsafe { v6d_create_schema_builder() };
    for vertex in &schema.entities {
        let label = &vertex.label.as_ref().unwrap();
        let name = CString::new(label.name.to_owned()).unwrap();
        let v_type_builder = unsafe { v6d_build_vertex_type(builder, label.id, name.as_ptr()) };

        let mut primary_key_list = vec![];
        let mut primary_key_ptr_list = vec![];

        for column in &vertex.columns {
            let key = &column.key.as_ref().unwrap();
            let prop_id = key.id;
            let prop_name = CString::new(key.name.to_owned()).unwrap();
            let prop_type = PropertyType::from_common_data_type(
                common_pb::DataType::from_i32(column.data_type).unwrap(),
            );
            let state = unsafe {
                v6d_build_vertex_property(v_type_builder, prop_id, prop_name.as_ptr(), prop_type)
            };
            check_ffi_state(state, "build_vertex_property() for vineyard")?;

            if column.is_primary_key {
                primary_key_ptr_list.push(prop_name.as_ptr());
                primary_key_list.push(prop_name);
            }
        }
        unsafe {
            let state = v6d_build_vertex_primary_keys(
                v_type_builder,
                primary_key_list.len(),
                primary_key_ptr_list.as_ptr(),
            );
            check_ffi_state(state, "build_vertex_primary_keys() for vineyard")?;
            let state = v6d_finish_build_vertex(v_type_builder);
            check_ffi_state(state, "finish_build_vertex() for vineyard")?;
        };
    }

    for edge in &schema.relations {
        let label = &edge.label.as_ref().unwrap();
        let name = CString::new(label.name.to_owned()).unwrap();
        let e_type_builder = unsafe { v6d_build_edge_type(builder, label.id, name.as_ptr()) };

        for column in &edge.columns {
            let key = &column.key.as_ref().unwrap();
            let prop_id = key.id;
            let prop_name = CString::new(key.name.to_owned()).unwrap();
            let prop_type = PropertyType::from_common_data_type(
                common_pb::DataType::from_i32(column.data_type).unwrap(),
            );
            let state =
                unsafe { v6d_build_edge_property(e_type_builder, prop_id, prop_name.as_ptr(), prop_type) };
            check_ffi_state(state, "build_edge_property() for vineyard")?;
        }

        for relation in &edge.entity_pairs {
            let src = &relation.src.as_ref().unwrap();
            let src_name = CString::new(src.name.to_owned()).unwrap();
            let dst = &relation.dst.as_ref().unwrap();
            let dst_name = CString::new(dst.name.to_owned()).unwrap();
            let state =
                unsafe { v6d_build_edge_relation(e_type_builder, src_name.as_ptr(), dst_name.as_ptr()) };
            check_ffi_state(state, "build_edge_relation() for vineyard")?;
        }

        let state = unsafe { v6d_finish_build_edge(e_type_builder) };
        check_ffi_state(state, "finish_build_edge() for vineyard")?;
    }

    unsafe { v6d_finish_build_schema(builder) };

    return Ok(builder);
}
