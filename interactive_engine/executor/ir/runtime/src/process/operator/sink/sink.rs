//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::convert::TryInto;

use dyn_type::Object;
use graph_proxy::apis::{Edge, Element, GraphElement, GraphPath, Vertex};
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::sink_default::MetaType;
use ir_common::generated::common as common_pb;
use ir_common::generated::results as result_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::api::function::{FnResult, MapFunction};
use pegasus_common::downcast::AsAny;
use prost::Message;

use crate::error::FnGenResult;
use crate::process::entry::{CollectionEntry, DynEntry, Entry, EntryDataType};
use crate::process::operator::map::Intersection;
use crate::process::operator::sink::{SinkGen, Sinker};
use crate::process::record::Record;

#[derive(Debug)]
pub struct RecordSinkEncoder {
    /// the given column tags to sink;
    sink_keys: Vec<Option<KeyId>>,
    /// A map from id to name; Now we only support to map Tag (Alias) in Runtime.
    schema_map: Option<HashMap<(MetaType, i32), String>>,
}

impl RecordSinkEncoder {
    fn entry_to_pb(&self, e: &DynEntry) -> result_pb::Entry {
        let inner = match e.get_type() {
            EntryDataType::Collection => {
                let collection = e
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .unwrap();
                let mut collection_pb = Vec::with_capacity(collection.len());
                for element in &collection.inner {
                    let element_pb = self.element_to_pb(element);
                    collection_pb.push(element_pb);
                }
                Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                    collection: collection_pb,
                }))
            }
            EntryDataType::Intersect => {
                let intersection = e
                    .as_any_ref()
                    .downcast_ref::<Intersection>()
                    .unwrap();
                let mut collection_pb = Vec::with_capacity(intersection.len());
                for v in intersection.iter() {
                    let vertex_pb = self.vertex_to_pb(v);
                    let element_pb =
                        result_pb::Element { inner: Some(result_pb::element::Inner::Vertex(vertex_pb)) };
                    collection_pb.push(element_pb);
                }
                Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                    collection: collection_pb,
                }))
            }
            _ => {
                let element_pb = self.element_to_pb(e);
                Some(result_pb::entry::Inner::Element(element_pb))
            }
        };
        result_pb::Entry { inner }
    }

    fn element_to_pb(&self, e: &DynEntry) -> result_pb::Element {
        let inner = match e.get_type() {
            EntryDataType::V => {
                let vertex_pb = self.vertex_to_pb(e.as_graph_vertex().unwrap());
                Some(result_pb::element::Inner::Vertex(vertex_pb))
            }
            EntryDataType::E => {
                let edge_pb = self.edge_to_pb(e.as_graph_edge().unwrap());
                Some(result_pb::element::Inner::Edge(edge_pb))
            }
            EntryDataType::P => {
                let path_pb = self.path_to_pb(e.as_graph_path().unwrap());
                Some(result_pb::element::Inner::GraphPath(path_pb))
            }
            EntryDataType::Obj => {
                let obj_pb = self.object_to_pb(e.as_object().unwrap().clone());
                Some(result_pb::element::Inner::Object(obj_pb))
            }
            EntryDataType::Collection => {
                unreachable!()
            }
            EntryDataType::Intersect => {
                unreachable!()
            }
            _ => {
                todo!()
            }
        };
        result_pb::Element { inner }
    }

    fn object_to_pb(&self, value: Object) -> common_pb::Value {
        if let Object::KV(kv) = value {
            let mut pairs: Vec<common_pb::Pair> = Vec::with_capacity(kv.len());
            for (mut key, val) in kv {
                // a special case to parse key in KV, where the key is vec![tag, prop_name]
                if let Object::Vector(ref mut v) = key {
                    if v.len() == 2 {
                        // map tag_id to tag_name
                        if let Ok(tag_id) = v.get(0).unwrap().as_i32() {
                            let mapped_tag = Object::from(self.get_meta_name(tag_id, MetaType::Tag));
                            *(v[0].borrow_mut()) = mapped_tag;
                        }
                    }
                }
                let key_pb: common_pb::Value = key.into();
                let val_pb: common_pb::Value = val.into();
                pairs.push(common_pb::Pair { key: Some(key_pb), val: Some(val_pb) })
            }
            let item = common_pb::value::Item::PairArray(common_pb::PairArray { item: pairs });
            common_pb::Value { item: Some(item) }
        } else {
            common_pb::Value::from(value)
        }
    }

    fn meta_to_pb(&self, meta: NameOrId, t: MetaType) -> common_pb::NameOrId {
        let mapped_meta = match meta {
            NameOrId::Str(_) => meta,
            NameOrId::Id(id) => self.get_meta_name(id, t),
        };
        mapped_meta.into()
    }

    fn get_meta_name(&self, meta_id: KeyId, t: MetaType) -> NameOrId {
        if let Some(schema_map) = self.schema_map.as_ref() {
            if let Some(meta_name) = schema_map.get(&(t, meta_id)) {
                return NameOrId::Str(meta_name.clone());
            }
        }
        // if we can not find mapped meta_name, we return meta_id.to_string() instead.
        NameOrId::Str(meta_id.to_string())
    }

    fn vertex_to_pb(&self, v: &Vertex) -> result_pb::Vertex {
        result_pb::Vertex {
            id: v.id() as i64,
            label: v.label().map(|label| label.clone().into()),
            // TODO: return detached vertex without property for now
            properties: vec![],
        }
    }

    fn edge_to_pb(&self, e: &Edge) -> result_pb::Edge {
        result_pb::Edge {
            id: e.id() as i64,
            label: e.label().map(|label| label.clone().into()),
            src_id: e.src_id as i64,
            src_label: e
                .get_src_label()
                .map(|label| label.clone().into()),
            dst_id: e.dst_id as i64,
            dst_label: e
                .get_dst_label()
                .map(|label| label.clone().into()),
            // TODO: return detached edge without property for now
            properties: vec![],
        }
    }

    fn vertex_or_edge_to_pb(&self, vertex_or_edge: &Vertex) -> result_pb::graph_path::VertexOrEdge {
        let vertex_pb = self.vertex_to_pb(vertex_or_edge);
        result_pb::graph_path::VertexOrEdge {
            inner: Some(result_pb::graph_path::vertex_or_edge::Inner::Vertex(vertex_pb)),
        }
    }

    fn path_to_pb(&self, p: &GraphPath) -> result_pb::GraphPath {
        let mut graph_path_pb = vec![];
        match p {
            GraphPath::AllV(path) | GraphPath::SimpleAllV(path) => {
                for vertex_or_edge in path {
                    let vertex_or_edge_pb = self.vertex_or_edge_to_pb(vertex_or_edge);
                    graph_path_pb.push(vertex_or_edge_pb);
                }
            }
            GraphPath::EndV((path_end, _)) | GraphPath::SimpleEndV((path_end, _)) => {
                let vertex_or_edge_pb = self.vertex_or_edge_to_pb(path_end);
                graph_path_pb.push(vertex_or_edge_pb);
            }
        }
        result_pb::GraphPath { path: graph_path_pb }
    }
}

impl MapFunction<Record, Vec<u8>> for RecordSinkEncoder {
    fn exec(&self, input: Record) -> FnResult<Vec<u8>> {
        let mut sink_columns = Vec::with_capacity(self.sink_keys.len());
        for sink_key in self.sink_keys.iter() {
            if let Some(entry) = input.get(sink_key.clone()) {
                let entry_pb = self.entry_to_pb(entry);
                let column_pb = result_pb::Column {
                    name_or_id: sink_key
                        .clone()
                        .map(|sink_key| self.meta_to_pb(NameOrId::Id(sink_key), MetaType::Tag)),
                    entry: Some(entry_pb),
                };
                sink_columns.push(column_pb);
            }
        }

        let record_pb = result_pb::Record { columns: sink_columns };
        let results = result_pb::Results { inner: Some(result_pb::results::Inner::Record(record_pb)) };
        Ok(results.encode_to_vec())
    }
}

pub struct DefaultSinkOp {
    pub tags: Vec<common_pb::NameOrIdKey>,
    pub id_name_mappings: Vec<algebra_pb::sink_default::IdNameMapping>,
}

impl SinkGen for DefaultSinkOp {
    fn gen_sink(self) -> FnGenResult<Sinker> {
        let mut sink_keys = Vec::with_capacity(self.tags.len());
        for sink_key_pb in self.tags.into_iter() {
            let sink_key = sink_key_pb
                .key
                .map(|tag| tag.try_into())
                .transpose()?;
            sink_keys.push(sink_key);
        }

        let mut schema_map = HashMap::new();
        for id_name_mappings_pb in self.id_name_mappings {
            let meta_type = unsafe { ::std::mem::transmute(id_name_mappings_pb.meta_type) };
            schema_map.insert((meta_type, id_name_mappings_pb.id), id_name_mappings_pb.name);
        }
        let record_sinker = RecordSinkEncoder {
            sink_keys,
            schema_map: if schema_map.is_empty() { None } else { Some(schema_map) },
        };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime sink operator: {:?}", record_sinker);
        }
        Ok(Sinker::DefaultSinker(record_sinker))
    }
}
