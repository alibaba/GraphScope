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

use dyn_type::Object;
use graph_proxy::apis::VertexOrEdge;
use graph_proxy::apis::{Edge, Element, GraphElement, GraphPath, Vertex, ID};
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::sink_default::MetaType;
use ir_common::generated::common as common_pb;
use ir_common::generated::results as result_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::api::function::{FnResult, MapFunction};
use pegasus_common::downcast::AsAny;
use prost::Message;

use crate::error::{FnExecError, FnExecResult, FnGenResult};
use crate::process::entry::{CollectionEntry, DynEntry, Entry, EntryType, PairEntry};
use crate::process::operator::map::{GeneralIntersectionEntry, IntersectionEntry};
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
    fn entry_to_pb(&self, e: &DynEntry) -> FnExecResult<result_pb::Entry> {
        let inner = match e.get_type() {
            EntryType::Collection => {
                let collection = e
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .unwrap();
                let mut collection_pb = Vec::with_capacity(collection.len());
                if collection.len() == 0 {
                    return Ok(result_pb::Entry {
                        inner: Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                            collection: vec![],
                        })),
                    });
                }
                if collection.inner[0]
                    .get_type()
                    .eq(&EntryType::Pair)
                {
                    // convert to a map result
                    let map_pb = self.collection_map_to_pb(collection.clone())?;
                    Some(result_pb::entry::Inner::Map(map_pb))
                } else {
                    // convert to a collection result
                    for element in &collection.inner {
                        let element_pb = self.element_to_pb(element);
                        collection_pb.push(element_pb);
                    }
                    Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                        collection: collection_pb,
                    }))
                }
            }
            EntryType::Intersection => {
                if let Some(intersection) = e
                    .as_any_ref()
                    .downcast_ref::<IntersectionEntry>()
                {
                    let mut collection_pb = Vec::with_capacity(intersection.len());
                    for v in intersection.iter() {
                        let vertex_pb = self.vid_to_pb(v);
                        let element_pb = result_pb::Element {
                            inner: Some(result_pb::element::Inner::Vertex(vertex_pb)),
                        };
                        collection_pb.push(element_pb);
                    }
                    Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                        collection: collection_pb,
                    }))
                } else if let Some(general_intersection) = e
                    .as_any_ref()
                    .downcast_ref::<GeneralIntersectionEntry>()
                {
                    let mut collection_pb = Vec::with_capacity(general_intersection.len());
                    for v in general_intersection.iter() {
                        let vertex_pb = self.vid_to_pb(v);
                        let element_pb = result_pb::Element {
                            inner: Some(result_pb::element::Inner::Vertex(vertex_pb)),
                        };
                        collection_pb.push(element_pb);
                    }

                    Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                        collection: collection_pb,
                    }))
                } else {
                    Err(FnExecError::unsupported_error("unsupported intersection entry type"))?
                }
            }
            _ => {
                if let Some(map_pb) = self.try_map_to_pb(e) {
                    Some(result_pb::entry::Inner::Map(map_pb))
                } else if let Some(collection_pb) = self.try_collection_to_pb(e) {
                    Some(result_pb::entry::Inner::Collection(collection_pb))
                } else {
                    let element_pb = self.element_to_pb(e);
                    Some(result_pb::entry::Inner::Element(element_pb))
                }
            }
        };
        Ok(result_pb::Entry { inner })
    }

    // return if the given entry is a collection entry result from PathValueProjector eval, etc.
    fn try_collection_to_pb(&self, e: &DynEntry) -> Option<result_pb::Collection> {
        if let EntryType::Object = e.get_type() {
            if let Object::Vector(vec) = e.as_object().unwrap() {
                let mut collection_pb = Vec::with_capacity(vec.len());
                for obj in vec {
                    let obj_pb = self.object_to_pb(obj.clone());
                    let element_pb =
                        result_pb::Element { inner: Some(result_pb::element::Inner::Object(obj_pb)) };
                    collection_pb.push(element_pb);
                }
                return Some(result_pb::Collection { collection: collection_pb });
            }
        }

        None
    }

    // return if the given entry is a map entry result from Map eval.
    fn try_map_to_pb(&self, e: &DynEntry) -> Option<result_pb::KeyValues> {
        if let EntryType::Object = e.get_type() {
            if let Object::KV(kv) = e.as_object().unwrap() {
                let mut key_values: Vec<result_pb::key_values::KeyValue> = Vec::with_capacity(kv.len());
                if let Some(probe) = kv.iter().next() {
                    if let Object::Vector(_) = probe.0 {
                        // the value computed by VarMap.eval(), which will return an Element result. This will be deprecated soon.
                        return None;
                    }
                }
                for (key, val) in kv {
                    let key_pb: common_pb::Value = key.clone().into();
                    let val_pb: common_pb::Value = val.clone().into();
                    key_values.push(result_pb::key_values::KeyValue {
                        key: Some(key_pb),
                        value: Some(result_pb::Entry {
                            inner: Some(result_pb::entry::Inner::Element(result_pb::Element {
                                inner: Some(result_pb::element::Inner::Object(val_pb)),
                            })),
                        }),
                    })
                }
                return Some(result_pb::KeyValues { key_values });
            }
        }

        None
    }

    fn collection_map_to_pb(&self, e: CollectionEntry) -> FnExecResult<result_pb::KeyValues> {
        let mut key_values: Vec<result_pb::key_values::KeyValue> = Vec::with_capacity(e.len());
        for key_val_entry in e.inner {
            let pair = key_val_entry
                .as_any_ref()
                .downcast_ref::<PairEntry>()
                .unwrap();
            if let Some(key_obj) = pair.get_left().as_object() {
                let key_pb: common_pb::Value = key_obj.clone().into();
                let val = pair.get_right();
                if val.get_type() == EntryType::Collection {
                    let inner_collection = val
                        .as_any_ref()
                        .downcast_ref::<CollectionEntry>()
                        .unwrap();
                    let inner_map_pb = self.collection_map_to_pb(inner_collection.clone())?;
                    key_values.push(result_pb::key_values::KeyValue {
                        key: Some(key_pb),
                        value: Some(result_pb::Entry {
                            inner: Some(result_pb::entry::Inner::Map(inner_map_pb)),
                        }),
                    })
                } else {
                    let right = pair.get_right();
                    if let Some(collection) = self.try_collection_to_pb(right) {
                        key_values.push(result_pb::key_values::KeyValue {
                            key: Some(key_pb),
                            value: Some(result_pb::Entry {
                                inner: Some(result_pb::entry::Inner::Collection(collection)),
                            }),
                        });
                    } else {
                        let val_pb = self.element_to_pb(right);
                        key_values.push(result_pb::key_values::KeyValue {
                            key: Some(key_pb),
                            value: Some(result_pb::Entry {
                                inner: Some(result_pb::entry::Inner::Element(val_pb)),
                            }),
                        })
                    }
                }
            } else {
                Err(FnExecError::unsupported_error(&format!(
                    "only support map result with object key, while it is {:?}",
                    pair.get_left()
                )))?
            }
        }
        Ok(result_pb::KeyValues { key_values })
    }

    fn element_to_pb(&self, e: &DynEntry) -> result_pb::Element {
        let inner = match e.get_type() {
            EntryType::Vertex => {
                let vertex_pb = self.vertex_to_pb(e.as_vertex().unwrap());
                Some(result_pb::element::Inner::Vertex(vertex_pb))
            }
            EntryType::Edge => {
                let edge_pb = self.edge_to_pb(e.as_edge().unwrap());
                Some(result_pb::element::Inner::Edge(edge_pb))
            }
            EntryType::Path => {
                let path_pb = self.path_to_pb(e.as_graph_path().unwrap());
                Some(result_pb::element::Inner::GraphPath(path_pb))
            }
            EntryType::Object => {
                let obj_pb = self.object_to_pb(e.as_object().unwrap().clone());
                Some(result_pb::element::Inner::Object(obj_pb))
            }
            EntryType::Collection => {
                unreachable!()
            }
            EntryType::Intersection => {
                unreachable!()
            }
            EntryType::Pair => {
                unreachable!()
            }
            EntryType::Null => Some(result_pb::element::Inner::Object(Object::None.into())),
        };
        result_pb::Element { inner }
    }

    fn object_to_pb(&self, value: Object) -> common_pb::Value {
        if let Object::KV(kv) = value {
            let mut pairs: Vec<common_pb::Pair> = Vec::with_capacity(kv.len());
            for (mut key, val) in kv {
                // a special case to parse key in KV, where the key is vec![tag, prop_name]
                // TODO: this is the result of VarMap.eval(), which will be deprecated soon.
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
        // if we cannot find mapped meta_name, we return meta_id directly.
        NameOrId::Id(meta_id)
    }

    fn vid_to_pb(&self, vid: &ID) -> result_pb::Vertex {
        result_pb::Vertex {
            id: *vid as i64,
            label: None,
            // TODO: return detached vertex without property for now
            properties: vec![],
        }
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

    fn vertex_or_edge_to_pb(&self, vertex_or_edge: &VertexOrEdge) -> result_pb::graph_path::VertexOrEdge {
        match vertex_or_edge {
            VertexOrEdge::V(v) => {
                let vertex_pb = self.vertex_to_pb(v);
                result_pb::graph_path::VertexOrEdge {
                    inner: Some(result_pb::graph_path::vertex_or_edge::Inner::Vertex(vertex_pb)),
                }
            }
            VertexOrEdge::E(e) => {
                let edge_pb = self.edge_to_pb(e);
                result_pb::graph_path::VertexOrEdge {
                    inner: Some(result_pb::graph_path::vertex_or_edge::Inner::Edge(edge_pb)),
                }
            }
        }
    }

    fn path_to_pb(&self, p: &GraphPath) -> result_pb::GraphPath {
        let mut graph_path_pb = vec![];
        match p {
            GraphPath::AllPath(path) | GraphPath::SimpleAllPath(path) | GraphPath::TrailAllPath(path) => {
                for vertex_or_edge in path {
                    let vertex_or_edge_pb = self.vertex_or_edge_to_pb(vertex_or_edge);
                    graph_path_pb.push(vertex_or_edge_pb);
                }
            }
            GraphPath::EndV((path_end, _)) | GraphPath::SimpleEndV((path_end, _, _)) => {
                let vertex_or_edge_pb = self.vertex_or_edge_to_pb(path_end);
                graph_path_pb.push(vertex_or_edge_pb);
            }
        }
        result_pb::GraphPath { path: graph_path_pb }
    }
}

impl MapFunction<Record, Vec<u8>> for RecordSinkEncoder {
    fn exec(&self, mut input: Record) -> FnResult<Vec<u8>> {
        let mut sink_columns = Vec::with_capacity(self.sink_keys.len());
        if self.sink_keys.is_empty() {
            // the case of sink all **tagged** columns by default.
            let columns = input.get_columns_mut();
            for (sink_key, entry) in columns.into_iter() {
                let entry_pb = self.entry_to_pb(entry)?;
                let column_pb = result_pb::Column {
                    name_or_id: Some(self.meta_to_pb(NameOrId::Id(sink_key as KeyId), MetaType::Tag)),
                    entry: Some(entry_pb),
                };
                sink_columns.push(column_pb);
            }
        } else {
            for sink_key in self.sink_keys.iter() {
                if let Some(entry) = input.get(sink_key.clone()) {
                    let entry_pb = self.entry_to_pb(entry)?;
                    let column_pb = result_pb::Column {
                        name_or_id: sink_key
                            .clone()
                            .map(|sink_key| self.meta_to_pb(NameOrId::Id(sink_key), MetaType::Tag)),
                        entry: Some(entry_pb),
                    };
                    sink_columns.push(column_pb);
                }
            }
        }

        let record_pb = result_pb::Record { columns: sink_columns };
        debug!("sink record_pb {:?}", record_pb);
        let results = result_pb::Results { inner: Some(result_pb::results::Inner::Record(record_pb)) };
        Ok(results.encode_to_vec())
    }
}

pub struct DefaultSinkOp {
    pub tags: Vec<Option<KeyId>>,
    pub id_name_mappings: Vec<algebra_pb::sink_default::IdNameMapping>,
}

impl SinkGen for DefaultSinkOp {
    fn gen_sink(self) -> FnGenResult<Sinker> {
        let mut schema_map = HashMap::new();
        for id_name_mappings_pb in self.id_name_mappings {
            let meta_type = unsafe { ::std::mem::transmute(id_name_mappings_pb.meta_type) };
            schema_map.insert((meta_type, id_name_mappings_pb.id), id_name_mappings_pb.name);
        }
        let record_sinker = RecordSinkEncoder {
            sink_keys: self.tags,
            schema_map: if schema_map.is_empty() { None } else { Some(schema_map) },
        };
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!("Runtime sink operator: {:?}", record_sinker);
        }
        Ok(Sinker::DefaultSinker(record_sinker))
    }
}
