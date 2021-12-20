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

use ir_common::generated::common as common_pb;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::graph::element::{Edge, GraphElement, Vertex, VertexOrEdge};
use crate::process::record::{Entry, ObjectElement, Record, RecordElement};

pub struct RecordSinkEncoder {
    /// the given output fields; None means to output current entry;
    sink_keys: Vec<NameOrId>,
    is_output_head: bool,
}

// TODO(bingqing): gen RecordSinkEncoder from pb;
impl Default for RecordSinkEncoder {
    fn default() -> Self {
        RecordSinkEncoder { sink_keys: vec![], is_output_head: true }
    }
}

impl MapFunction<Record, result_pb::Results> for RecordSinkEncoder {
    fn exec(&self, mut input: Record) -> FnResult<result_pb::Results> {
        let mut sink_columns = Vec::with_capacity(self.sink_keys.len());
        for sink_key in self.sink_keys.iter() {
            let entry = input.take(Some(sink_key));
            let entry_pb = entry.map(|entry| result_pb::Entry::from((*entry).clone()));
            let column_pb = result_pb::Column {
                name_or_id: Some(common_pb::NameOrId::from(sink_key.clone())),
                entry: entry_pb,
            };
            sink_columns.push(column_pb);
        }
        if self.is_output_head {
            let entry = input.take(None);
            let entry_pb = entry.map(|entry| result_pb::Entry::from((*entry).clone()));
            let column_pb = result_pb::Column { name_or_id: None, entry: entry_pb };
            sink_columns.push(column_pb);
        }
        let record_pb = result_pb::Record { columns: sink_columns };
        let results = result_pb::Results { inner: Some(result_pb::results::Inner::Record(record_pb)) };
        info!("results {:?}", results);
        Ok(results)
    }
}

impl From<Entry> for result_pb::Entry {
    fn from(e: Entry) -> Self {
        let inner = match e {
            Entry::Element(element) => {
                let element_pb = result_pb::Element::from(element);
                Some(result_pb::entry::Inner::Element(element_pb))
            }
            Entry::Collection(collection) => {
                let mut collection_pb = Vec::with_capacity(collection.len());
                for element in collection.into_iter() {
                    let element_pb = result_pb::Element::from(element);
                    collection_pb.push(element_pb);
                }
                Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                    collection: collection_pb,
                }))
            }
        };
        result_pb::Entry { inner }
    }
}

impl From<RecordElement> for result_pb::Element {
    fn from(e: RecordElement) -> Self {
        let inner = match e {
            RecordElement::OnGraph(vertex_or_edge) => match vertex_or_edge {
                VertexOrEdge::V(v) => {
                    let vertex_pb = result_pb::Vertex::from(v);
                    Some(result_pb::element::Inner::Vertex(vertex_pb))
                }
                VertexOrEdge::E(e) => {
                    let edge_pb = result_pb::Edge::from(e);
                    Some(result_pb::element::Inner::Edge(edge_pb))
                }
            },
            RecordElement::OffGraph(o) => match o {
                ObjectElement::None => None,
                ObjectElement::Prop(obj) | ObjectElement::Agg(obj) => {
                    Some(result_pb::element::Inner::Object(obj.into()))
                }
                ObjectElement::Count(cnt) => {
                    let item = if cnt <= (i64::MAX as u64) {
                        common_pb::value::Item::I64(cnt as i64)
                    } else {
                        common_pb::value::Item::Blob(cnt.to_be_bytes().to_vec())
                    };
                    let value_pb = common_pb::Value { item: Some(item) };
                    Some(result_pb::element::Inner::Object(value_pb))
                }
            },
        };
        result_pb::Element { inner }
    }
}

impl From<Vertex> for result_pb::Vertex {
    fn from(v: Vertex) -> Self {
        result_pb::Vertex {
            id: v.id() as i64,
            label: v.label().map(|label| label.clone().into()),
            // TODO: return detached vertex without property for now
            properties: vec![],
        }
    }
}

impl From<Edge> for result_pb::Edge {
    fn from(e: Edge) -> Self {
        result_pb::Edge {
            id: e.id() as i64,
            label: e.label().map(|label| label.clone().into()),
            src_id: e.src_id as i64,
            src_label: e.get_src_label().map(|label| label.clone().into()),
            dst_id: e.dst_id as i64,
            dst_label: e.get_dst_label().map(|label| label.clone().into()),
            // TODO: return detached edge without property for now
            properties: vec![],
        }
    }
}
