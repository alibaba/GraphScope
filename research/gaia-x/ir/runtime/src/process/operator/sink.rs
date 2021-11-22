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

use crate::graph::element::{Edge, GraphElement, Vertex, VertexOrEdge};
use crate::graph::property::{DefaultDetails, DynDetails};
use crate::graph::ID;
use crate::process::record::{Entry, ObjectElement, Record, RecordElement};
use dyn_type::{Object, Primitives};
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::generated::result as result_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};
use std::convert::{TryFrom, TryInto};

pub struct RecordSinkEncoder {
    sink_keys: Vec<Option<NameOrId>>,
}

impl Default for RecordSinkEncoder {
    fn default() -> Self {
        RecordSinkEncoder {
            sink_keys: vec![None],
        }
    }
}

impl MapFunction<Record, result_pb::Result> for RecordSinkEncoder {
    fn exec(&self, mut input: Record) -> FnResult<result_pb::Result> {
        let mut sink_columns = Vec::with_capacity(self.sink_keys.len());
        for sink_key in self.sink_keys.iter() {
            let entry = input.take(sink_key.as_ref());
            let entry_pb = entry.map(|entry| result_pb::Entry::from((*entry).clone()));
            let column_pb = result_pb::Column {
                name_or_id: sink_key.clone().map(|key| key.into()),
                entry: entry_pb,
            };
            sink_columns.push(column_pb);
        }
        let record_pb = result_pb::Record {
            columns: sink_columns,
        };
        Ok(result_pb::Result {
            inner: Some(result_pb::result::Inner::Record(record_pb)),
        })
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
                    let value_pb = object_to_pb_value(obj);
                    Some(result_pb::element::Inner::Object(value_pb))
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
            id: v.id().expect("vid should not be empty") as i64,
            label: v.label().map(|label| label.clone().into()),
            // TODO: return detached vertex without property for now
            properties: vec![],
        }
    }
}

impl From<Edge> for result_pb::Edge {
    fn from(e: Edge) -> Self {
        result_pb::Edge {
            id: e.id().expect("vid should not be empty") as i64,
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

fn object_to_pb_value(value: Object) -> common_pb::Value {
    let item = match value {
        Object::Primitive(v) => match v {
            Primitives::Byte(v) => common_pb::value::Item::I32(v as i32),
            Primitives::Integer(v) => common_pb::value::Item::I32(v),
            Primitives::Long(v) => common_pb::value::Item::I64(v),
            Primitives::ULLong(v) => common_pb::value::Item::Blob(v.to_be_bytes().to_vec()),
            Primitives::Float(v) => common_pb::value::Item::F64(v),
        },
        Object::String(s) => common_pb::value::Item::Str(s),
        Object::Blob(b) => common_pb::value::Item::Blob(b.to_vec()),
        Object::DynOwned(_u) => {
            todo!()
        }
    };
    common_pb::Value { item: Some(item) }
}

impl TryFrom<result_pb::Entry> for Entry {
    type Error = ParsePbError;

    fn try_from(entry_pb: result_pb::Entry) -> Result<Self, Self::Error> {
        if let Some(inner) = entry_pb.inner {
            match inner {
                result_pb::entry::Inner::Element(e) => Ok(Entry::Element(e.try_into()?)),
                result_pb::entry::Inner::Collection(c) => Ok(Entry::Collection(
                    c.collection
                        .into_iter()
                        .map(|e| e.try_into())
                        .collect::<Result<Vec<_>, Self::Error>>()?,
                )),
            }
        } else {
            Err(ParsePbError::EmptyFieldError(
                "entry inner is empty".to_string(),
            ))?
        }
    }
}

impl TryFrom<result_pb::Element> for RecordElement {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Element) -> Result<Self, Self::Error> {
        if let Some(inner) = e.inner {
            match inner {
                result_pb::element::Inner::Vertex(v) => Ok(RecordElement::OnGraph(v.into())),
                result_pb::element::Inner::Edge(e) => Ok(RecordElement::OnGraph(e.into())),
                result_pb::element::Inner::Object(_o) => Err(ParsePbError::NotSupported(
                    "Cannot parse object".to_string(),
                )),
            }
        } else {
            Err(ParsePbError::EmptyFieldError(
                "element inner is empty".to_string(),
            ))?
        }
    }
}

impl From<result_pb::Vertex> for VertexOrEdge {
    fn from(v: result_pb::Vertex) -> Self {
        let vertex = Vertex::new(DynDetails::new(DefaultDetails::new(
            v.id as ID,
            v.label.unwrap().try_into().unwrap(),
        )));
        VertexOrEdge::V(vertex)
    }
}

impl From<result_pb::Edge> for VertexOrEdge {
    fn from(e: result_pb::Edge) -> Self {
        let mut edge = Edge::new(
            e.src_id as ID,
            e.dst_id as ID,
            DynDetails::new(DefaultDetails::new(
                e.id as ID,
                e.label.unwrap().try_into().unwrap(),
            )),
        );
        edge.set_src_label(e.src_label.unwrap().try_into().unwrap());
        edge.set_dst_label(e.dst_label.unwrap().try_into().unwrap());
        VertexOrEdge::E(edge)
    }
}
