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

use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::Deref;

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::sink::MetaType;
use ir_common::generated::common as common_pb;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
use pegasus::api::function::{FnResult, MapFunction};

use crate::error::FnGenResult;
use crate::graph::element::{Edge, GraphElement, Vertex, VertexOrEdge};
use crate::process::operator::sink::SinkFunctionGen;
use crate::process::record::{Entry, ObjectElement, Record, RecordElement};

#[derive(Debug)]
pub struct RecordSinkEncoder {
    /// the given column tags to sink;
    sink_keys: Vec<NameOrId>,
    /// flag of sink head
    is_output_head: bool,
    /// A map from id to name; including type of Entity (Vertex in Graph Database),
    /// Relation (Edge in Graph Database) and Column (Property in Graph Database)
    schema_map: Option<HashMap<(MetaType, i32), String>>,
}

// sink head by default
impl Default for RecordSinkEncoder {
    fn default() -> Self {
        RecordSinkEncoder { sink_keys: vec![], is_output_head: true, schema_map: None }
    }
}

impl RecordSinkEncoder {
    fn entry_to_pb(&self, e: &Entry) -> result_pb::Entry {
        let inner = match e {
            Entry::Element(element) => {
                let element_pb = self.element_to_pb(element);
                Some(result_pb::entry::Inner::Element(element_pb))
            }
            Entry::Collection(collection) => {
                let mut collection_pb = Vec::with_capacity(collection.len());
                for element in collection.into_iter() {
                    let element_pb = self.element_to_pb(element);
                    collection_pb.push(element_pb);
                }
                Some(result_pb::entry::Inner::Collection(result_pb::Collection {
                    collection: collection_pb,
                }))
            }
        };
        result_pb::Entry { inner }
    }

    fn element_to_pb(&self, e: &RecordElement) -> result_pb::Element {
        let inner = match e {
            RecordElement::OnGraph(vertex_or_edge) => match vertex_or_edge {
                VertexOrEdge::V(v) => {
                    let vertex_pb = self.vertex_to_pb(v);
                    Some(result_pb::element::Inner::Vertex(vertex_pb))
                }
                VertexOrEdge::E(e) => {
                    let edge_pb = self.edge_to_pb(e);
                    Some(result_pb::element::Inner::Edge(edge_pb))
                }
            },
            RecordElement::OffGraph(o) => match o {
                ObjectElement::None => None,
                ObjectElement::Prop(obj) | ObjectElement::Agg(obj) => {
                    Some(result_pb::element::Inner::Object(obj.clone().into()))
                }
                ObjectElement::Count(cnt) => {
                    let item = if *cnt <= (i64::MAX as u64) {
                        common_pb::value::Item::I64(*cnt as i64)
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

    fn label_to_pb(&self, label: NameOrId, t: MetaType) -> common_pb::NameOrId {
        let mapped_label = if let Some(schema_map) = self.schema_map.as_ref() {
            match label {
                NameOrId::Str(_) => label,
                NameOrId::Id(id) => {
                    let str_label = schema_map.get(&(t, id)).unwrap();
                    NameOrId::Str(str_label.clone())
                }
            }
        } else {
            label
        };
        mapped_label.into()
    }

    fn vertex_to_pb(&self, v: &Vertex) -> result_pb::Vertex {
        result_pb::Vertex {
            id: v.id() as i64,
            label: v
                .label()
                .map(|label| self.label_to_pb(label.clone(), MetaType::Entity)),
            // TODO: return detached vertex without property for now
            properties: vec![],
        }
    }

    fn edge_to_pb(&self, e: &Edge) -> result_pb::Edge {
        result_pb::Edge {
            id: e.id() as i64,
            label: e
                .label()
                .map(|label| self.label_to_pb(label.clone(), MetaType::Relation)),
            src_id: e.src_id as i64,
            src_label: e
                .get_src_label()
                .map(|label| self.label_to_pb(label.clone(), MetaType::Entity)),
            dst_id: e.dst_id as i64,
            dst_label: e
                .get_dst_label()
                .map(|label| self.label_to_pb(label.clone(), MetaType::Entity)),
            // TODO: return detached edge without property for now
            properties: vec![],
        }
    }
}

impl MapFunction<Record, result_pb::Results> for RecordSinkEncoder {
    fn exec(&self, input: Record) -> FnResult<result_pb::Results> {
        let mut sink_columns = Vec::with_capacity(self.sink_keys.len());
        if self.is_output_head {
            let entry = input.get(None);
            let entry_pb = entry.map(|entry| self.entry_to_pb(entry.deref()));
            let column_pb = result_pb::Column { name_or_id: None, entry: entry_pb };
            sink_columns.push(column_pb);
        }
        for sink_key in self.sink_keys.iter() {
            let entry = input.get(Some(sink_key));
            let entry_pb = entry.map(|entry| self.entry_to_pb(entry.deref()));
            let column_pb = result_pb::Column {
                name_or_id: Some(common_pb::NameOrId::from(sink_key.clone())),
                entry: entry_pb,
            };
            sink_columns.push(column_pb);
        }

        let record_pb = result_pb::Record { columns: sink_columns };
        let results = result_pb::Results { inner: Some(result_pb::results::Inner::Record(record_pb)) };
        Ok(results)
    }
}

impl SinkFunctionGen for algebra_pb::Sink {
    fn gen_sink(self) -> FnGenResult<Box<dyn MapFunction<Record, result_pb::Results>>> {
        let sink_keys = self
            .tags
            .into_iter()
            .map(|tag| tag.try_into())
            .collect::<Result<_, _>>()?;
        let is_output_head = self.sink_current;
        let mut schema_map = HashMap::new();
        for id_name_mappings_pb in self.id_name_mappings {
            let meta_type = unsafe { ::std::mem::transmute(id_name_mappings_pb.meta_type) };
            schema_map.insert((meta_type, id_name_mappings_pb.id), id_name_mappings_pb.name);
        }
        let record_sinker = RecordSinkEncoder {
            sink_keys,
            is_output_head,
            schema_map: if schema_map.is_empty() { None } else { Some(schema_map) },
        };
        debug!("Runtime sink operator: {:?}", record_sinker);
        Ok(Box::new(record_sinker))
    }
}

#[cfg(test)]
mod tests {
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::generated::results as result_pb;
    use pegasus::api::{Map, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::graph::element::{Edge, Vertex};
    use crate::graph::property::{DefaultDetails, DynDetails};
    use crate::process::operator::sink::SinkFunctionGen;
    use crate::process::record::Record;

    fn sink_test(source: Vec<Record>, sink_opr_pb: pb::Sink) -> ResultStream<result_pb::Results> {
        let conf = JobConf::new("sink_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let sink_opr_pb = sink_opr_pb.clone();
            |input, output| {
                let stream = input.input_from(source)?;
                let ec = sink_opr_pb.gen_sink().unwrap();
                stream
                    .map(move |record| ec.exec(record))?
                    .sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    // g.V()
    #[test]
    fn sink_vertex_label_mapping_test() {
        let v1 = Vertex::new(1, Some(1.into()), DynDetails::new(DefaultDetails::default()));
        let v2 = Vertex::new(2, Some(2.into()), DynDetails::new(DefaultDetails::default()));

        let sink_opr_pb = pb::Sink {
            tags: vec![],
            sink_current: true,
            id_name_mappings: vec![
                pb::sink::IdNameMapping {
                    id: 1,
                    name: "person".to_string(),
                    meta_type: 0, // pb::sink::MetaType::Entity
                },
                pb::sink::IdNameMapping {
                    id: 2,
                    name: "software".to_string(),
                    meta_type: 0, // pb::sink::MetaType::Entity
                },
            ],
        };

        let mut result = sink_test(vec![Record::new(v1, None), Record::new(v2, None)], sink_opr_pb);
        let mut result_id_labels = vec![];
        while let Some(Ok(result_pb)) = result.next() {
            if let Some(result_pb::results::Inner::Record(record)) = result_pb.inner {
                assert_eq!(record.columns.len(), 1);
                let entry = record
                    .columns
                    .get(0)
                    .unwrap()
                    .entry
                    .as_ref()
                    .unwrap();
                if let Some(result_pb::entry::Inner::Element(e)) = entry.inner.as_ref() {
                    if let Some(result_pb::element::Inner::Vertex(v)) = e.inner.as_ref() {
                        result_id_labels.push((v.id, v.label.clone().unwrap()))
                    }
                }
            }
        }
        result_id_labels.sort_by(|a, b| a.0.cmp(&b.0));

        let expected_results = vec![
            (
                1,
                common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Name("person".to_string())) },
            ),
            (
                2,
                common_pb::NameOrId {
                    item: Some(common_pb::name_or_id::Item::Name("software".to_string())),
                },
            ),
        ];

        assert_eq!(result_id_labels, expected_results);
    }

    // g.E()
    #[test]
    fn sink_edge_label_mapping_test() {
        // label_mapping:
        // vlabel: 11:  person, 22:  software,
        // elabel: 111: create, 222: created_by
        let mut e1 = Edge::new(1, Some(111.into()), 1, 2, DynDetails::new(DefaultDetails::default()));
        e1.set_src_label(Some(11.into()));
        e1.set_dst_label(Some(22.into()));

        let mut e2 = Edge::new(2, Some(222.into()), 2, 1, DynDetails::new(DefaultDetails::default()));
        e2.set_src_label(Some(22.into()));
        e2.set_dst_label(Some(11.into()));

        let sink_opr_pb = pb::Sink {
            tags: vec![],
            sink_current: true,
            id_name_mappings: vec![
                pb::sink::IdNameMapping {
                    id: 11,
                    name: "person".to_string(),
                    meta_type: 0, // pb::sink::MetaType::Entity
                },
                pb::sink::IdNameMapping {
                    id: 22,
                    name: "software".to_string(),
                    meta_type: 0, // pb::sink::MetaType::Entity
                },
                pb::sink::IdNameMapping {
                    id: 111,
                    name: "create".to_string(),
                    meta_type: 1, // pb::sink::MetaType::Relation
                },
                pb::sink::IdNameMapping {
                    id: 222,
                    name: "created_by".to_string(),
                    meta_type: 1, // pb::sink::MetaType::Relation
                },
            ],
        };

        let mut result = sink_test(vec![Record::new(e1, None), Record::new(e2, None)], sink_opr_pb);
        let mut result_eid_labels = vec![];
        while let Some(Ok(result_pb)) = result.next() {
            if let Some(result_pb::results::Inner::Record(record)) = result_pb.inner {
                assert_eq!(record.columns.len(), 1);
                let entry = record
                    .columns
                    .get(0)
                    .unwrap()
                    .entry
                    .as_ref()
                    .unwrap();
                if let Some(result_pb::entry::Inner::Element(e)) = entry.inner.as_ref() {
                    if let Some(result_pb::element::Inner::Edge(e)) = e.inner.as_ref() {
                        result_eid_labels.push((
                            e.src_id,
                            e.src_label.clone().unwrap(),
                            e.dst_id,
                            e.dst_label.clone().unwrap(),
                            e.id,
                            e.label.clone().unwrap(),
                        ));
                    }
                }
            }
        }
        result_eid_labels.sort_by(|a, b| a.0.cmp(&b.0));

        let expected_results = vec![
            (
                1,
                common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Name("person".to_string())) },
                2,
                common_pb::NameOrId {
                    item: Some(common_pb::name_or_id::Item::Name("software".to_string())),
                },
                1,
                common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Name("create".to_string())) },
            ),
            (
                2,
                common_pb::NameOrId {
                    item: Some(common_pb::name_or_id::Item::Name("software".to_string())),
                },
                1,
                common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Name("person".to_string())) },
                2,
                common_pb::NameOrId {
                    item: Some(common_pb::name_or_id::Item::Name("created_by".to_string())),
                },
            ),
        ];

        assert_eq!(result_eid_labels, expected_results);
    }
}
