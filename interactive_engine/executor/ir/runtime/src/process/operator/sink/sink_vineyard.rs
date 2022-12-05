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

use std::convert::TryInto;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use graph_proxy::apis::{get_graph, GraphElement, WriteGraphProxy};
use graph_proxy::{GraphProxyError, VineyardGraphWriter};
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;
use ir_common::KeyId;

use crate::error::{FnExecError, FnExecResult, FnGenResult};
use crate::process::operator::accum::accumulator::Accumulator;
use crate::process::operator::sink::{SinkGen, Sinker};
use crate::process::record::Record;

#[derive(Clone, Debug)]
pub struct GraphSinkEncoder {
    graph_writer: Arc<Mutex<VineyardGraphWriter>>,
    sink_keys: Vec<Option<KeyId>>,
}

impl Accumulator<Record, Record> for GraphSinkEncoder {
    fn accum(&mut self, mut next: Record) -> FnExecResult<()> {
        let graph = get_graph().ok_or(FnExecError::NullGraphError)?;
        for sink_key in &self.sink_keys {
            let entry = next
                .take(sink_key.as_ref())
                .ok_or(FnExecError::get_tag_error(&format!(
                    "tag {:?} in GraphWriter on {:?}",
                    sink_key, next
                )))?;
            if let Some(v) = entry.as_graph_vertex() {
                let vertex_pk = graph
                    .get_primary_key(&v.id())?
                    .ok_or(GraphProxyError::query_store_error("get_primary_key() returns empty pk"))?;
                let label = v
                    .label()
                    .ok_or(FnExecError::unexpected_data_error(&format!(
                        "label of vertex {:?} is None in sink_vineyard",
                        v.id()
                    )))?;
                loop {
                    if let Ok(mut graph_writer_guard) = self.graph_writer.try_lock() {
                        graph_writer_guard.add_vertex(label.clone(), vertex_pk, v.details().cloned())?;
                        break;
                    }
                }
            } else if let Some(e) = entry.as_graph_edge() {
                let src_vertex_pk =
                    graph
                        .get_primary_key(&e.src_id)?
                        .ok_or(GraphProxyError::query_store_error(
                            "get_primary_key() of src_vertex returns empty pk",
                        ))?;
                let dst_vertex_pk =
                    graph
                        .get_primary_key(&e.dst_id)?
                        .ok_or(GraphProxyError::query_store_error(
                            "get_primary_key() of src_vertex returns empty pk",
                        ))?;
                let label = e
                    .label()
                    .ok_or(FnExecError::unexpected_data_error(&format!(
                        "label of edge {:?} is None in sink_vineyard",
                        e.id()
                    )))?;
                let src_label = e
                    .get_src_label()
                    .ok_or(FnExecError::unexpected_data_error(&format!(
                        "src_label of edge {:?} is None in sink_vineyard",
                        e.id()
                    )))?;
                let dst_label = e
                    .get_dst_label()
                    .ok_or(FnExecError::unexpected_data_error(&format!(
                        "dst_label of edge {:?} is None in sink_vineyard",
                        e.id()
                    )))?;
                loop {
                    if let Ok(mut graph_writer_guard) = self.graph_writer.try_lock() {
                        graph_writer_guard.add_edge(
                            label.clone(),
                            src_label.clone(),
                            src_vertex_pk,
                            dst_label.clone(),
                            dst_vertex_pk,
                            e.details().cloned(),
                        )?;
                        break;
                    }
                }
            } else {
                Err(FnExecError::unexpected_data_error("neither vertex nor edge in GraphWriter"))?
            }
        }
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Record> {
        loop {
            if let Ok(mut graph_writer_guard) = self.graph_writer.try_lock() {
                graph_writer_guard.finish()?;
                break;
            }
        }
        Ok(Record::default())
    }
}

pub struct SinkVineyardOp {
    pub tags: Vec<common_pb::NameOrIdKey>,
    pub graph_name: String,
    pub graph_schema: Option<schema_pb::Schema>,
}

impl SinkGen for SinkVineyardOp {
    fn gen_sink(self) -> FnGenResult<Sinker> {
        let mut sink_keys = Vec::with_capacity(self.tags.len());
        for sink_key_pb in self.tags.into_iter() {
            let sink_key = sink_key_pb
                .key
                .map(|tag| tag.try_into())
                .transpose()?;
            sink_keys.push(sink_key);
        }
        if let Some(graph_schema) = self.graph_schema {
            let graph_writer = VineyardGraphWriter::new(
                self.graph_name,
                &graph_schema,
                pegasus::get_current_worker().index as i32,
            )?;
            let graph_sink_encoder =
                GraphSinkEncoder { graph_writer: Arc::new(Mutex::new(graph_writer)), sink_keys };
            if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                debug!("Runtime sink graph operator: {:?}", graph_sink_encoder,);
            }
            Ok(Sinker::GraphSinker(graph_sink_encoder))
        } else {
            Err(ParsePbError::EmptyFieldError("graph_schema in SinkVineyardOp".to_string()))?
        }
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};

    use ahash::HashMap;
    use dyn_type::Object;
    use graph_proxy::apis::graph::PKV;
    use graph_proxy::apis::{DynDetails, Edge, GraphElement, Vertex, WriteGraphProxy, ID};
    use graph_proxy::{GraphProxyError, GraphProxyResult};
    use ir_common::{LabelId, NameOrId, OneOrMany};
    use pegasus::api::{Fold, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::error::FnExecResult;
    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::tests::{init_source, init_vertex1, init_vertex2, PERSON_LABEL};
    use crate::process::record::Record;

    #[derive(Default, Debug)]
    struct TestGraphWriter {
        vertices: Arc<Mutex<Vec<Vertex>>>,
        edges: Arc<Mutex<Vec<Edge>>>,
    }

    impl TestGraphWriter {
        fn encode_id(&self, pkv: PKV) -> ID {
            match pkv {
                OneOrMany::One(pkv) => {
                    let pk_value = &pkv[0].1;
                    pk_value.as_u64().unwrap()
                }
                OneOrMany::Many(_) => unreachable!(),
            }
        }
    }

    impl Clone for TestGraphWriter {
        fn clone(&self) -> Self {
            TestGraphWriter { vertices: self.vertices.clone(), edges: self.edges.clone() }
        }
    }

    impl WriteGraphProxy for TestGraphWriter {
        fn add_vertex(
            &mut self, label: LabelId, vertex_pk: PKV, properties: Option<DynDetails>,
        ) -> GraphProxyResult<()> {
            let vid = self.encode_id(vertex_pk);
            let vertex = Vertex::new(vid, Some(label.clone()), properties.unwrap().clone());
            self.vertices
                .lock()
                .map_err(|_e| GraphProxyError::write_graph_error("add_vertex failed"))?
                .push(vertex);
            Ok(())
        }

        fn add_edge(
            &mut self, label: LabelId, src_vertex_label: LabelId, src_vertex_pk: PKV,
            dst_vertex_label: LabelId, dst_vertex_pk: PKV, properties: Option<DynDetails>,
        ) -> GraphProxyResult<()> {
            let src_id = self.encode_id(src_vertex_pk);
            let dst_id = self.encode_id(dst_vertex_pk);
            let eid = encode_eid(src_id, dst_id);
            let mut edge = Edge::new(eid, Some(label.clone()), src_id, dst_id, properties.unwrap().clone());
            edge.set_src_label(src_vertex_label.clone());
            edge.set_src_label(dst_vertex_label.clone());
            self.edges
                .lock()
                .map_err(|_e| GraphProxyError::write_graph_error("add_edge failed"))?
                .push(edge);
            Ok(())
        }

        fn finish(&mut self) -> GraphProxyResult<()> {
            Ok(())
        }
    }

    impl TestGraphWriter {
        fn scan_vertex(&self) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
            Ok(Box::new(
                self.vertices
                    .lock()
                    .map_err(|_e| GraphProxyError::query_store_error("scan_vertex failed"))?
                    .clone()
                    .into_iter(),
            ))
        }

        fn scan_edge(&self) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
            Ok(Box::new(
                self.edges
                    .lock()
                    .map_err(|_e| GraphProxyError::query_store_error("scan_edge failed"))?
                    .clone()
                    .into_iter(),
            ))
        }
    }

    fn get_primary_key(id: &ID) -> PKV {
        ("".into(), Object::from(*id)).into()
    }

    impl Accumulator<Record, Record> for TestGraphWriter {
        fn accum(&mut self, mut next: Record) -> FnExecResult<()> {
            let entry = next.take(None).unwrap();
            if let Some(v) = entry.as_graph_vertex() {
                let pk = get_primary_key(&v.id());
                self.add_vertex(v.label().unwrap().clone(), pk, v.details().cloned())?;
            } else if let Some(e) = entry.as_graph_edge() {
                let src_pk = get_primary_key(&e.src_id);
                let dst_pk = get_primary_key(&e.dst_id);
                self.add_edge(
                    e.label().unwrap().clone(),
                    e.get_src_label().unwrap().clone(),
                    src_pk,
                    e.get_dst_label().unwrap().clone(),
                    dst_pk,
                    e.details().cloned(),
                )?;
            }

            Ok(())
        }

        fn finalize(&mut self) -> FnExecResult<Record> {
            self.finish()?;
            Ok(Record::default())
        }
    }

    fn encode_eid(src_id: ID, dst_id: ID) -> ID {
        src_id << 8 | dst_id
    }

    fn init_edge1() -> Edge {
        let map1: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(12)), ("score".into(), object!(11))]
                .into_iter()
                .collect();
        let mut e = Edge::new(encode_eid(1, 2), Some(1.into()), 1, 2, DynDetails::new(map1));
        e.set_src_label(PERSON_LABEL);
        e.set_dst_label(PERSON_LABEL);
        e
    }

    fn init_edge2() -> Edge {
        let map2: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(21)), ("score".into(), object!(22))]
                .into_iter()
                .collect();
        let mut e = Edge::new(encode_eid(2, 1), Some(1.into()), 2, 1, DynDetails::new(map2));
        e.set_src_label(PERSON_LABEL);
        e.set_dst_label(PERSON_LABEL);
        e
    }

    fn graph_writer_test(source: Vec<Record>, graph_writer: &TestGraphWriter) -> ResultStream<Record> {
        let conf = JobConf::new("subgraph_write_test");
        let result = pegasus::run(conf, || {
            let source = source.clone();
            let test_graph_writer = graph_writer.clone();
            |input, output| {
                let mut stream = input.input_from(source)?;
                stream = stream
                    .fold_partition(test_graph_writer, || {
                        |mut accumulator, next| {
                            accumulator.accum(next)?;
                            Ok(accumulator)
                        }
                    })?
                    .map(move |mut accum| Ok(accum.finalize()?))?
                    .into_stream()?;
                stream.sink_into(output)
            }
        })
        .expect("build job failure");

        result
    }

    #[test]
    fn write_vertex_test() {
        let source = init_source();
        let test_graph = TestGraphWriter::default();
        let mut result_stream = graph_writer_test(source, &test_graph);

        let mut expected_vertices = vec![init_vertex1(), init_vertex2()];
        while let Some(Ok(_record)) = result_stream.next() {
            let vertices = test_graph.scan_vertex().unwrap();
            let mut result = vec![];
            for v in vertices {
                result.push(v);
            }
            expected_vertices.sort_by(|v1, v2| v1.id().cmp(&v2.id()));
            result.sort_by(|v1, v2| v1.id().cmp(&v2.id()));
            assert_eq!(result, expected_vertices);
        }
    }

    #[test]
    fn write_edge_test() {
        let r1 = Record::new(init_edge1(), None);
        let r2 = Record::new(init_edge2(), None);
        let source = vec![r1, r2];
        let test_graph = TestGraphWriter::default();
        let mut result_stream = graph_writer_test(source, &test_graph);

        let mut expected_edges = vec![init_edge1(), init_edge2()];
        while let Some(Ok(_record)) = result_stream.next() {
            let edges = test_graph.scan_edge().unwrap();
            let mut result = vec![];
            for e in edges {
                result.push(e);
            }
            expected_edges.sort_by(|e1, e2| e1.id().cmp(&e2.id()));
            result.sort_by(|e1, e2| e1.id().cmp(&e2.id()));
            assert_eq!(result, expected_edges);
        }
    }

    #[test]
    fn write_subgraph_test() {
        let r1 = Record::new(init_vertex1(), None);
        let r2 = Record::new(init_vertex2(), None);
        let r3 = Record::new(init_edge1(), None);
        let r4 = Record::new(init_edge2(), None);
        let source = vec![r1, r2, r3, r4];
        let test_graph = TestGraphWriter::default();
        let mut result_stream = graph_writer_test(source, &test_graph);

        let mut expected_vertices = vec![init_vertex1(), init_vertex2()];
        let mut expected_edges = vec![init_edge1(), init_edge2()];
        while let Some(Ok(_record)) = result_stream.next() {
            let vertices = test_graph.scan_vertex().unwrap();
            let mut result_vertices = vec![];
            for v in vertices {
                result_vertices.push(v);
            }
            let edges = test_graph.scan_edge().unwrap();
            let mut result_edges = vec![];
            for e in edges {
                result_edges.push(e);
            }
            expected_vertices.sort_by(|v1, v2| v1.id().cmp(&v2.id()));
            result_vertices.sort_by(|v1, v2| v1.id().cmp(&v2.id()));
            assert_eq!(result_vertices, expected_vertices);
            expected_edges.sort_by(|e1, e2| e1.id().cmp(&e2.id()));
            result_edges.sort_by(|e1, e2| e1.id().cmp(&e2.id()));
            assert_eq!(result_edges, expected_edges);
        }
    }
}
