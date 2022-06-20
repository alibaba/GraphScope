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
use std::sync::Arc;

use graph_proxy::apis::WriteGraphProxy;
use graph_proxy::VineyardGraphWriter;
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;
use ir_common::KeyId;

use crate::error::{FnExecError, FnExecResult, FnGenResult};
use crate::process::operator::accum::accumulator::Accumulator;
use crate::process::operator::sink::{SinkGen, Sinker};
use crate::process::record::Record;

#[derive(Clone, Debug)]
pub enum VineyardSinker {
    Writer(VinyardWriter),
    Dummy,
}

impl Accumulator<Record, Record> for VineyardSinker {
    fn accum(&mut self, next: Record) -> FnExecResult<()> {
        match self {
            VineyardSinker::Writer(vineyard_writer) => vineyard_writer.accum(next),
            VineyardSinker::Dummy => Ok(()),
        }
    }

    fn finalize(&mut self) -> FnExecResult<Record> {
        match self {
            VineyardSinker::Writer(vineyard_writer) => vineyard_writer.finalize(),
            VineyardSinker::Dummy => Ok(Record::default()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VinyardWriter {
    graph_writer: Arc<VineyardGraphWriter>,
    sink_keys: Vec<Option<KeyId>>,
}

impl Accumulator<Record, Record> for VinyardWriter {
    fn accum(&mut self, mut next: Record) -> FnExecResult<()> {
        for sink_key in &self.sink_keys {
            let entry = next
                .take(sink_key.as_ref())
                .ok_or(FnExecError::get_tag_error(&format!("tag {:?} in GraphWriter", sink_key)))?;
            if let Some(v) = entry.as_graph_vertex() {
                Arc::get_mut(&mut self.graph_writer)
                    .ok_or(FnExecError::accum_error("get mut graph writer failed"))?
                    .add_vertex(v.clone())?
            } else if let Some(e) = entry.as_graph_edge() {
                Arc::get_mut(&mut self.graph_writer)
                    .ok_or(FnExecError::accum_error("get mut graph writer failed"))?
                    .add_edge(e.clone())?
            } else {
                Err(FnExecError::unexpected_data_error("neither vertex nor edge in GraphWriter"))?
            }
        }
        Ok(())
    }

    fn finalize(&mut self) -> FnExecResult<Record> {
        Arc::get_mut(&mut self.graph_writer)
            .ok_or(FnExecError::accum_error("get mut graph writer failed"))?
            .finish()?;
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
            let worker = pegasus::get_current_worker();
            if worker.index % worker.local_peers == 0 {
                // the first worker on current server are assigned to sink to vineyard
                let graph_writer =
                    VineyardGraphWriter::new(self.graph_name, &graph_schema, worker.server_index as i32)?;
                let graph_writer = VinyardWriter { graph_writer: Arc::new(graph_writer), sink_keys };
                debug!("Runtime sink graph operator: {:?}", graph_writer);
                Ok(Sinker::GraphSinker(VineyardSinker::Writer(graph_writer)))
            } else {
                // other workers do nothing
                debug!("Runtime dummy sink graph operator");
                Ok(Sinker::GraphSinker(VineyardSinker::Dummy))
            }
        } else {
            Err(ParsePbError::EmptyFieldError("graph_schema in SinkVineyardOp".to_string()))?
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use dyn_type::Object;
    use graph_proxy::apis::{DefaultDetails, DynDetails, Edge, GraphElement, Vertex, WriteGraphProxy};
    use graph_proxy::{GraphProxyError, GraphProxyResult};
    use ir_common::NameOrId;
    use pegasus::api::{Fold, Sink};
    use pegasus::result::ResultStream;
    use pegasus::JobConf;

    use crate::error::FnExecResult;
    use crate::process::operator::accum::accumulator::Accumulator;
    use crate::process::operator::tests::{init_source, init_vertex1, init_vertex2};
    use crate::process::record::Record;

    #[derive(Default, Debug)]
    pub struct TestGraphWriter {
        vertices: Arc<Mutex<Vec<Vertex>>>,
        edges: Arc<Mutex<Vec<Edge>>>,
    }

    impl Clone for TestGraphWriter {
        fn clone(&self) -> Self {
            TestGraphWriter { vertices: self.vertices.clone(), edges: self.edges.clone() }
        }
    }

    impl WriteGraphProxy for TestGraphWriter {
        fn add_vertex(&mut self, vertex: Vertex) -> GraphProxyResult<()> {
            self.vertices
                .lock()
                .map_err(|_e| GraphProxyError::write_graph_error("add_vertex failed"))?
                .push(vertex);
            Ok(())
        }

        fn add_vertices(&mut self, vertices: Vec<Vertex>) -> GraphProxyResult<()> {
            for vertex in vertices {
                self.vertices
                    .lock()
                    .map_err(|_e| GraphProxyError::write_graph_error("add_vertices failed"))?
                    .push(vertex);
            }
            Ok(())
        }

        fn add_edge(&mut self, edge: Edge) -> GraphProxyResult<()> {
            self.edges
                .lock()
                .map_err(|_e| GraphProxyError::write_graph_error("add_edge failed"))?
                .push(edge);
            Ok(())
        }

        fn add_edges(&mut self, edges: Vec<Edge>) -> GraphProxyResult<()> {
            for edge in edges {
                self.edges
                    .lock()
                    .map_err(|_e| GraphProxyError::write_graph_error("add_edges failed"))?
                    .push(edge);
            }
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

    impl Accumulator<Record, Record> for TestGraphWriter {
        fn accum(&mut self, mut next: Record) -> FnExecResult<()> {
            let entry = next.take(None).unwrap();
            if let Some(v) = entry.as_graph_vertex() {
                self.add_vertex(v.clone())?
            } else if let Some(e) = entry.as_graph_edge() {
                self.add_edge(e.clone())?
            }

            Ok(())
        }

        fn finalize(&mut self) -> FnExecResult<Record> {
            self.finish()?;
            Ok(Record::default())
        }
    }

    fn init_edge1() -> Edge {
        let map1: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(12)), ("score".into(), object!(11))]
                .into_iter()
                .collect();
        Edge::new(12, Some(1.into()), 1, 2, DynDetails::new(DefaultDetails::new(map1)))
    }

    fn init_edge2() -> Edge {
        let map2: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(21)), ("score".into(), object!(22))]
                .into_iter()
                .collect();
        Edge::new(21, Some(1.into()), 2, 1, DynDetails::new(DefaultDetails::new(map2)))
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
