//
//! Copyright 2020 Alibaba Group Holding Limited.
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
//!

#[cfg(test)]
#[allow(dead_code)]
pub mod test {

    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use gremlin_core::compiler::GremlinJobCompiler;
    use gremlin_core::process::traversal::traverser::Traverser;
    use gremlin_core::{create_demo_graph, DynIter, Element, Object, Partitioner, ID};
    use pegasus::api::function::{
        CompareFunction, EncodeFunction, FilterFunction, FlatMapFunction, LeftJoinFunction,
        MapFunction, MultiRouteFunction, RouteFunction,
    };
    use pegasus::{Configuration, StartupError};
    use pegasus_common::collections::{Collection, CollectionFactory, Set};
    use pegasus_server::factory::{CompileResult, FoldFunction, GroupFunction, JobCompiler};
    use pegasus_server::service::{Output, Service};
    use pegasus_server::{JobRequest, JobResponse, JobResult};
    use prost::Message;
    use std::path::{Path, PathBuf};
    use std::sync::Once;

    const TEST_PLAN_PATH: &'static str = "resource/test/query_plans";

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            start_pegasus();
        });
    }

    pub struct TestJobFactory {
        inner: GremlinJobCompiler,
        expected_ids: Option<Vec<ID>>,
        expected_values: Option<Vec<Object>>,
        // is_ordered flag, if true, indicates that the expected_ids is ordered.
        is_ordered: bool,
    }

    impl TestJobFactory {
        pub fn with_expect_ids(expected_ids: Vec<ID>) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition, 1, 0),
                expected_ids: Some(expected_ids),
                expected_values: None,
                is_ordered: false,
            }
        }

        pub fn with_expect_values(expected_values: Vec<Object>) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition, 1, 0),
                expected_ids: None,
                expected_values: Some(expected_values),
                is_ordered: false,
            }
        }

        pub fn set_ordered(&mut self, ordered: bool) {
            self.is_ordered = ordered;
        }
    }

    pub struct TestSinkEncoder {
        expected_ids: Option<Vec<ID>>,
        expected_values: Option<Vec<Object>>,
        is_ordered: bool,
    }

    impl EncodeFunction<Traverser> for TestSinkEncoder {
        fn encode(&self, data: Vec<Traverser>) -> Vec<u8> {
            let mut id_result = vec![];
            let mut obj_result = vec![];
            for traverser in data {
                if let Some(element) = traverser.get_element() {
                    id_result.push(element.id() as ID);
                } else if let Some(object) = traverser.get_object() {
                    obj_result.push(object.clone());
                }
            }
            if !self.is_ordered {
                id_result.sort();
            }
            //  obj_result.sort();
            if self.expected_ids.is_some() {
                assert_eq!(self.expected_ids.as_ref().unwrap(), &id_result);
            } else if self.expected_values.is_some() {
                assert_eq!(self.expected_values.as_ref().unwrap(), &obj_result);
            } else {
                println!("no expected values specified in test");
            }
            vec![]
        }
    }

    impl JobCompiler<Traverser> for TestJobFactory {
        fn shuffle(&self, res: &[u8]) -> CompileResult<Box<dyn RouteFunction<Traverser>>> {
            self.inner.shuffle(res)
        }

        fn broadcast(&self, res: &[u8]) -> CompileResult<Box<dyn MultiRouteFunction<Traverser>>> {
            self.inner.broadcast(res)
        }

        fn source(&self, src: &[u8]) -> CompileResult<Box<dyn Iterator<Item = Traverser> + Send>> {
            self.inner.source(src)
        }

        fn map(&self, res: &[u8]) -> CompileResult<Box<dyn MapFunction<Traverser, Traverser>>> {
            self.inner.map(res)
        }

        fn flat_map(
            &self, res: &[u8],
        ) -> CompileResult<
            Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>,
        > {
            self.inner.flat_map(res)
        }

        fn filter(&self, res: &[u8]) -> CompileResult<Box<dyn FilterFunction<Traverser>>> {
            self.inner.filter(res)
        }

        fn left_join(&self, res: &[u8]) -> CompileResult<Box<dyn LeftJoinFunction<Traverser>>> {
            self.inner.left_join(res)
        }

        fn compare(&self, res: &[u8]) -> CompileResult<Box<dyn CompareFunction<Traverser>>> {
            self.inner.compare(res)
        }

        fn group(
            &self, map_factory: &[u8], unfold: &[u8], sink: &[u8],
        ) -> CompileResult<Box<dyn GroupFunction<Traverser>>> {
            self.inner.group(map_factory, unfold, sink)
        }

        fn fold(
            &self, accum: &[u8], unfold: &[u8], sink: &[u8],
        ) -> CompileResult<Box<dyn FoldFunction<Traverser>>> {
            self.inner.fold(accum, unfold, sink)
        }

        fn collection_factory(
            &self, res: &[u8],
        ) -> CompileResult<
            Box<dyn CollectionFactory<Traverser, Target = Box<dyn Collection<Traverser>>>>,
        > {
            self.inner.collection_factory(res)
        }

        fn set_factory(
            &self, res: &[u8],
        ) -> CompileResult<Box<dyn CollectionFactory<Traverser, Target = Box<dyn Set<Traverser>>>>>
        {
            self.inner.set_factory(res)
        }

        fn sink(&self, _res: &[u8]) -> CompileResult<Box<dyn EncodeFunction<Traverser>>> {
            Ok(Box::new(TestSinkEncoder {
                expected_ids: self.expected_ids.clone(),
                expected_values: self.expected_values.clone(),
                is_ordered: self.is_ordered,
            }))
        }
    }

    pub fn start_pegasus() {
        pegasus_common::logs::init_log();
        match pegasus::startup(Configuration::singleton()) {
            Ok(_) => {
                create_demo_graph();
            }
            Err(err) => match err {
                StartupError::AlreadyStarted(_) => {}
                _ => panic!("start pegasus failed"),
            },
        }
    }

    pub fn start_test_service(factory: TestJobFactory) -> Service<Traverser> {
        let service = Service::new(factory);
        service
    }

    pub fn read_pb_request<P: AsRef<Path>>(file_name: P) -> Option<JobRequest> {
        if let Ok(content) = std::fs::read(&file_name) {
            {
                if let Ok(pb_request) = JobRequest::decode(&content[0..]) {
                    Some(pb_request)
                } else {
                    println!("downcast pb_request failed");
                    None
                }
            }
        } else {
            println!("read file {:?} failed", file_name.as_ref());
            None
        }
    }

    pub struct TestPartition;

    impl Partitioner for TestPartition {
        fn get_partition(&self, _: &u128, _: u32) -> u64 {
            0
        }
    }

    #[derive(Clone)]
    pub struct TestOutputStruct;

    impl Output for TestOutputStruct {
        fn send(&self, res: JobResponse) {
            if let Some(result) = res.result {
                if let JobResult::Err(_) = result {
                    panic!("send result into test output failure");
                }
            }
        }

        fn close(&self) {}
    }

    fn modern_graph_vid_to_global_id(id: usize) -> DefaultId {
        match id {
            1 | 2 | 4 | 6 => LDBCVertexParser::to_global_id(id, 0),
            3 | 5 => LDBCVertexParser::to_global_id(id, 1),
            _ => unreachable!(),
        }
    }

    pub fn to_global_ids(ids: Vec<usize>) -> Vec<ID> {
        let mut global_ids = vec![];
        for id in ids {
            global_ids.push(modern_graph_vid_to_global_id(id) as ID);
        }
        global_ids
    }

    pub fn eids_to_global_ids(edges: Vec<(usize, usize)>) -> Vec<ID> {
        let mut global_ids = vec![];
        for (src, dst) in edges {
            let g_src = modern_graph_vid_to_global_id(src);
            let g_dst = modern_graph_vid_to_global_id(dst);
            let eid = ((g_src as ID) << 64) | g_dst as ID;
            global_ids.push(eid);
        }
        global_ids
    }

    pub fn gen_path(file: &str) -> PathBuf {
        Path::new(TEST_PLAN_PATH).join(file.to_string())
    }

    pub fn submit_query(service: &Service<Traverser>, job_req: JobRequest) {
        let job_id = job_req.conf.clone().expect("no job_conf").job_id;
        println!("job_id: {}", job_id);
        service.accept(job_req, TestOutputStruct);
        if let Ok(mut job_guards) = service.job_guards.write() {
            if let Some(job_guard) = job_guards.get_mut(&job_id) {
                job_guard.join().expect("run query failed");
            }
        }
    }
}
