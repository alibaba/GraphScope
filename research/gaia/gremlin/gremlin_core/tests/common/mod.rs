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
//!

#[cfg(test)]
#[allow(dead_code)]
pub mod test {

    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use gremlin_core::compiler::GremlinJobCompiler;
    use gremlin_core::process::traversal::path::ResultPath;
    use gremlin_core::process::traversal::step::result_downcast::{
        try_downcast_count, try_downcast_list, try_downcast_pair,
    };
    use gremlin_core::process::traversal::step::{graph_step_from, ResultProperty};
    use gremlin_core::process::traversal::traverser::{Requirement, Traverser};
    use gremlin_core::structure::Details;
    use gremlin_core::GremlinStepPb;
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
        expected_path_result: Option<Vec<Vec<ID>>>,
        expected_group_result: Option<Vec<(ID, Vec<ID>)>>,
        requirement: Requirement,
        // is_ordered flag, if true, indicates that the expected_ids is ordered.
        is_ordered: bool,
        // to test property optimization, with the saved property_names and unsaved prperty_names
        expected_properties: Option<(Vec<String>, Vec<String>)>,
        // to test remove tag optimization, with the expected history path length
        expected_path_len: Option<usize>,
    }

    impl TestJobFactory {
        pub fn with_expect_ids(expected_ids: Vec<ID>) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition { num_servers: 1 }, 1, 0),
                expected_ids: Some(expected_ids),
                expected_values: None,
                expected_path_result: None,
                expected_group_result: None,
                requirement: Requirement::OBJECT,
                is_ordered: false,
                expected_properties: None,
                expected_path_len: None,
            }
        }

        pub fn with_expect_values(expected_values: Vec<Object>) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition { num_servers: 1 }, 1, 0),
                expected_ids: None,
                expected_values: Some(expected_values),
                expected_path_result: None,
                expected_group_result: None,
                requirement: Requirement::OBJECT,
                is_ordered: false,
                expected_properties: None,
                expected_path_len: None,
            }
        }

        pub fn with_expect_path_result(expected_path_result: Vec<Vec<ID>>) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition { num_servers: 1 }, 1, 0),
                expected_ids: None,
                expected_values: None,
                expected_path_result: Some(expected_path_result),
                expected_group_result: None,
                requirement: Requirement::OBJECT,
                is_ordered: false,
                expected_properties: None,
                expected_path_len: None,
            }
        }

        pub fn with_expect_map_result(expected_map_result: Vec<(ID, Vec<ID>)>) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition { num_servers: 1 }, 1, 0),
                expected_ids: None,
                expected_values: None,
                expected_path_result: None,
                expected_group_result: Some(expected_map_result),
                requirement: Requirement::PATH,
                is_ordered: false,
                expected_properties: None,
                expected_path_len: None,
            }
        }

        pub fn with_expect_property_opt(expected_props: (Vec<String>, Vec<String>)) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition { num_servers: 1 }, 1, 0),
                expected_ids: None,
                expected_values: None,
                expected_path_result: None,
                expected_group_result: None,
                requirement: Requirement::OBJECT,
                is_ordered: false,
                expected_properties: Some(expected_props),
                expected_path_len: None,
            }
        }

        pub fn with_expect_path_len(expected_path_len: usize) -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(TestPartition { num_servers: 1 }, 1, 0),
                expected_ids: None,
                expected_values: None,
                expected_path_result: None,
                expected_group_result: None,
                requirement: Requirement::OBJECT,
                is_ordered: false,
                expected_properties: None,
                expected_path_len: Some(expected_path_len),
            }
        }

        pub fn set_ordered(&mut self, ordered: bool) {
            self.is_ordered = ordered;
        }

        pub fn set_requirement(&mut self, requirement: Requirement) {
            self.requirement = requirement;
        }
    }

    pub struct TestSinkEncoder {
        expected_ids: Option<Vec<ID>>,
        expected_values: Option<Vec<Object>>,
        expected_paths: Option<Vec<Vec<ID>>>,
        expected_group_result: Option<Vec<(ID, Vec<ID>)>>,
        is_ordered: bool,
        property_opt: Option<(Vec<String>, Vec<String>)>,
        expected_path_len: Option<usize>,
    }

    impl EncodeFunction<Traverser> for TestSinkEncoder {
        fn encode(&self, data: Vec<Traverser>) -> Vec<u8> {
            println!("result to encode {:?}", data);
            let mut id_result = vec![];
            let mut obj_result = vec![];
            let mut path_result = vec![];
            let mut map_result = vec![];
            for traverser in data.iter() {
                if let Some(element) = traverser.get_element() {
                    if let Some(property_opt) = self.property_opt.clone() {
                        // to test property optimization, we assume the last op generates graph_element traverser
                        for saved_properties in property_opt.0.iter() {
                            assert!(element.details().get_property(saved_properties).is_some());
                        }
                        for unsaved_properties in property_opt.1.iter() {
                            assert!(element.details().get_property(unsaved_properties).is_none());
                        }
                    } else if let Some(expected_path_len) = self.expected_path_len {
                        // to test remove tag optimization, we assume the last op generates graph_element traverser
                        assert_eq!(expected_path_len, traverser.get_path_len())
                    } else {
                        id_result.push(element.id() as ID);
                    }
                } else if let Some(o) = traverser.get_object() {
                    match o {
                        Object::Primitive(_) | Object::String(_) | Object::Blob(_) => {
                            obj_result.push(o.clone());
                        }
                        Object::UnknownOwned(x) => {
                            if let Some(p) = x.try_downcast_ref::<ResultPath>() {
                                let mut path = vec![];
                                for item in p.iter() {
                                    path.push(item.as_element().expect("element").id());
                                }
                                path_result.push(path);
                            } else if let Some(_result_prop) =
                                x.try_downcast_ref::<ResultProperty>()
                            {
                                // TODO: compare this with expected result (may need to redefine the struct of ResultProperty)
                            } else if let Some(result_pair) = try_downcast_pair(o) {
                                let group_key =
                                    if let Some(graph_element) = result_pair.0.get_element() {
                                        graph_element.id()
                                    } else if let Some(obj) = result_pair.0.get_object() {
                                        obj.as_u128().expect("cannot cast to u128")
                                    } else {
                                        unreachable!()
                                    };
                                let group_value =
                                    result_pair.1.get_object().expect("we assume value is object");
                                if let Some(count) = try_downcast_count(group_value) {
                                    map_result.push((group_key, vec![count as u128]));
                                } else if let Some(list) = try_downcast_list(group_value) {
                                    let value_list: Vec<u128> = list
                                        .into_iter()
                                        .map(|tra| tra.get_element().expect("assume element").id())
                                        .collect();
                                    map_result.push((group_key, value_list));
                                }
                            }
                        }
                        _ => {
                            break;
                        }
                    }
                }
            }
            if !self.is_ordered {
                id_result.sort();
            }
            if self.expected_ids.is_some() {
                assert_eq!(self.expected_ids.as_ref().unwrap(), &id_result);
            } else if self.expected_paths.is_some() {
                assert_eq!(self.expected_paths.as_ref().unwrap(), &path_result);
            } else if self.expected_group_result.is_some() {
                assert_eq!(self.expected_group_result.as_ref().unwrap(), &map_result);
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
            let mut step = GremlinStepPb::decode(&src[0..])
                .map_err(|e| format!("protobuf decode failure: {}", e))?;
            if let Some(worker_id) = pegasus::get_current_worker() {
                let num_workers = worker_id.peers / self.inner.get_num_servers();
                let mut step = graph_step_from(&mut step, self.inner.get_num_servers())?;
                step.set_num_workers(num_workers);
                step.set_server_index(self.inner.get_server_index());
                step.set_requirement(self.requirement);
                Ok(step.gen_source(Some(worker_id.index)))
            } else {
                let mut step = graph_step_from(&mut step, self.inner.get_num_servers())?;
                step.set_server_index(self.inner.get_server_index());
                step.set_requirement(self.requirement);
                Ok(step.gen_source(None))
            }
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
                expected_paths: self.expected_path_result.clone(),
                expected_group_result: self.expected_group_result.clone(),
                is_ordered: self.is_ordered,
                property_opt: self.expected_properties.clone(),
                expected_path_len: self.expected_path_len,
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

    fn start_test_service(factory: TestJobFactory) -> Service<Traverser> {
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

    struct TestPartition {
        pub num_servers: usize,
    }

    impl Partitioner for TestPartition {
        fn get_partition(&self, id: &u128, job_workers: u32) -> u64 {
            let workers = job_workers as usize;
            let id_usize = *id as usize;
            (id_usize % self.num_servers * workers + (_hash(id_usize)) % workers) as u64
        }
    }

    fn _hash(key: usize) -> usize {
        fasthash::metro::hash64_with_seed(&key.to_le_bytes(), 2654435761) as usize
    }

    #[derive(Clone)]
    struct TestOutputStruct;

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

    pub fn to_global_id(id: usize) -> DefaultId {
        match id {
            1 | 2 | 4 | 6 => LDBCVertexParser::to_global_id(id, 0),
            3 | 5 => LDBCVertexParser::to_global_id(id, 1),
            _ => unreachable!(),
        }
    }

    pub fn to_global_ids(ids: Vec<usize>) -> Vec<ID> {
        let mut global_ids = vec![];
        for id in ids {
            global_ids.push(to_global_id(id) as ID);
        }
        global_ids
    }

    pub fn eids_to_global_ids(edges: Vec<(usize, usize)>) -> Vec<ID> {
        let mut global_ids = vec![];
        for (src, dst) in edges {
            let g_src = to_global_id(src);
            let g_dst = to_global_id(dst);
            let eid = ((g_src as ID) << 64) | g_dst as ID;
            global_ids.push(eid);
        }
        global_ids
    }

    pub fn gen_path(file: &str) -> PathBuf {
        Path::new(TEST_PLAN_PATH).join(file.to_string())
    }

    fn submit_query(service: &Service<Traverser>, mut job_req: JobRequest, num_workers: u32) {
        let job_id = job_req.conf.clone().expect("no job_conf").job_id;
        job_req.conf.as_mut().expect("no job_conf").workers = num_workers;
        println!("job_id: {}", job_id);
        service.accept(job_req, TestOutputStruct);
        if let Ok(mut job_guards) = service.job_guards.write() {
            if let Some(job_guard) = job_guards.get_mut(&job_id) {
                job_guard.join().expect("run query failed");
            }
        }
    }

    pub fn run_test_with_worker_num(
        factory: TestJobFactory, job_request: JobRequest, num_workers: u32,
    ) {
        let service = start_test_service(factory);
        submit_query(&service, job_request, num_workers);
    }

    pub fn run_test(factory: TestJobFactory, job_request: JobRequest) {
        let service = start_test_service(factory);
        submit_query(&service, job_request, 1);
    }
}
