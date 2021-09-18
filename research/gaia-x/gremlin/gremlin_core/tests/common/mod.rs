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
#[allow(unused_imports)]
pub mod test {
    use core::time;
    use dyn_type::Object;
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use gremlin_core::compiler::GremlinJobCompiler;
    use gremlin_core::process::traversal::path::ResultPath;
    use gremlin_core::process::traversal::step::accum::Accumulator;
    use gremlin_core::process::traversal::step::functions::EncodeFunction;
    use gremlin_core::process::traversal::step::result_downcast::{
        try_downcast_list, try_downcast_pair,
    };
    use gremlin_core::process::traversal::step::{graph_step_from, ResultProperty};
    use gremlin_core::process::traversal::traverser::{Requirement, Traverser};
    use gremlin_core::structure::{Details, PropKey, Tag, VertexOrEdge};
    use gremlin_core::{create_demo_graph, str_to_dyn_error, DynIter, Element, Partitioner, ID};
    use gremlin_core::{GremlinStepPb, Partition};
    use pegasus::api::function::{
        FilterFunction, FlatMapFunction, FnResult, MapFunction, RouteFunction,
    };
    use pegasus::api::{Count, Fold, FoldByKey, KeyBy, Map, Sink, Source};
    use pegasus::result::{ResultSink, ResultStream};
    use pegasus::stream::Stream;
    use pegasus::{run_opt, BuildJobError, Configuration, JobConf, StartupError};
    use pegasus_common::collections::{Collection, Set};
    use pegasus_server::pb as server_pb;
    use pegasus_server::pb::AccumKind;
    use pegasus_server::service::{JobParser, Service};
    use pegasus_server::{JobRequest, JobResponse};
    use prost::Message;
    use std::error::Error;
    use std::path::{Path, PathBuf};
    use std::sync::Once;
    use std::thread::sleep;

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
        // to test property optimization, with the saved property_names and unsaved property_names
        expected_properties: Option<(Vec<PropKey>, Vec<PropKey>)>,
        // to test remove tag optimization, with the expected history path length
        expected_path_len: Option<usize>,
        // to test the result of select step
        expected_tag_props: Option<Vec<Vec<(Tag, Vec<(PropKey, Object)>)>>>,
        // to test early stop, with the expected value of number of results
        expected_result_num: Option<usize>,
    }

    impl TestJobFactory {
        pub fn new() -> Self {
            TestJobFactory {
                inner: GremlinJobCompiler::new(Partition { num_servers: 1 }, 1, 0),
                expected_ids: None,
                expected_values: None,
                expected_path_result: None,
                expected_group_result: None,
                requirement: Requirement::OBJECT,
                is_ordered: false,
                expected_properties: None,
                expected_path_len: None,
                expected_tag_props: None,
                expected_result_num: None,
            }
        }

        pub fn with_expect_ids(expected_ids: Vec<ID>) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_ids = Some(expected_ids);
            factory
        }

        pub fn with_expect_values(expected_values: Vec<Object>) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_values = Some(expected_values);
            factory
        }

        pub fn with_expect_path_result(expected_path_result: Vec<Vec<ID>>) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_path_result = Some(expected_path_result);
            factory
        }

        pub fn with_expect_map_result(expected_map_result: Vec<(ID, Vec<ID>)>) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_group_result = Some(expected_map_result);
            factory
        }

        pub fn with_expect_property_opt(expected_props: (Vec<PropKey>, Vec<PropKey>)) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_properties = Some(expected_props);
            factory
        }

        pub fn with_expect_path_len(expected_path_len: usize) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_path_len = Some(expected_path_len);
            factory
        }

        pub fn with_expect_get_properties(
            expected_tag_props: Vec<Vec<(Tag, Vec<(PropKey, Object)>)>>,
        ) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_tag_props = Some(expected_tag_props);
            factory
        }

        pub fn with_expect_result_num(expected_result_num: usize) -> Self {
            let mut factory = TestJobFactory::new();
            factory.expected_result_num = Some(expected_result_num);
            factory
        }

        pub fn set_ordered(&mut self, ordered: bool) {
            self.is_ordered = ordered;
        }

        pub fn set_requirement(&mut self, requirement: Requirement) {
            self.requirement = requirement;
        }
    }

    impl JobParser<Traverser, Traverser> for TestJobFactory {
        fn parse(
            &self, plan: &JobRequest, input: &mut Source<Traverser>, output: ResultSink<Traverser>,
        ) -> Result<(), BuildJobError> {
            if let Some(source) = plan.source.as_ref() {
                let source = input.input_from(self.gen_source(source.resource.as_ref())?)?;
                let stream = if let Some(task) = plan.plan.as_ref() {
                    self.inner.install(source, &task.plan)?
                } else {
                    source
                };
                match plan.sink.as_ref().unwrap().sinker.as_ref() {
                    // TODO: more sink process here
                    Some(server_pb::sink::Sinker::Fold(fold)) => {
                        let accum_kind: server_pb::AccumKind =
                            unsafe { std::mem::transmute(fold.accum) };
                        match accum_kind {
                            AccumKind::Cnt => stream
                                .count()?
                                .into_stream()?
                                .map(|v| Ok(Traverser::Object(v.into())))?
                                .sink_into(output),
                            _ => todo!(),
                        }
                    }
                    _ => stream.sink_into(output),
                }
            } else {
                Err("source of job not found".into())
            }
        }
    }

    impl TestJobFactory {
        fn gen_source(
            &self, src: &[u8],
        ) -> Result<Box<dyn Iterator<Item = Traverser> + Send>, BuildJobError> {
            let mut step = GremlinStepPb::decode(&src[0..])
                .map_err(|e| format!("protobuf decode failure: {}", e))?;
            let worker_id = pegasus::get_current_worker();
            let job_workers = worker_id.local_peers as usize;
            let mut step = graph_step_from(
                &mut step,
                job_workers,
                worker_id.index,
                self.inner.get_partitioner(),
            )?;
            step.set_requirement(self.requirement);
            Ok(step.gen_source(worker_id.index as usize))
        }

        fn check_result(&self, result: Vec<Traverser>) {
            println!("result to check {:?}", result);
            if self.expected_result_num.is_some() {
                assert_eq!(self.expected_result_num.unwrap(), result.len());
            }
            let mut id_result = vec![];
            let mut obj_result = vec![];
            let mut path_result = vec![];
            let mut map_result = vec![];
            let mut tag_result = vec![];
            for traverser in result.iter() {
                if let Some(element) = traverser.get_element() {
                    if let Some(property_opt) = self.expected_properties.clone() {
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
                        match element.get() {
                            VertexOrEdge::V(v) => {
                                id_result.push(v.id() as ID);
                            }
                            VertexOrEdge::E(e) => {
                                let g_src = e.src_id;
                                let g_dst = e.dst_id;
                                let eid = ((g_dst as ID) << 64) | (g_src as ID);
                                id_result.push(eid as ID);
                            }
                        }
                    }
                } else if let Some(o) = traverser.get_object() {
                    match o {
                        Object::Primitive(_) | Object::String(_) | Object::Blob(_) => {
                            obj_result.push(o.clone());
                        }
                        Object::DynOwned(x) => {
                            if let Some(p) = x.try_downcast_ref::<ResultPath>() {
                                let mut path = vec![];
                                for item in p.iter() {
                                    path.push(item.as_element().expect("element").id());
                                }
                                path_result.push(path);
                            } else if let Some(result_prop) = x.try_downcast_ref::<ResultProperty>()
                            {
                                let mut tag_entries = vec![];
                                for (tag, one_tag_value) in result_prop.tag_entries.iter() {
                                    let tag_entry: Vec<(PropKey, Object)> = if let Some(element) =
                                        one_tag_value.graph_element.as_ref()
                                    {
                                        vec![("".into(), element.id().into())]
                                    } else if let Some(value) = one_tag_value.value.as_ref() {
                                        vec![("".into(), value.clone())]
                                    } else {
                                        let value_map = one_tag_value
                                            .properties
                                            .as_ref()
                                            .expect("value_map does not exists");
                                        let mut props = vec![];
                                        for (prop_name, prop_val) in value_map {
                                            props.push((prop_name.clone(), prop_val.clone()));
                                        }
                                        props
                                    };
                                    tag_entries.push((tag.clone(), tag_entry));
                                }
                                tag_result.push(tag_entries);
                            } else if let Some(result_pair) = try_downcast_pair(o) {
                                let group_key =
                                    if let Some(graph_element) = result_pair.0.get_element() {
                                        graph_element.id()
                                    } else if let Some(obj) = result_pair.0.get_object() {
                                        obj.as_u128().expect("cannot cast to u128") as ID
                                    } else {
                                        unreachable!()
                                    };
                                let group_value =
                                    result_pair.1.get_object().expect("we assume value is object");
                                if let Ok(count) = group_value.as_u64() {
                                    map_result.push((group_key, vec![count as ID]));
                                } else if let Some(list) = try_downcast_list(group_value) {
                                    let value_list: Vec<ID> = list
                                        .into_iter()
                                        .map(|tra| tra.get_element().expect("assume element").id())
                                        .collect();
                                    map_result.push((group_key, value_list));
                                }
                            }
                        }
                    }
                }
            }
            if !self.is_ordered {
                id_result.sort();
            }
            if self.expected_ids.is_some() {
                assert_eq!(self.expected_ids.as_ref().unwrap(), &id_result);
            } else if self.expected_path_result.is_some() {
                assert_eq!(self.expected_path_result.as_ref().unwrap(), &path_result);
            } else if self.expected_group_result.is_some() {
                assert_eq!(self.expected_group_result.as_ref().unwrap(), &map_result);
            } else if self.expected_tag_props.is_some() {
                assert_eq!(self.expected_tag_props.as_ref().unwrap(), &tag_result);
            } else if self.expected_values.is_some() {
                assert_eq!(self.expected_values.as_ref().unwrap(), &obj_result);
            } else {
                println!("no expected values specified in test");
            }
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
            let eid = ((g_dst as ID) << 64) | (g_src as ID);
            global_ids.push(eid);
        }
        global_ids
    }

    pub fn gen_path(file: &str) -> PathBuf {
        Path::new(TEST_PLAN_PATH).join(file.to_string())
    }

    fn submit_query(factory: &TestJobFactory, job_req: JobRequest, num_workers: u32) {
        let job_config = job_req.conf.clone().expect("no job_conf");
        let conf = JobConf::with_id(job_config.job_id, job_config.job_name, num_workers);
        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = ResultSink::new(tx);
        let cancel_hook = sink.get_cancel_hook().clone();
        let mut results = ResultStream::new(conf.job_id, cancel_hook, rx);
        run_opt(conf, sink, |worker| {
            worker.dataflow(|input, output| factory.parse(&job_req, input, output))
        })
        .expect("submit job failure;");

        let mut trav_results = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    trav_results.push(res);
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        factory.check_result(trav_results);
    }

    pub fn run_test_with_worker_num(
        factory: TestJobFactory, job_request: JobRequest, num_workers: u32,
    ) {
        submit_query(&factory, job_request, num_workers);
    }

    pub fn run_test(factory: TestJobFactory, job_request: JobRequest) {
        submit_query(&factory, job_request, 1);
    }
}
