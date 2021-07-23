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
#[allow(unused_imports)]
pub mod benchmark {

    lazy_static! {
        pub static ref BENCHMARK_PLAN_PATH: String = tempdir::TempDir::new("benchmark_binary_plan")
            .expect("Open temp folder error")
            .path()
            .to_str()
            .expect("temp folder path to str error")
            .to_string();
        pub static ref BENCHMARK_PARAM_PATH: String = "resource/benchmark/query_param".to_string();
    }

    use gremlin_core::compiler::GremlinJobCompiler;
    use gremlin_core::process::traversal::step::graph_step_from;
    use gremlin_core::process::traversal::traverser::{Requirement, Traverser};
    use gremlin_core::GremlinStepPb;
    pub use gremlin_core::ID;
    use gremlin_core::{create_demo_graph, DynIter, Partition, Partitioner};
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
    use std::path::Path;
    use std::process::Command;
    use std::sync::Once;

    static INIT: Once = Once::new();

    pub fn initialize() {
        INIT.call_once(|| {
            start_pegasus();
            prepare_binary_plan();
        });
    }

    fn prepare_binary_plan() {
        let prepare_plan_script = "./benches/prepare_plan.sh".to_string();
        let output = Command::new("sh")
            .arg(prepare_plan_script)
            .arg(&*BENCHMARK_PLAN_PATH)
            .output()
            .expect("sh exec error!");
        println!("prepare binary plan {:?}", output);
    }

    pub fn start_bench_service(factory: BenchJobFactory) -> Service<Traverser> {
        let service = Service::new(factory);
        service
    }

    pub struct BenchJobFactory {
        inner: GremlinJobCompiler,
        substitute_src_ids: Vec<ID>,
        requirement: Requirement,
    }

    impl BenchJobFactory {
        pub fn new(substitute_src_ids: Vec<ID>, requirement: Requirement) -> Self {
            BenchJobFactory {
                inner: GremlinJobCompiler::new(Partition { num_servers: 1 }, 1, 0),
                substitute_src_ids,
                requirement,
            }
        }
    }

    impl JobCompiler<Traverser> for BenchJobFactory {
        fn shuffle(&self, res: &[u8]) -> CompileResult<Box<dyn RouteFunction<Traverser>>> {
            self.inner.shuffle(res)
        }

        fn broadcast(&self, res: &[u8]) -> CompileResult<Box<dyn MultiRouteFunction<Traverser>>> {
            self.inner.broadcast(res)
        }

        fn source(&self, src: &[u8]) -> CompileResult<Box<dyn Iterator<Item = Traverser> + Send>> {
            let mut gremlin_step = GremlinStepPb::decode(&src[0..])
                .map_err(|e| format!("protobuf decode failure: {}", e))?;
            if let Some(worker_id) = pegasus::get_current_worker() {
                let num_workers = worker_id.peers as usize / self.inner.get_num_servers();
                let mut step = graph_step_from(&mut gremlin_step, self.inner.get_partitioner())?;
                step.set_src(self.substitute_src_ids.clone(), self.inner.get_partitioner());
                step.set_num_workers(num_workers);
                step.set_requirement(self.requirement);
                Ok(step.gen_source(worker_id.index as usize))
            } else {
                let mut step = graph_step_from(&mut gremlin_step, self.inner.get_partitioner())?;
                step.set_src(self.substitute_src_ids.clone(), self.inner.get_partitioner());
                step.set_requirement(self.requirement);
                Ok(step.gen_source(self.inner.get_server_index() as usize))
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

        fn sink(&self, res: &[u8]) -> CompileResult<Box<dyn EncodeFunction<Traverser>>> {
            self.inner.sink(res)
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

    pub fn submit_query(service: &Service<Traverser>, job_req: JobRequest) {
        let job_id = job_req.conf.clone().expect("no job_conf").job_id;
        println!("job_id: {}", job_id);
        service.accept(job_req, TestOutputStruct);
        if let Ok(mut job_guards) = service.job_guards.write() {
            let job_guard = job_guards.get_mut(&job_id).expect("get job guard failed");
            job_guard.join().expect("run query failed");
        }
    }
}
