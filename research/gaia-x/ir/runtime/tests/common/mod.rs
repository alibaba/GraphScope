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
//!
//!

#[cfg(test)]
#[allow(dead_code)]
#[allow(unused_imports)]
pub mod test {
    use ir_common::generated::result::Result;
    use lazy_static::lazy_static;
    use pegasus::result::{ResultSink, ResultStream};
    use pegasus::{run_opt, Configuration, JobConf, StartupError};
    use pegasus_server::service::JobParser;
    use pegasus_server::JobRequest;
    use runtime::expr::to_suffix_expr_pb;
    use runtime::expr::token::tokenize;
    use runtime::{create_demo_graph, IRJobCompiler, SinglePartition};
    use std::sync::Once;

    static INIT: Once = Once::new();

    lazy_static! {
        static ref FACTORY: IRJobCompiler = initialize_job_compiler();
    }

    pub fn initialize() {
        INIT.call_once(|| {
            start_pegasus();
        });
    }

    pub fn start_pegasus() {
        match pegasus::startup(Configuration::singleton()) {
            Ok(_) => {
                create_demo_graph();
                lazy_static::initialize(&FACTORY);
            }
            Err(err) => match err {
                StartupError::AlreadyStarted(_) => {}
                _ => panic!("start pegasus failed"),
            },
        }
    }

    pub fn submit_query(job_req: JobRequest, num_workers: u32) -> ResultStream<Result> {
        let mut conf = JobConf::default();
        conf.workers = num_workers;
        let (tx, rx) = crossbeam_channel::unbounded();
        let sink = ResultSink::new(tx);
        let cancel_hook = sink.get_cancel_hook().clone();
        let results = ResultStream::new(conf.job_id, cancel_hook, rx);
        run_opt(conf, sink, |worker| {
            worker.dataflow(|input, output| FACTORY.parse(&job_req, input, output))
        })
        .expect("submit job failure;");

        results
    }

    fn initialize_job_compiler() -> IRJobCompiler {
        let partitioner = SinglePartition { num_servers: 1 };
        IRJobCompiler::new(partitioner)
    }

    use ir_common::generated::common as common_pb;
    pub fn str_to_expr(expr_str: String) -> Option<common_pb::SuffixExpr> {
        let tokens_result = tokenize(&expr_str);
        if let Ok(tokens) = tokens_result {
            if let Ok(expr) = to_suffix_expr_pb(tokens) {
                return Some(expr);
            }
        }
        None
    }
}
