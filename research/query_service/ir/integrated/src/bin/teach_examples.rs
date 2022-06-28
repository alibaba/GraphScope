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

use core::time;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::slice::SliceIndex;
use std::sync::Arc;
use std::time::Instant;
use std::vec;

use graph_store::config::GraphDBConfig;
use graph_store::graph_db::Direction;
use graph_store::graph_db::GlobalStoreTrait;
use graph_store::graph_db_impl::LargeGraphDB;
use graph_proxy::create_exp_store;
use graph_proxy::a
use pegasus::api::{Count, Dedup, Filter, Fold, Join, KeyBy, Map, PartitionByKey, Sink};
use pegasus::errors::{BuildJobError, JobSubmitError, SpawnJobError, StartupError};
use pegasus::result::ResultStream;
use pegasus::{flat_map, Configuration, JobConf, ServerConf};
use structopt::StructOpt;
use strum_macros::ToString;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "w", long = "workers", default_value = "1")]
    workers: u32,
    #[structopt(short = "b", long = "benchmark", default_value = "t")]
    benchmark_type: String,
}

fn main() {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };
    pegasus::startup(server_conf).unwrap();
    let mut conf = JobConf::new("example");
    conf.set_workers(config.workers);

    if config.servers.is_some() {
        conf.reset_servers(ServerConf::All);
    }

    pegasus::wait_servers_ready(conf.servers());

    create_exp_store();

    let mut result = _teach_example1(conf).expect("Run Job Error!");

    while let Some(Ok(data)) = result.next() {
        println!("{:?}", data);
    }

    pegasus::shutdown_all();
}

fn _teach_example1(conf: JobConf) -> Result<ResultStream<u64>, JobSubmitError> {
    pegasus::run(conf, move || {
        move |input, output| {
            input
                .input_from(
                    GRAPH
                        .get_all_vertices(None)
                        .map(|v| (v.get_id() as u64)),
                )?
                .sink_into(output)
        }
    })
}
