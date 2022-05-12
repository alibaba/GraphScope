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

use std::path::PathBuf;

use graph_proxy::{InitializeJobCompiler, QueryExpGraph};
use log::info;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "EchoServer", about = "example of rpc service")]
struct Config {
    #[structopt(long = "config", parse(from_os_str))]
    config_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let (server_config, rpc_config) = pegasus_server::config::load_configs(config.config_dir).unwrap();

    let num_servers = server_config.servers_size();
    let query_exp_graph = QueryExpGraph::new(num_servers);
    let factory = query_exp_graph.initialize_job_compiler();
    info!("try to start rpc server;");

    pegasus_server::cluster::standalone::start(rpc_config, server_config, factory).await?;

    Ok(())
}
