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

extern crate clap;

use gremlin_core::compiler::GremlinJobCompiler;
use gremlin_core::{create_demo_graph, register_gremlin_types, Partitioner};
use log::info;
use pegasus::Configuration;
use pegasus_server::config::combine_config;
use pegasus_server::rpc::start_debug_rpc_server;
use pegasus_server::service::Service;
use pegasus_server::{CommonConfig, HostsConfig};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt)]
#[structopt(about = "An RPC server for accepting queries from Gremlin server.")]
pub struct ServerConfig {
    #[structopt(
        long = "port",
        short = "p",
        default_value = "1234",
        help = "the port to accept RPC connections"
    )]
    pub rpc_port: u16,
    #[structopt(
        long = "index",
        short = "i",
        default_value = "0",
        help = "the current server id among all servers"
    )]
    pub server_id: u64,
    #[structopt(
        long = "hosts",
        short = "h",
        default_value = "",
        help = "the path of hosts file for pegasus communication"
    )]
    pub hosts: String,
    #[structopt(
        long = "config",
        short = "c",
        default_value = "",
        help = "the path of config file for pegasus"
    )]
    pub config: String,
    #[structopt(long = "report", help = "the option to report the job latency and memory usage")]
    pub report: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let server_config: ServerConfig = ServerConfig::from_args();
    let host_config = if server_config.hosts.is_empty() {
        None
    } else {
        Some(HostsConfig::read_from(server_config.hosts)?)
    };
    let common_config = if server_config.config.is_empty() {
        None
    } else {
        Some(CommonConfig::read_from(server_config.config)?)
    };
    let num_servers = if let Some(h) = &host_config { h.peers.len() } else { 1 };
    let config = combine_config(server_config.server_id, host_config, common_config);
    let addr = format!("{}:{}", "0.0.0.0", server_config.rpc_port);

    create_demo_graph();
    register_gremlin_types().expect("register gremlin types failed");

    if let Some(engine_config) = config {
        pegasus::startup(engine_config).unwrap();
    } else {
        pegasus::startup(Configuration::singleton()).unwrap();
    }

    info!("try to start rpc server;");
    let partition = Partition { num_servers: num_servers.clone() };
    let factory = GremlinJobCompiler::new(partition, num_servers as u32, server_config.server_id);
    let service = Service::new(factory);
    start_debug_rpc_server(addr.parse().unwrap(), service, server_config.report).await?;

    Ok(())
}

pub struct Partition {
    pub num_servers: usize,
}

impl Partitioner for Partition {
    fn get_partition(&self, id: &u128, job_workers: u32) -> u64 {
        let workers = job_workers as usize;
        let id_usize = *id as usize;
        (id_usize % self.num_servers * workers + (_hash(id_usize)) % workers) as u64
    }
}

fn _hash(key: usize) -> usize {
    fasthash::metro::hash64_with_seed(&key.to_le_bytes(), 2654435761) as usize
}
