extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;
extern crate core;

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use bmcsr::graph_db::GraphDB;

use rpc_server::queries;
use rpc_server::queries::register::{PrecomputeVertexApi, QueryApi, QueryRegister};
use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::Item;
use graph_index::GraphIndex;
use bmcsr::types::LabelId;
use pegasus::api::*;
use pegasus::{Configuration, JobConf, ServerConf};
use serde::{Deserialize, Serialize};
use serde_yaml::{self};
use structopt::StructOpt;

use crate::queries::rpc::RPCServerConfig;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data")]
    graph_data: PathBuf,
    #[structopt(short = "s", long = "servers_config")]
    servers_config: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PrecomputeLabel {
    src_label: Option<u32>,
    dst_label: Option<u32>,
    edge_label: Option<u32>,
    vertex_label: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PrecomputeProperty {
    name: String,
    data_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PrecomputeSetting {
    precompute_name: String,
    precompute_type: String,
    label: PrecomputeLabel,
    properties: Vec<PrecomputeProperty>,
    path: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesSetting {
    queries_name: String,
    path: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesConfig {
    precompute: Vec<PrecomputeSetting>,
    read_queries: Vec<QueriesSetting>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PegasusConfig {
    pub worker_num: Option<u32>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub network_config: Option<Configuration>,
    pub rpc_server: Option<RPCServerConfig>,
    pub pegasus_config: Option<PegasusConfig>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let graph_data_str = config.graph_data.to_str().unwrap();

    let shared_graph = Arc::new(RwLock::new(GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap()));
    let shared_graph_index = Arc::new(RwLock::new(GraphIndex::new(0)));

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");

    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str())?;
    let server_conf = if let Some( servers) = servers_conf.network_config {
        servers
    } else {
        Configuration::singleton()
    };

    let workers = servers_conf.pegasus_config.expect("Could not read pegasus config").worker_num.expect("Could not read worker num");

    let mut servers = vec![];
    if let Some(network) = &server_conf.network {
        for i in 0..network.servers_size {
            servers.push(i as u64);
        }
    }

    let mut query_register = QueryRegister::new();
    println!("Start load lib");
    query_register.load(&PathBuf::from(config.queries_config));
    println!("Finished load libs");

    let rpc_config = servers_conf.rpc_server.expect("Rpc config not set");
    pegasus::startup(server_conf.clone()).ok();
    pegasus::wait_servers_ready(&ServerConf::All);
    queries::rpc::start_all(rpc_config, server_conf, query_register, workers, &servers, shared_graph, shared_graph_index).await?;

    Ok(())
}