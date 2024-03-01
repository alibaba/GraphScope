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
use rpc_server::queries::register::{PrecomputeApi, QueryApi, QueryRegister};
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

    let mut servers = vec![];
    if let Some(network) = &server_conf.network {
        for i in 0..network.servers_size {
            servers.push(i as u64);
        }
    }

    let queries_config_path = config.queries_config;
    let file = File::open(queries_config_path).expect("Failed to open precompute config file");
    let precompute_config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values.");
    let mut index = 0;
    let workers = servers_conf.pegasus_config.expect("Could not read pegasus config").worker_num.expect("Could not read worker num");
    for precompute in precompute_config.precompute {
        let lib_path = precompute.path;
        let libc: Container<crate::PrecomputeApi> =
            unsafe { Container::load(lib_path.clone()) }.expect("Could not open library or load symbols");
        let start = Instant::now();
        println!("Start run query {}", &precompute.precompute_name);
        let mut conf =
            JobConf::new(precompute.precompute_name.clone().to_owned() + "-" + &index.to_string());
        conf.set_workers(workers);
        conf.reset_servers(ServerConf::Partial(servers.clone()));
        let label = precompute.label.edge_label.unwrap() as LabelId;
        let src_label = Some(precompute.label.src_label.unwrap() as LabelId);
        let dst_label = Some(precompute.label.dst_label.unwrap() as LabelId);
        let mut properties_info = vec![];
        let properties_size = precompute.properties.len();
        for i in 0..properties_size {
            let index_name = precompute.properties[i].name.clone();
            let data_type = graph_index::types::str_to_data_type(&precompute.properties[i].data_type);
            properties_info.push((index_name, data_type));
        }
        {
            let graph = shared_graph.read().unwrap();
            let mut graph_index = shared_graph_index.write().unwrap();
            if precompute.precompute_type == "vertex" {
                let property_size = graph.get_vertices_num(label);
                for i in 0..properties_info.len() {
                    graph_index.init_vertex_index(
                        properties_info[i].0.clone(),
                        label,
                        properties_info[i].1.clone(),
                        Some(property_size),
                        Some(Item::Int32(0)),
                    );
                }
            } else {
                let property_size = graph.get_edges_num(src_label.unwrap(), label, dst_label.unwrap());
                for i in 0..properties_info.len() {
                    graph_index.init_edge_index(
                        properties_info[i].0.clone(),
                        src_label.unwrap(),
                        dst_label.unwrap(),
                        label,
                        properties_info[i].1.clone(),
                        Some(property_size),
                        Some(Item::Int32(0)),
                    );
                }
            }
        }
        let result = {
            pegasus::run(conf.clone(), || {
                let graph = shared_graph.read().unwrap();
                let graph_index = shared_graph_index.write().unwrap();
                libc.Precompute(
                    conf.clone(),
                    &graph,
                    &graph_index,
                    &properties_info,
                    true,
                    label,
                    src_label,
                    dst_label,
                )
            })
            .expect("submit precompute failure")
        };
        let mut result_vec = vec![];
        for x in result {
            result_vec.push(x.unwrap());
        }
        {
            let graph = shared_graph.read().unwrap();
            let mut graph_index = shared_graph_index.write().unwrap();
            for (index_set, data_set) in result_vec {
                if precompute.precompute_type == "edge" {
                    for i in 0..properties_size {
                        let graph_index = shared_graph_index.write().unwrap();
                        graph_index.add_edge_index_batch(
                            src_label.unwrap(),
                            label,
                            dst_label.unwrap(),
                            &properties_info[i].0,
                            &index_set,
                            data_set[i].as_ref(),
                        )?;
                    }
                } else if precompute.precompute_type == "vertex" {
                    for i in 0..properties_size {
                        let graph_index = shared_graph_index.write().unwrap();
                        graph_index.add_vertex_index_batch(
                            label,
                            &properties_info[i].0,
                            &index_set,
                            data_set[i].as_ref(),
                        )?;
                    }
                }
            }
        }
        println!(
            "Finished run query {}, time: {}",
            &precompute.precompute_name,
            start.elapsed().as_millis()
        );
        index += 1;
    }

    println!("Start load lib");
    let mut query_register = QueryRegister::new();
    for queries in precompute_config.read_queries {
        let query_name = queries.queries_name;
        let lib_path = queries.path;
        println!("Start load query {}", query_name);
        let libc: Container<crate::QueryApi> =
            unsafe { Container::load(lib_path.clone()) }.expect("Could not open library or load symbols");
        query_register.register(query_name, libc);
    }

    let rpc_config = servers_conf.rpc_server.expect("Rpc config not set");
    pegasus::startup(server_conf.clone()).ok();
    pegasus::wait_servers_ready(&ServerConf::All);
    queries::rpc::start_all(rpc_config, server_conf, query_register, workers, &servers, shared_graph, shared_graph_index).await?;

    Ok(())
}
