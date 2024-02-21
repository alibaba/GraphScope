extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;
extern crate core;

use std::any::TypeId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, Read};
use std::ops::Add;
use std::path::PathBuf;
use std::time::Instant;

use codegen_benchmark::queries;
use codegen_benchmark::queries::graph;
use codegen_benchmark::queries::register::{PrecomputeApi, QueryApi, QueryRegister};
use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::{ArrayData, DataType as IndexDataType, Item};
use graph_index::GraphIndex;
use itertools::Itertools;
use mcsr::graph_db::GlobalCsrTrait;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use mcsr::types::LabelId;
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;
use pegasus_network::Server;
use serde::{Deserialize, Serialize};
use serde_yaml::{self};
use structopt::StructOpt;

use crate::queries::rpc::RPCServerConfig;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
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

    lazy_static::initialize(&queries::graph::CSR);
    lazy_static::initialize(&queries::graph::GRAPH_INDEX);

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");

    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str())?;
    let mut server_conf = if let Some( servers) = servers_conf.network_config {
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
        if precompute.precompute_type == "vertex" {
            let property_size = graph::CSR.get_vertices_num(label);
            for i in 0..properties_info.len() {
                graph::GRAPH_INDEX.init_vertex_index(
                    properties_info[i].0.clone(),
                    label,
                    properties_info[i].1.clone(),
                    Some(property_size),
                    Some(Item::Int32(0)),
                );
            }
        } else {
            let property_size = graph::CSR.get_edges_num(src_label.unwrap(), label, dst_label.unwrap());
            for i in 0..properties_info.len() {
                graph::GRAPH_INDEX.init_edge_index(
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
        let result = {
            pegasus::run(conf.clone(), || {
                libc.Precompute(
                    conf.clone(),
                    &queries::graph::CSR,
                    &queries::graph::GRAPH_INDEX,
                    &properties_info,
                    true,
                    label,
                    src_label,
                    dst_label,
                )
            })
            .expect("submit precompute failure")
        };
        for x in result {
            let (index_set, data_set) = x.expect("Fail to get result");
            if precompute.precompute_type == "edge" {
                for i in 0..properties_size {
                    queries::graph::GRAPH_INDEX.add_edge_index_batch(
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
                    queries::graph::GRAPH_INDEX.add_vertex_index_batch(
                        label,
                        &properties_info[i].0,
                        &index_set,
                        data_set[i].as_ref(),
                    )?;
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
    queries::rpc::start_all(rpc_config, server_conf, query_register, workers, &servers).await?;

    Ok(())
}
