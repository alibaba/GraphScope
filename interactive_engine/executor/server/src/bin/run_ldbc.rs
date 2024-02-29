extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;

use std::any::TypeId;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::ops::Add;
use std::path::PathBuf;
use std::time::Instant;

use rpc_server::queries;
use rpc_server::queries::graph;
use rpc_server::queries::register::{PrecomputeApi, QueryApi};
use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::{ArrayData, DataType as IndexDataType, Item};
use graph_index::GraphIndex;
use itertools::Itertools;
use bmcsr::types::LabelId;
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::result::ResultStream;
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::ServerAddr;
use serde::{Deserialize, Serialize};
use serde_yaml::{self};
use structopt::StructOpt;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "m", long = "mode", default_value = "codegen")]
    mode: String,
    #[structopt(short = "w", long = "workers", default_value = "2")]
    workers: u32,
    #[structopt(short = "q", long = "query")]
    query_path: String,
    #[structopt(short = "p", long = "print")]
    print_result: bool,
    #[structopt(short = "s", long = "servers")]
    servers: Option<PathBuf>,
    #[structopt(short = "l", long = "dylib")]
    lib_path: String,
    #[structopt(short = "c", long = "precompute_config", default_value = "")]
    precompute_config: String,
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
pub struct PrecomputeConfig {
    precompute: Vec<PrecomputeSetting>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();

    let config: Config = Config::from_args();

    lazy_static::initialize(&queries::graph::CSR);
    lazy_static::initialize(&queries::graph::GRAPH_INDEX);

    let mut server_conf = if let Some(ref servers) = config.servers {
        let servers = std::fs::read_to_string(servers).unwrap();
        Configuration::parse(&servers).unwrap()
    } else {
        Configuration::singleton()
    };

    let mut servers = vec![];
    if let Some(network) = &server_conf.network {
        for i in 0..network.servers_size {
            servers.push(i as u64);
        }
    }
    pegasus::startup(server_conf).ok();
    pegasus::wait_servers_ready(&ServerConf::All);

    let precompute_config_path = config.precompute_config;
    if !precompute_config_path.is_empty() {
        let file = File::open(precompute_config_path).expect("Failed to open precompute config file");
        let precompute_config: PrecomputeConfig =
            serde_yaml::from_reader(file).expect("Could not read values.");
        let mut index = 0;
        for precompute in precompute_config.precompute {
            let lib_path = precompute.path;
            let libc: Container<PrecomputeApi> = unsafe { Container::load(lib_path.clone()) }
                .expect("Could not open library or load symbols");

            println!("Start run query {}", &precompute.precompute_name);
            let mut conf =
                JobConf::new(precompute.precompute_name.clone().to_owned() + "-" + &index.to_string());
            conf.set_workers(config.workers);
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
            index += 1;
        }
    }

    println!("Start load lib");
    let mut query_map = HashMap::new();
    let libs_path = config.lib_path;
    let file = File::open(libs_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for line in lines {
        let line = line.unwrap();
        let split = line.trim().split("|").collect::<Vec<&str>>();
        let query_name = split[0].clone().to_string();
        let lib_path = split[1].clone().to_string();
        println!("Start load query {}", query_name);
        let libc: Container<QueryApi> =
            unsafe { Container::load(lib_path.clone()) }.expect("Could not open library or load symbols");
        query_map.insert(query_name, libc);
    }

    let query_start = Instant::now();
    if config.mode == "handwriting" {
        let query_path = config.query_path;
        let mut queries = vec![];
        let file = File::open(query_path).unwrap();
        let lines = io::BufReader::new(file).lines();
        for line in lines {
            queries.push(line.unwrap());
        }
        let mut index = 0i32;
        for query in queries {
            let split = query.trim().split("|").collect::<Vec<&str>>();
            let query_name = split[0].clone();
            let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
            conf.set_workers(config.workers);
            conf.reset_servers(ServerConf::Partial(servers.clone()));
            match split[0] {
                _ => println!("Unknown query"),
            }
            index += 1;
        }
    } else if config.mode == "codegen" {
        let query_path = config.query_path;
        let mut queries = vec![];
        let file = File::open(query_path).unwrap();
        let lines = io::BufReader::new(file).lines();
        for line in lines {
            queries.push(line.unwrap());
        }
        let mut index = 0i32;
        for query in queries {
            let split = query.trim().split("|").collect::<Vec<&str>>();
            let query_name = split[0].to_string();
            let mut input_params = HashMap::new();
            println!("Split len is {}", split.len());
            for i in 1..split.len() {
                input_params.insert("i".to_string(), split[i].to_string());
            }
            println!("Start run query {}", query_name);
            let mut conf = JobConf::new(query_name.clone().to_owned() + "-" + &index.to_string());
            conf.set_workers(config.workers);
            conf.reset_servers(ServerConf::Partial(servers.clone()));
            if let Some(libc) = query_map.get(&query_name) {
                let func = || {
                    libc.Query(
                        conf.clone(),
                        &queries::graph::CSR,
                        &queries::graph::GRAPH_INDEX,
                        input_params.clone(),
                    )
                };
                let result = pegasus::run(conf.clone(), func).expect("submit Query0 failure");
                let mut results = vec![];
                for x in result {
                    results.push(String::from_utf8(x.unwrap()).unwrap());
                }
                println!("{:?}", results);
            }
            index += 1;
        }
    }
    pegasus::shutdown_all();
    println!("Finished query, elapsed time: {:?}", query_start.elapsed());
    Ok(())
}
