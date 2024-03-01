extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;
extern crate core;

use std::arch::aarch64::vqmovuns_s32;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

use rpc_server::queries;
use rpc_server::queries::graph;
use rpc_server::queries::register::{PrecomputeApi, QueryApi, QueryRegister};
use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::{ArrayData, DataType as IndexDataType, Item};

use pegasus::{Configuration, JobConf, ServerConf};
use serde::{Deserialize, Serialize};
use rpc_server::queries::rpc::RPCServerConfig;

use std::fs::File;
use std::io::BufRead;
use bmcsr::graph_db::GraphDB;

use bmcsr::types::LabelId;
use graph_index::GraphIndex;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data")]
    graph_data: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "p", long = "parameters", default_value = "")]
    parameters: PathBuf,
    #[structopt(short = "r", long = "graph_raw_data", default_value = "")]
    graph_raw: PathBuf,
    #[structopt(short = "b", long = "batch_update_configs", default_value = "")]
    batch_update_configs: PathBuf,
    #[structopt(short = "c", long = "output_dir", default_value = "")]
    output_dir: PathBuf,
    #[structopt(short = "w", long = "worker_num", default_value = 8)]
    worker_num: u32,
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

fn register_precompute(settings: &Vec<PrecomputeSetting>) -> Vec<Container<crate::PrecomputeApi>> {
    let mut ret = vec![];
    for precompute in settings.iter() {
        let lib_path = precompute.path.clone();
        let libc: Container<crate::PrecomputeApi> = unsafe { Container::load(lib_path) }.unwrap();
        ret.push(libc);
    }
    ret
}
fn precompute(settings: &Vec<PrecomputeSetting>, libs: &Vec<Container<PrecomputeApi>>, worker_num: u32) {
    let num = settings.len();
    for i in 0..num {
        let index_str = i;
        let index_str = index_str.to_string();
        let precompute = &settings[i];
        let libc = &libs[i];
        let start = Instant::now();

        let mut conf = JobConf::new(precompute.precompute_name.clone().to_string() + "-" + &*index_str.clone());
        conf.set_workers(worker_num);
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
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();

    let worker_num = config.worker_num;

    let graph_data_str = config.graph_data.to_str().unwrap();

    let mut graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let graph_index = GraphIndex::new(0);

    let queries_config_path = config.queries_config;
    let file = File::open(queries_config_path).expect("Failed to open precompute config file");
    let precompute_config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values.");
    let precompute_libs = register_precompute(&precompute_config.precompute);

    precompute(&precompute_config.precompute, &precompute_libs, worker_num);

    let queries_config_path = config.queries_config;
    let file = File::open(queries_config_path).expect("Failed to open precompute config file");
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

    println!("Finished load libs");
    println!("Start iterating parameter files: {:?}", config.parameters);

    if config.parameters.is_dir() {
        let start = Instant::now();
        for entry in fs::read_dir(config.parameters)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let file = File::open(path).expect("Failed to open query parameter file");
                let reader = std::io::BufReader::new(file);
                for line_result in reader.lines() {
                    let line = line_result?;
                    let parts: Vec<String> = line.split('|').map(|s| s.to_string()).collect();
                    let query_name = parts[0].clone();
                    let query_params = parts[1..].to_vec();
                    let query = query_register.get_query(&query_name).expect("Could not find query");
                    let mut conf = JobConf::new(query_name.clone());
                    conf.set_workers(workers);
                    conf.reset_servers(ServerConf::Partial(servers.clone()));
                    let result = {
                        pegasus::run(conf.clone(), || {
                            query.Query(
                                conf.clone(),
                                &queries::graph::CSR,
                                &queries::graph::GRAPH_INDEX,
                                HashMap::new(),
                            )
                        }).expect("submit query failure")
                    };
                    for x in result {
                        let data_set = x.expect("Fail to get result");
                    }
                }
            }
        }
        println!(
            "Finished run queries, time: {} ms", start.elapsed().as_millis()
        );
    } else if config.parameters.is_file() {
        println!("{:?} is expected to be a directory", config.parameters);
    }

    Ok(())
}