use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use bmcsr::graph_db::GraphDB;
use bmcsr::traverse::traverse;
use dlopen::wrapper::{Container, WrapperApi};
use dlopen_derive::WrapperApi;
use graph_index::types::WriteType;
use graph_index::GraphIndex;
#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
use pegasus::api::Source;
use pegasus::resource::{DistributedParResourceMaps, KeyedResources, ResourceMap};
use pegasus::result::ResultSink;
use pegasus::BuildJobError;
use pegasus::JobConf;
use pegasus::{Configuration, ServerConf};
use rpc_server::queries;
use rpc_server::queries::rpc::RPCServerConfig;
use rpc_server::queries::{register, write_graph};
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[cfg(feature = "use_mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "use_mimalloc_rust")]
use mimalloc_rust::*;
use rpc_server::queries::register::QueryApi;

#[cfg(feature = "use_mimalloc_rust")]
#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data")]
    graph_data: PathBuf,
    #[structopt(short = "s", long = "servers_config")]
    servers_config: PathBuf,
    #[structopt(short = "l", long = "lib_path")]
    lib_path: String,
    #[structopt(short = "q", long = "query")]
    query_path: String,
    #[structopt(short = "o", long = "output_dir")]
    output_dir: PathBuf,
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
pub struct Param {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueryConfig {
    pub name: String,
    pub description: String,
    pub mode: String,
    pub extension: String,
    pub library: String,
    pub params: Option<Vec<Param>>,
    pub returns: Option<Vec<Param>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesConfig {
    pub queries: Option<Vec<QueryConfig>>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let graph_data_str = config.graph_data.to_str().unwrap();
    let shared_graph =
        Arc::new(RwLock::new(GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap()));
    let shared_graph_index = Arc::new(RwLock::new(GraphIndex::new(0)));

    let servers_config =
        std::fs::read_to_string(config.servers_config).expect("Failed to read server config");

    let servers_conf: ServerConfig = toml::from_str(servers_config.as_str())?;
    let server_conf =
        if let Some(servers) = servers_conf.network_config { servers } else { Configuration::singleton() };

    let workers = servers_conf
        .pegasus_config
        .expect("Could not read pegasus config")
        .worker_num
        .expect("Could not read worker num");

    let mut servers = vec![];
    if let Some(network) = &server_conf.network {
        for i in 0..network.servers_size {
            servers.push(i as u64);
        }
    }

    let mut register = register::QueryRegister::new();

    let lib_config_path = config.lib_path;
    let lib_config_file = File::open(lib_config_path).unwrap();

    let queries_config: QueriesConfig =
        serde_yaml::from_reader(lib_config_file).expect("Could not read values");

    if let Some(queries) = queries_config.queries {
        for query in queries {
            let query_name = query.name;
            let description = query.description;
            let lib_path = query.library;
            let mut inputs = vec![];
            if let Some(params) = query.params {
                for param in params {
                    inputs.push((param.name, param.data_type));
                }
            }
            let libc: Vec<Container<QueryApi>> = vec![unsafe { Container::load(lib_path) }.unwrap()];
            register.register_new_query(query_name, libc, inputs, description);
        }
    }

    let query_path = config.query_path;
    let mut queries = vec![];
    let file = File::open(query_path).unwrap();
    let lines = io::BufReader::new(file).lines();
    for (i, line) in lines.enumerate() {
        queries.push(line.unwrap());
    }

    let mut index = 0i32;
    for query in queries {
        let mut params = HashMap::new();
        let mut split = query.trim().split("|").collect::<Vec<&str>>();
        let query_name = split[0].to_string();
        if let Some(inputs_info) = register.get_query_inputs_info(&query_name) {
            for (index, (name, _)) in inputs_info.iter().enumerate() {
                params.insert(name.clone(), split[index + 1].to_string());
            }
        }
        let mut conf = JobConf::new(query_name.clone() + "-" + &index.to_string());
        conf.set_workers(workers);
        conf.reset_servers(ServerConf::Partial(vec![0]));

        let mut resource_map = Vec::with_capacity(workers as usize);
        let mut keyed_resource_map = Vec::with_capacity(workers as usize);
        for _ in 0..workers {
            resource_map.push(Some(Arc::new(Mutex::new(ResourceMap::default()))));
            keyed_resource_map.push(Some(Arc::new(Mutex::new(KeyedResources::default()))));
        }
        let mut resource_maps = DistributedParResourceMaps::new(&conf, resource_map, keyed_resource_map);
        if let Some(queries) = register.get_new_query(&query_name) {
            for query in queries {
                let graph = shared_graph.read().unwrap();
                let graph_index = shared_graph_index.read().unwrap();
                let results = {
                    pegasus::run_with_resource_map(conf.clone(), Some(resource_maps.clone()), || {
                        query.Query(conf.clone(), &graph, &graph_index, params.clone(), None)
                    })
                    .expect("submit query failure")
                };
                let mut write_operations = vec![];
                for result in results {
                    if let Ok((worker_id, alias_datas, write_ops, query_result)) = result {
                        if let Some(write_ops) = write_ops {
                            for write_op in write_ops {
                                write_operations.push(write_op);
                            }
                        }
                    }
                }
                drop(graph);
                drop(graph_index);
                let mut graph = shared_graph.write().unwrap();
                let mut graph_index = shared_graph_index.write().unwrap();
                for mut write_op in write_operations.drain(..) {
                    match write_op.write_type() {
                        WriteType::Insert => {
                            if let Some(vertex_mappings) = write_op.vertex_mappings() {
                                let vertex_label = vertex_mappings.vertex_label();
                                let inputs = vertex_mappings.inputs();
                                let column_mappings = vertex_mappings.column_mappings();
                                for input in inputs.iter() {
                                    write_graph::insert_vertices(
                                        &mut graph,
                                        vertex_label,
                                        input,
                                        column_mappings,
                                        8,
                                    );
                                }
                            } else if let Some(edge_mappings) = write_op.edge_mappings() {
                                let src_label = edge_mappings.src_label();
                                let edge_label = edge_mappings.edge_label();
                                let dst_label = edge_mappings.dst_label();
                                let inputs = edge_mappings.inputs();
                                let src_vertex_mappings = edge_mappings.src_column_mappings();
                                let dst_vertex_mappings = edge_mappings.dst_column_mappings();
                                let column_mappings = edge_mappings.column_mappings();
                                for input in inputs.iter() {
                                    write_graph::insert_edges(
                                        &mut graph,
                                        src_label,
                                        edge_label,
                                        dst_label,
                                        input,
                                        src_vertex_mappings,
                                        dst_vertex_mappings,
                                        column_mappings,
                                        8,
                                    );
                                }
                            }
                        }
                        WriteType::Delete => {
                            if let Some(vertex_mappings) = write_op.take_vertex_mappings() {
                                let vertex_label = vertex_mappings.vertex_label();
                                let inputs = vertex_mappings.inputs();
                                let column_mappings = vertex_mappings.column_mappings();
                                for input in inputs.iter() {
                                    write_graph::delete_vertices(
                                        &mut graph,
                                        vertex_label,
                                        input,
                                        column_mappings,
                                        8,
                                    );
                                }
                            }
                            if let Some(edge_mappings) = write_op.take_edge_mappings() {
                                let src_label = edge_mappings.src_label();
                                let edge_label = edge_mappings.edge_label();
                                let dst_label = edge_mappings.dst_label();
                                let inputs = edge_mappings.inputs();
                                let src_column_mappings = edge_mappings.src_column_mappings();
                                let dst_column_mappings = edge_mappings.dst_column_mappings();
                                let column_mappings = edge_mappings.column_mappings();
                                for input in inputs.iter() {
                                    write_graph::delete_edges(
                                        &mut graph,
                                        src_label,
                                        edge_label,
                                        dst_label,
                                        input,
                                        src_column_mappings,
                                        dst_column_mappings,
                                        column_mappings,
                                        8,
                                    );
                                }
                            }
                        }
                        _ => todo!(),
                    }
                }
            }
        }
    }
    println!("Start traverse");
    let mut graph = shared_graph.read().unwrap();
    let output_dir = config.output_dir;
    std::fs::create_dir_all(&output_dir).unwrap();
    traverse(&graph, output_dir.to_str().unwrap());
    Ok(())
}
