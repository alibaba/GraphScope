use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
use bmcsr::schema::InputSchema;
use bmcsr::traverse::traverse;
use graph_index::GraphIndex;
use lazy_static::lazy_static;
use log::info;
#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
use pegasus::{Configuration, JobConf, ServerConf};
use rpc_server::queries::register::QueryRegister;
use rpc_server::queries::rpc::RPCServerConfig;
use serde::Deserialize;
use structopt::StructOpt;

#[cfg(feature = "use_mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "use_mimalloc_rust")]
use mimalloc_rust::*;

#[cfg(feature = "use_mimalloc_rust")]
#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Debug, Clone, StructOpt, Default)]
pub struct Config {
    #[structopt(short = "g", long = "graph_data")]
    graph_data: PathBuf,
    #[structopt(short = "q", long = "queries_config", default_value = "")]
    queries_config: String,
    #[structopt(short = "p", long = "parameters", default_value = "")]
    parameters: PathBuf,
    #[structopt(short = "o", long = "output_dir", default_value = "")]
    output_dir: PathBuf,
    #[structopt(short = "w", long = "worker_num", default_value = "8")]
    worker_num: u32,
    #[structopt(short = "s", long = "batch_size", default_value = "1024")]
    batch_size: u32,
    #[structopt(short = "c", long = "batch_capacity", default_value = "64")]
    batch_capacity: u32,
    #[structopt(short = "t", long = "target_query", default_value = "")]
    target_query: String,
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

lazy_static! {
    static ref PARAMETERS_LIST: Vec<(&'static str, Vec<&'static str>)> = {
        let mut m = Vec::new();
        m.push(("bi1", vec!["bi-1.csv"]));
        m.push(("bi2", vec!["bi-2a.csv", "bi-2b.csv"]));
        m.push(("bi3", vec!["bi-3.csv"]));
        m.push(("bi4", vec!["bi-4.csv"]));
        m.push(("bi5", vec!["bi-5.csv"]));
        m.push(("bi6", vec!["bi-6.csv"]));
        m.push(("bi7", vec!["bi-7.csv"]));
        m.push(("bi8", vec!["bi-8a.csv", "bi-8b.csv"]));
        m.push(("bi9", vec!["bi-9.csv"]));
        m.push(("bi10", vec!["bi-10a.csv", "bi-10b.csv"]));
        m.push(("bi11", vec!["bi-11.csv"]));
        m.push(("bi12", vec!["bi-12.csv"]));
        m.push(("bi13", vec!["bi-13.csv"]));
        m.push(("bi14", vec!["bi-14a.csv", "bi-14b.csv"]));
        m.push(("bi15", vec!["bi-15a.csv", "bi-15b.csv"]));
        m.push(("bi16", vec!["bi-16a.csv", "bi-16b.csv"]));
        m.push(("bi17", vec!["bi-17.csv"]));
        m.push(("bi18", vec!["bi-18.csv"]));
        m.push(("bi19", vec!["bi-19a.csv", "bi-19b.csv"]));
        m.push(("bi20", vec!["bi-20a.csv", "bi-20b.csv"]));

        m
    };
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let worker_num = config.worker_num;
    let graph_data_str = config.graph_data.to_str().unwrap();
    let output_dir = config.output_dir;
    let batch_size = config.batch_size;
    let batch_capacity = config.batch_capacity;

    let mut graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let mut graph_index = GraphIndex::new(0);

    let mut query_register = QueryRegister::new();
    if !config.queries_config.is_empty() {
        println!("Start load lib");
        query_register.load(&PathBuf::from(&config.queries_config));
        println!("Finished load libs");
    }

    if !config.queries_config.is_empty() {
        println!("before run precomputes...");
        if config.target_query == "" {
            query_register.run_precomputes(&graph, &mut graph_index, worker_num);
        } else if config.target_query == "bi4" {
            query_register.run_precompute(
                &graph,
                &mut graph_index,
                worker_num,
                &"bi4_precompute".to_string(),
            );
        } else if config.target_query == "bi6" {
            query_register.run_precompute(
                &graph,
                &mut graph_index,
                worker_num,
                &"bi6_precompute".to_string(),
            );
        } else if config.target_query == "bi14" {
            query_register.run_precompute(
                &graph,
                &mut graph_index,
                worker_num,
                &"bi14_precompute".to_string(),
            );
        } else if config.target_query == "bi19" {
            query_register.run_precompute(
                &graph,
                &mut graph_index,
                worker_num,
                &"bi14_precompute".to_string(),
            );
        } else if config.target_query == "bi20" {
            query_register.run_precompute(
                &graph,
                &mut graph_index,
                worker_num,
                &"bi20_precompute".to_string(),
            );
        }
        println!("after run precomputes...");
    }

    if config.parameters.is_dir() {
        println!("Start iterating parameter files: {:?}", config.parameters);
        let start = Instant::now();
        for pair in PARAMETERS_LIST.iter() {
            let query_name = pair.0.to_string();
            if config.target_query != "" && query_name != config.target_query {
                continue;
            }
            let query = query_register
                .get_query(&query_name)
                .expect("Could not find query");
            let files = pair.1.clone();

            for filename in files.iter() {
                let path = config.parameters.clone().join(filename);
                if !path.is_file() {
                    continue;
                }
                println!("start query: {}, file: {:?}", query_name, path);
                let file = File::open(path.clone()).expect("Failed to open query parameter file");
                let reader = BufReader::new(file);
                let mut keys = Vec::<String>::new();
                let mut first_line = true;
                let file_start = Instant::now();
                let mut query_idx = 0;
                for line_result in reader.lines() {
                    let line = line_result.unwrap();
                    if first_line {
                        first_line = false;
                        keys = line
                            .split('|')
                            .map(|s| {
                                s.to_string()
                                    .split(':')
                                    .next()
                                    .unwrap()
                                    .to_string()
                            })
                            .collect();
                        continue;
                    }

                    let params: Vec<String> = line.split('|').map(|s| s.to_string()).collect();
                    let mut params_map = HashMap::new();
                    for (index, key) in keys.iter().enumerate() {
                        params_map.insert(key.clone(), params[index].clone());
                    }
                    let mut conf = JobConf::new(query_name.clone());
                    conf.set_workers(worker_num);
                    conf.reset_servers(ServerConf::Partial(vec![0]));
                    conf.batch_capacity = batch_capacity;
                    conf.batch_size = batch_size;
                    let query_start = Instant::now();
                    let result = {
                        pegasus::run(conf.clone(), || {
                            query.Query(conf.clone(), &graph, &graph_index, params_map.clone())
                        })
                        .expect("submit query failure")
                    };
                    for x in result {
                        let data_set = x.expect("Fail to get result");
                    }
                    println!(
                        "Finished query: {}, time: {} ms",
                        query_idx,
                        query_start.elapsed().as_micros() as f64 / 1e3
                    );
                    query_idx += 1;
                }

                println!(
                    "finished {} queries of file: {:?}, time: {} s",
                    query_idx,
                    path,
                    file_start.elapsed().as_micros() as f64 / 1e6,
                );
            }
        }
        println!("Finished run queries, time: {} s", start.elapsed().as_micros() as f64 / 1e6);
    } else if config.parameters.is_file() {
        println!("{:?} is expected to be a directory", config.parameters);
    }

    println!("before shutdown pegasus...");
    pegasus::shutdown_all();
    println!("after shutdown pegasus...");

    Ok(())
}
