use std::collections::HashMap;
use std::io::stdin;
use std::path::PathBuf;
use std::time::Instant;

use bmcsr::graph_db::GraphDB;
use graph_index::GraphIndex;
use lazy_static::lazy_static;
#[cfg(feature = "use_mimalloc")]
use mimalloc::MiMalloc;
use pegasus::{tag, Configuration, JobConf, ServerConf};
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
    #[structopt(short = "w", long = "worker_num", default_value = "8")]
    worker_num: u32,
    #[structopt(short = "s", long = "batch_size", default_value = "1024")]
    batch_size: u32,
    #[structopt(short = "c", long = "batch_capacity", default_value = "64")]
    batch_capacity: u32,
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let worker_num = config.worker_num;
    let graph_data_str = config.graph_data.to_str().unwrap();
    let batch_size = config.batch_size;
    let batch_capacity = config.batch_capacity;

    let graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let mut graph_index = GraphIndex::new(0);

    let mut query_register = QueryRegister::new();
    if !config.queries_config.is_empty() {
        println!("Start load lib");
        query_register.load(&PathBuf::from(&config.queries_config));
        println!("Finished load libs");
    }

    if !config.queries_config.is_empty() {
        println!("before run precomputes...");
        query_register.run_precomputes(&graph, &mut graph_index, worker_num);
        println!("after run precomputes...");
    }

    let mut line = String::new();
    stdin().read_line(&mut line).unwrap();
    let fields: Vec<String> = line
        .trim_end()
        .split('|')
        .map(|s| s.to_string())
        .collect();
    println!("fields: {:?}", fields);
    let mut params_map = HashMap::new();
    let query_name = fields[0].clone();
    if query_name == "bi9" || query_name == "bi9_person" {
        let start_date = fields[1].clone();
        let end_date = fields[2].clone();

        println!("start_date: {}, end_date: {}", start_date, end_date);

        params_map.insert("startDate".to_string(), start_date);
        params_map.insert("endDate".to_string(), end_date);
    } else if query_name == "bi14" {
        let country1 = fields[1].clone();
        let country2 = fields[2].clone();

        println!("country1: {}, country2: {}", country1, country2);

        params_map.insert("country1".to_string(), country1);
        params_map.insert("country2".to_string(), country2);
    } else if query_name == "bi15" {
        let person1Id = fields[1].clone();
        let person2Id = fields[2].clone();
        let startDate = fields[3].clone();
        let endDate = fields[4].clone();

        println!(
            "person1Id: {}, person2Id: {}, startDate: {}, endDate: {}",
            person1Id, person2Id, startDate, endDate
        );

        params_map.insert("person1Id".to_string(), person1Id);
        params_map.insert("person2Id".to_string(), person2Id);
        params_map.insert("startDate".to_string(), startDate);
        params_map.insert("endDate".to_string(), endDate);
    } else if query_name == "bi10" {
        let personId = fields[1].clone();
        let country = fields[2].clone();
        let tagClass = fields[3].clone();
        let minPathDistance = fields[4].clone();
        let maxPathDistance = fields[4].clone();

        println!(
            "personId: {}, country: {}, tagClass: {}, minPathDistance: {}, maxPathDistance: {}",
            personId, country, tagClass, minPathDistance, maxPathDistance
        );

        params_map.insert("personId".to_string(), personId);
        params_map.insert("country".to_string(), country);
        params_map.insert("tagClass".to_string(), tagClass);
        params_map.insert("minPathDistance".to_string(), minPathDistance);
        params_map.insert("maxPathDistance".to_string(), maxPathDistance);
    }

    let mut conf = JobConf::new(query_name.clone());
    conf.set_workers(worker_num);
    conf.reset_servers(ServerConf::Partial(vec![0]));
    conf.batch_capacity = batch_capacity;
    conf.batch_size = batch_size;

    let query = query_register
        .get_query(&query_name)
        .expect("Could not find query");

    let query_start = Instant::now();
    let result = {
        pegasus::run(conf.clone(), || query.Query(conf.clone(), &graph, &graph_index, params_map.clone()))
            .expect("submit query failure")
    };
    for x in result {
        let data_set = x.expect("Fail to get result");
    }
    println!("Finished query elapsed: {} ms", query_start.elapsed().as_micros() as f64 / 1e3);

    Ok(())
}
