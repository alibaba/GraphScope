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
use pegasus::{Configuration, JobConf, ServerConf};
use rpc_server::queries::register::QueryRegister;
use rpc_server::queries::rpc::RPCServerConfig;
use serde::Deserialize;
use structopt::StructOpt;

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
    #[structopt(short = "o", long = "output_dir", default_value = "")]
    output_dir: PathBuf,
    #[structopt(short = "w", long = "worker_num", default_value = "8")]
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

lazy_static! {
    static ref PARAMETERS_MAP: HashMap<&'static str, Vec<&'static str>> = {
        let mut m = HashMap::new();
        m.insert("bi1", vec!["bi-1.csv"]);
        m.insert("bi2", vec!["bi-2a.csv", "bi-2b.csv"]);
        m.insert("bi3", vec!["bi-3.csv"]);
        m.insert("bi4", vec!["bi-4.csv"]);
        m.insert("bi5", vec!["bi-5.csv"]);
        m.insert("bi6", vec!["bi-6.csv"]);
        m.insert("bi7", vec!["bi-7.csv"]);
        m.insert("bi8", vec!["bi-8a.csv", "bi-8b.csv"]);
        m.insert("bi9", vec!["bi-9.csv"]);
        m.insert("bi10", vec!["bi-10a.csv", "bi-10b.csv"]);
        m.insert("bi11", vec!["bi-11.csv"]);
        m.insert("bi12", vec!["bi-12.csv"]);
        m.insert("bi13", vec!["bi-13.csv"]);
        m.insert("bi14", vec!["bi-14a.csv", "bi-14b.csv"]);
        m.insert("bi15", vec!["bi-15a.csv", "bi-15b.csv"]);
        m.insert("bi16", vec!["bi-16a.csv", "bi-16b.csv"]);
        m.insert("bi17", vec!["bi-17.csv"]);
        m.insert("bi18", vec!["bi-18.csv"]);
        m.insert("bi19", vec!["bi-19a.csv", "bi-19b.csv"]);
        m.insert("bi20", vec!["bi-20a.csv", "bi-20b.csv"]);

        m
    };
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    let config: Config = Config::from_args();
    let worker_num = config.worker_num;
    let graph_data_str = config.graph_data.to_str().unwrap();
    let output_dir = config.output_dir;

    let mut graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let mut graph_index = GraphIndex::new(0);

    // let traverse_out = output_dir.clone().join(format!("init"));
    // std::fs::create_dir_all(&traverse_out).unwrap();
    // traverse(&graph, traverse_out.to_str().unwrap());

    let mut query_register = QueryRegister::new();
    if !config.queries_config.is_empty() {
        println!("Start load lib");
        query_register.load(&PathBuf::from(&config.queries_config));
        println!("Finished load libs");
    }

    let batches = [
        "2012-11-29",
        "2012-11-30",
        "2012-12-01",
        "2012-12-02",
        "2012-12-03",
        "2012-12-04",
        "2012-12-05",
        "2012-12-06",
        "2012-12-07",
        "2012-12-08",
        "2012-12-09",
        "2012-12-10",
        "2012-12-11",
        "2012-12-12",
        "2012-12-13",
        "2012-12-14",
        "2012-12-15",
        "2012-12-16",
        "2012-12-17",
        "2012-12-18",
        "2012-12-19",
        "2012-12-20",
        "2012-12-21",
        "2012-12-22",
        "2012-12-23",
        "2012-12-24",
        "2012-12-25",
        "2012-12-26",
        "2012-12-27",
        "2012-12-28",
        "2012-12-29",
        "2012-12-30",
        "2012-12-31",
    ];
    let batches = ["2012-11-29"];

    let graph_raw = config.graph_raw;
    let batch_configs = config.batch_update_configs;

    for batch_id in batches {
        let batch_id = batch_id.to_string();
        let insert_file_name = format!("insert-{}.json", batch_id);
        let insert_schema_file_path = batch_configs.join(insert_file_name);
        let delete_file_name = format!("delete-{}.json", batch_id);
        let delete_schema_file_path = batch_configs.join(delete_file_name);

        let mut graph_modifier = GraphModifier::new(&graph_raw);
        graph_modifier.skip_header();
        let insert_schema =
            InputSchema::from_json_file(insert_schema_file_path, &graph.graph_schema).unwrap();
        graph_modifier
            .insert(&mut graph, &insert_schema)
            .unwrap();

        let mut delete_generator = DeleteGenerator::new(&graph_raw);
        delete_generator.skip_header();
        delete_generator.generate(&mut graph, batch_id.as_str());

        info!("delete schema file: {:?}", delete_schema_file_path);
        let delete_schema =
            InputSchema::from_json_file(delete_schema_file_path, &graph.graph_schema).unwrap();
        graph_modifier
            .delete(&mut graph, &delete_schema)
            .unwrap();

        let traverse_out = output_dir
            .clone()
            .join(format!("date-{}", batch_id));
        std::fs::create_dir_all(&traverse_out).unwrap();
        traverse(&graph, traverse_out.to_str().unwrap());
    }

    Ok(())
}
