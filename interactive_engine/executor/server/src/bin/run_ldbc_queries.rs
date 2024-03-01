extern crate dlopen;
#[macro_use]
extern crate dlopen_derive;
extern crate core;

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use structopt::StructOpt;

use rpc_server::queries::register::{PrecomputeApi, QueryApi, QueryRegister};
use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::Item;

use pegasus::{Configuration, JobConf, ServerConf};
use serde::{Deserialize, Serialize};
use rpc_server::queries::rpc::RPCServerConfig;

use std::fs::File;
use std::io::{BufRead, BufReader};
use bmcsr::graph_db::GraphDB;
use bmcsr::graph_modifier::{DeleteGenerator, GraphModifier};
use bmcsr::schema::InputSchema;

use bmcsr::types::LabelId;
use graph_index::GraphIndex;
use lazy_static::lazy_static;

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

fn register_precompute(settings: &Vec<PrecomputeSetting>) -> Vec<Container<crate::PrecomputeApi>> {
    let mut ret = vec![];
    for precompute in settings.iter() {
        let lib_path = precompute.path.clone();
        let libc: Container<crate::PrecomputeApi> = unsafe { Container::load(lib_path) }.unwrap();
        ret.push(libc);
    }
    ret
}
fn precompute_all(
    graph: &GraphDB<usize, usize>, graph_index: &GraphIndex,
    settings: &Vec<PrecomputeSetting>, libs: &Vec<Container<PrecomputeApi>>, worker_num: u32) {
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
        let result = {
            pegasus::run(conf.clone(), || {
                libc.Precompute(
                    conf.clone(),
                    graph,
                    graph_index,
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
                   graph_index.add_edge_index_batch(
                        src_label.unwrap(),
                        label,
                        dst_label.unwrap(),
                        &properties_info[i].0,
                        &index_set,
                        data_set[i].as_ref(),
                    ).unwrap();
                }
            } else if precompute.precompute_type == "vertex" {
                for i in 0..properties_size {
                    graph_index.add_vertex_index_batch(
                        label,
                        &properties_info[i].0,
                        &index_set,
                        data_set[i].as_ref(),
                    ).unwrap();
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
    let output_dir = config.output_dir;

    let mut graph = GraphDB::<usize, usize>::deserialize(graph_data_str, 0, None).unwrap();
    let graph_index = GraphIndex::new(0);

    let queries_config_path = config.queries_config;
    let file = File::open(queries_config_path).expect("Failed to open precompute config file");
    let precompute_config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values.");
    let precompute_libs = register_precompute(&precompute_config.precompute);
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
    // let batches = ["2012-11-29"];

    let graph_raw = config.graph_raw;
    let batch_configs = config.batch_update_configs;

    for batch_id in batches {
        let batch_id = batch_id.to_string();
        let insert_file_name = format!("insert-{}.json", batch_id);
        let insert_schema_file_path = batch_configs.join(insert_file_name);
        let delete_file_name = format!("delete-{}.json", batch_id);
        let delete_schema_file_path = PathBuf::from(delete_file_name);

        let mut graph_modifier = GraphModifier::new(&graph_raw);
        graph_modifier.skip_header();
        let insert_schema = InputSchema::from_json_file(insert_schema_file_path, &graph.graph_schema).unwrap();
        graph_modifier
            .insert(&mut graph, &insert_schema)
            .unwrap();

        let mut delete_generator = DeleteGenerator::new(&graph_raw);
        delete_generator.skip_header();
        delete_generator.generate(&mut graph, batch_id.as_str());

        let delete_schema = InputSchema::from_json_file(delete_schema_file_path, &graph.graph_schema).unwrap();
        graph_modifier
            .delete(&mut graph, &delete_schema)
            .unwrap();

        precompute_all(&graph, &graph_index, &precompute_config.precompute, &precompute_libs, worker_num);

        println!("Start iterating parameter files: {:?}", config.parameters);
        if config.parameters.is_dir() {
            let start = Instant::now();
            for pair in PARAMETERS_MAP.iter() {
                let query_name = pair.0.to_string();
                let query = query_register.get_query(&query_name).expect("Could not find query");
                let files = pair.1.clone();

                for filename in files.iter() {
                    let path = config.parameters.clone().join(filename);
                    if !path.is_file() {
                        continue;
                    }
                    let file = File::open(path).expect("Failed to open query parameter file");
                    let reader = BufReader::new(file);
                    let mut keys = Vec::<String>::new();
                    let mut first_line = true;
                    for line_result in reader.lines() {
                        let line = line_result.unwrap();
                        if first_line {
                            first_line = false;
                            keys = line.split('|').map(|s| s.to_string().split(':').next().unwrap().to_string()).collect();
                            continue;
                        }

                        let params: Vec<String> = line.split('|').map(|s| s.to_string()).collect();
                        let mut params_map = HashMap::new();
                        for (index, key) in keys.iter().enumerate() {
                            params_map.insert(key, params[index].clone());
                        }
                        let mut conf = JobConf::new(query_name.clone());
                        conf.set_workers(worker_num);
                        let result = {
                            pegasus::run(conf.clone(), || {
                                query.Query(
                                    conf.clone(),
                                    &graph,
                                    &graph_index,
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
    }

    Ok(())
}