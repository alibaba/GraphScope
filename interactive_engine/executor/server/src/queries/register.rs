use std::arch::aarch64::vqmovuns_s32;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;

use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::{ArrayData, DataType as IndexDataType, Item};
use graph_index::GraphIndex;
use bmcsr::graph_db::GraphDB;
use bmcsr::types::LabelId;
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::{Configuration, JobConf, ServerConf};
use serde::{Deserialize, Serialize};

#[derive(WrapperApi)]
pub struct QueryApi {
    Query: fn(
        conf: JobConf,
        graph: &GraphDB<usize, usize>,
        graph_index: &GraphIndex,
        input_params: HashMap<String, String>,
    ) -> Box<dyn Fn(&mut Source<i32>, ResultSink<Vec<u8>>) -> Result<(), BuildJobError>>,
}

#[derive(WrapperApi)]
pub struct PrecomputeApi {
    Precompute: fn(
        conf: JobConf,
        graph: &GraphDB<usize, usize>,
        graph_index: &GraphIndex,
        index_info: &Vec<(String, IndexDataType)>,
        is_edge: bool,
        label: LabelId,
        src_label: Option<LabelId>,
        dst_label: Option<LabelId>,
    ) -> Box<
        dyn Fn(&mut Source<i32>, ResultSink<(Vec<usize>, Vec<ArrayData>)>) -> Result<(), BuildJobError>,
    >,
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

pub struct QueryRegister {
    query_map: HashMap<String, Container<QueryApi>>,
    precompute_map: HashMap<String, (PrecomputeSetting, Container<PrecomputeApi>)>,
}

impl QueryRegister {
    pub fn new() -> Self {
        Self {
            query_map: HashMap::new(),
            precompute_map: HashMap::new(),
        }
    }

    fn register(&mut self, query_name: String, lib: Container<QueryApi>) {
        self.query_map.insert(query_name, lib);
    }

    fn register_precompute(&mut self, query_name: String, setting: PrecomputeSetting, lib: Container<PrecomputeApi>) {
        self.precompute_map.insert(query_name, (setting, lib));
    }

    pub fn load(&mut self, config_path: &PathBuf) {
        let file = File::open(config_path).expect("Failed to open config file");
        let config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values");
        for precompute in config.precompute {
            let lib_path = precompute.path.clone();
            let libc: Container<PrecomputeApi> = unsafe { Container::load(lib_path) }.unwrap();

            self.register_precompute(precompute.precompute_name.clone(), precompute.clone(), libc);
        }
        for query in config.read_queries {
            let lib_path = query.path.clone();
            let libc: Container<QueryApi> = unsafe { Container::load(lib_path) }.unwrap();

            self.register(query.queries_name, libc);
        }
    }

    pub fn get_query(&self, query_name: &String) -> Option<&Container<QueryApi>> {
        self.query_map.get(query_name)
    }

    pub fn get_precompute(&self, precompute_name: &String) -> Option<&(PrecomputeSetting, Container<PrecomputeApi>)> {
        self.precompute_map.get(precompute_name)
    }

    pub fn precompute_names(&self) -> Vec<String> {
        self.precompute_map.keys().cloned().collect()
    }

    pub fn run_precomputes(&self, graph: &GraphDB<usize, usize>, graph_index: &GraphIndex, worker_num: u32)
    {
        for (i, (name, (setting, libc))) in self.precompute_map.iter().enumerate() {
            let start = Instant::now();

            let job_name = format!("{}-{}", name, i);
            let mut conf = JobConf::new(job_name);
            conf.set_workers(worker_num);
            let label = setting.label.edge_label.unwrap() as LabelId;
            let src_label = Some(setting.label.src_label.unwrap() as LabelId);
            let dst_label = Some(setting.label.dst_label.unwrap() as LabelId);
            let mut properties_info = vec![];
            let properties_size = setting.properties.len();
            for i in 0..properties_size {
                let index_name = setting.properties[i].name.clone();
                let data_type = graph_index::types::str_to_data_type(&setting.properties[i].data_type);
                properties_info.push((index_name, data_type));
            }
            if setting.precompute_type == "vertex" {
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
                if setting.precompute_type == "edge" {
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
                } else if setting.precompute_type == "vertex" {
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
                &setting.precompute_name,
                start.elapsed().as_millis()
            );
        }
    }
}
