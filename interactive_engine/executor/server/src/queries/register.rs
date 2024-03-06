use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;
use bmcsr::graph::Direction;

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
pub struct PrecomputeVertexApi {
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

#[derive(WrapperApi)]
pub struct PrecomputeEdgeApi {
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
        dyn Fn(&mut Source<i32>, ResultSink<(Vec<usize>, Vec<ArrayData>, Vec<usize>, Vec<ArrayData>)>) -> Result<(), BuildJobError>,
    >,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PrecomputeLabel {
    pub src_label: Option<u32>,
    pub dst_label: Option<u32>,
    pub edge_label: Option<u32>,
    vertex_label: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PrecomputeProperty {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PrecomputeSetting {
    pub precompute_name: String,
    pub precompute_type: String,
    pub label: PrecomputeLabel,
    pub properties: Vec<PrecomputeProperty>,
    path: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesSetting {
    queries_name: String,
    path: String,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct QueriesConfig {
    precompute: Option<Vec<PrecomputeSetting>>,
    read_queries: Vec<QueriesSetting>,
}

pub struct QueryRegister {
    query_map: HashMap<String, Container<QueryApi>>,
    precompute_vertex_map: HashMap<String, (PrecomputeSetting, Container<PrecomputeVertexApi>)>,
    precompute_edge_map: HashMap<String, (PrecomputeSetting, Container<PrecomputeEdgeApi>)>,
}

impl QueryRegister {
    pub fn new() -> Self {
        Self {
            query_map: HashMap::new(),
            precompute_vertex_map: HashMap::new(),
            precompute_edge_map: HashMap::new(),
        }
    }

    fn register(&mut self, query_name: String, lib: Container<QueryApi>) {
        self.query_map.insert(query_name, lib);
    }

    fn register_vertex_precompute(&mut self, query_name: String, setting: PrecomputeSetting, lib: Container<PrecomputeVertexApi>) {
        self.precompute_vertex_map.insert(query_name, (setting, lib));
    }

    fn register_edge_precompute(&mut self, query_name: String, setting: PrecomputeSetting, lib: Container<PrecomputeEdgeApi>) {
        self.precompute_edge_map.insert(query_name, (setting, lib));
    }

    pub fn load(&mut self, config_path: &PathBuf) {
        let file = File::open(config_path).expect("Failed to open config file");
        let config: QueriesConfig = serde_yaml::from_reader(file).expect("Could not read values");
        if let Some(precomputes) = config.precompute {
            for precompute in precomputes {
                let lib_path = precompute.path.clone();
                if precompute.precompute_type == "vertex" {
                    let libc: Container<PrecomputeVertexApi> = unsafe { Container::load(lib_path) }.unwrap();
                    self.register_vertex_precompute(precompute.precompute_name.clone(), precompute.clone(), libc);
                } else {
                    let libc: Container<PrecomputeEdgeApi> = unsafe { Container::load(lib_path) }.unwrap();
                    self.register_edge_precompute(precompute.precompute_name.clone(), precompute.clone(), libc);
                }
            }
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

    pub fn get_precompute_vertex(&self, precompute_name: &String) -> Option<&(PrecomputeSetting, Container<PrecomputeVertexApi>)> {
        self.precompute_vertex_map.get(precompute_name)
    }

    pub fn precompute_names(&self) -> Vec<String> {
        self.precompute_vertex_map.keys().cloned().collect()
    }

    pub fn run_precomputes(&self, graph: &GraphDB<usize, usize>, graph_index: &mut GraphIndex, worker_num: u32)
    {

        for (i, (name, (setting, libc))) in self.precompute_vertex_map.iter().enumerate() {
            let start = Instant::now();

            let job_name = format!("{}-{}", name, i);
            let mut conf = JobConf::new(job_name);
            conf.set_workers(worker_num);
            conf.reset_servers(ServerConf::Partial(vec![0]));
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
            let mut result_vec = vec![];
            for x in result {
                let (index_set, data_set) = x.expect("Fail to get result");
                result_vec.push((index_set, data_set));
            }
            for (index_set, data_set) in result_vec {
                for i in 0..properties_size {
                    graph_index.add_vertex_index_batch(
                        label,
                        &properties_info[i].0,
                        &index_set,
                        data_set[i].as_ref(),
                    ).unwrap();
                }
            }
            println!(
                "Finished run query {}, time: {}",
                &setting.precompute_name,
                start.elapsed().as_millis()
            );
        }
        for (i, (name, (setting, libc))) in self.precompute_edge_map.iter().enumerate() {
            let start = Instant::now();

            let job_name = format!("{}-{}", name, i);
            let mut conf = JobConf::new(job_name);
            conf.set_workers(worker_num);
            conf.reset_servers(ServerConf::Partial(vec![0]));
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
            let oe_property_size = graph.get_max_edge_offset(src_label.unwrap(), label, dst_label.unwrap(), Direction::Outgoing);
            for i in 0..properties_info.len() {
                graph_index.init_outgoing_edge_index(
                    properties_info[i].0.clone(),
                    src_label.unwrap(),
                    dst_label.unwrap(),
                    label,
                    properties_info[i].1.clone(),
                    Some(oe_property_size),
                    Some(Item::Int32(0)),
                ).unwrap();
            }
            let ie_property_size = graph.get_max_edge_offset(src_label.unwrap(), label, dst_label.unwrap(), Direction::Incoming);
            for i in 0..properties_info.len() {
                graph_index.init_incoming_edge_index(
                    properties_info[i].0.clone(),
                    src_label.unwrap(),
                    dst_label.unwrap(),
                    label,
                    properties_info[i].1.clone(),
                    Some(ie_property_size),
                    Some(Item::Int32(0)),
                ).unwrap();
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
            let mut result_vec = vec![];
            for x in result {
                let (in_index_set, in_data_set, out_index_set, out_data_set) = x.expect("Fail to get result");
                result_vec.push((in_index_set, in_data_set, out_index_set, out_data_set));
            }
            for (in_index_set, in_data_set, out_index_set, out_data_set) in result_vec {
                for i in 0..properties_size {
                    graph_index.add_outgoing_edge_index_batch(
                        src_label.unwrap(),
                        label,
                        dst_label.unwrap(),
                        &properties_info[i].0,
                        &out_index_set,
                        out_data_set[i].as_ref(),
                    ).unwrap();
                    graph_index.add_incoming_edge_index_batch(
                        src_label.unwrap(),
                        label,
                        dst_label.unwrap(),
                        &properties_info[i].0,
                        &in_index_set,
                        in_data_set[i].as_ref(),
                    ).unwrap();
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