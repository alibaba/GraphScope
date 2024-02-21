use std::collections::HashMap;

use dlopen::wrapper::{Container, WrapperApi};
use graph_index::types::{ArrayData, DataType as IndexDataType};
use graph_index::GraphIndex;
use mcsr::graph_db_impl::{CsrDB, SingleSubGraph, SubGraph};
use mcsr::types::LabelId;
use pegasus::api::*;
use pegasus::errors::BuildJobError;
use pegasus::result::ResultSink;
use pegasus::{Configuration, JobConf, ServerConf};

#[derive(WrapperApi)]
pub struct QueryApi {
    Query: fn(
        conf: JobConf,
        graph: &'static CsrDB<usize, usize>,
        graph_index: &'static GraphIndex,
        input_params: Vec<String>,
    ) -> Box<dyn Fn(&mut Source<i32>, ResultSink<Vec<u8>>) -> Result<(), BuildJobError>>,
}

#[derive(WrapperApi)]
pub struct PrecomputeApi {
    Precompute: fn(
        conf: JobConf,
        graph: &'static CsrDB<usize, usize>,
        graph_index: &'static GraphIndex,
        index_info: &Vec<(String, IndexDataType)>,
        is_edge: bool,
        label: LabelId,
        src_label: Option<LabelId>,
        dst_label: Option<LabelId>,
    ) -> Box<
        dyn Fn(&mut Source<i32>, ResultSink<(Vec<usize>, Vec<ArrayData>)>) -> Result<(), BuildJobError>,
    >,
}

pub struct QueryRegister {
    query_map: HashMap<String, Container<QueryApi>>,
}

impl QueryRegister {
    pub fn new() -> Self {
        Self { query_map: HashMap::new() }
    }

    pub fn register(&mut self, query_name: String, lib: Container<QueryApi>) {
        self.query_map.insert(query_name, lib);
    }

    pub fn get_query(&self, query_name: &String) -> Option<&Container<QueryApi>> {
        self.query_map.get(query_name)
    }
}
