//
//! Copyright 2021 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use dyn_type::Object;
use graph_proxy::apis::graph::PKV;
use graph_proxy::apis::{get_graph, Edge, Partitioner, QueryParams, Vertex, ID};
use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::algebra as algebra_pb;
use ir_common::{KeyId, NameOrId};

use crate::error::{FnGenError, FnGenResult};
use crate::process::record::Record;

#[derive(Debug)]
pub enum SourceType {
    Vertex,
    Edge,
    Table,
}

/// Source Operator, fetching a source from the (graph) database
#[derive(Debug)]
pub struct SourceOperator {
    query_params: QueryParams,
    src: Option<HashMap<u64, Vec<ID>>>,
    primary_key_values: Option<PKV>,
    alias: Option<KeyId>,
    source_type: SourceType,
}

impl SourceOperator {
    pub fn new(
        opr: algebra_pb::logical_plan::operator::Opr, job_workers: usize, worker_index: u32,
        partitioner: Arc<dyn Partitioner>,
    ) -> ParsePbResult<Self> {
        match opr {
            algebra_pb::logical_plan::operator::Opr::Scan(scan) => {
                if let Some(index_predicate) = &scan.idx_predicate {
                    let ip = index_predicate.clone();
                    let ip2 = index_predicate.clone();
                    let mut source_op = SourceOperator::try_from(scan)?;
                    let global_ids: Vec<ID> = <Vec<i64>>::try_from(ip)?
                        .into_iter()
                        .map(|i| i as ID)
                        .collect();
                    if !global_ids.is_empty() {
                        // query by global_ids
                        source_op.set_src(global_ids, job_workers, partitioner)?;
                        debug!("Runtime source op of indexed scan of global ids {:?}", source_op);
                    } else {
                        // query by indexed_scan
                        let primary_key_values = <Vec<(NameOrId, Object)>>::try_from(ip2)?;
                        source_op.primary_key_values = Some(PKV::from(primary_key_values));
                        debug!("Runtime source op of indexed scan {:?}", source_op);
                    }
                    Ok(source_op)
                } else {
                    let mut source_op = SourceOperator::try_from(scan)?;
                    source_op.set_partitions(job_workers, worker_index, partitioner)?;
                    debug!("Runtime source op of scan {:?}", source_op);
                    Ok(source_op)
                }
            }
            _ => Err(ParsePbError::from("algebra_pb op is not a source"))?,
        }
    }

    /// Assign source vertex ids for each worker to call get_vertex
    fn set_src(
        &mut self, ids: Vec<ID>, job_workers: usize, partitioner: Arc<dyn Partitioner>,
    ) -> ParsePbResult<()> {
        let mut partitions = HashMap::new();
        for id in ids {
            match partitioner.get_partition(&id, job_workers) {
                Ok(wid) => {
                    partitions
                        .entry(wid)
                        .or_insert_with(Vec::new)
                        .push(id);
                }
                Err(err) => Err(ParsePbError::Unsupported(format!(
                    "get server id failed in graph_partition_manager in source op {:?}",
                    err
                )))?,
            }
        }
        self.src = Some(partitions);
        Ok(())
    }

    /// Assign partition_list for each worker to call scan_vertex
    fn set_partitions(
        &mut self, job_workers: usize, worker_index: u32, partitioner: Arc<dyn Partitioner>,
    ) -> ParsePbResult<()> {
        let res = partitioner.get_worker_partitions(job_workers, worker_index);
        match res {
            Ok(partition_list) => {
                debug!("Assign worker {:?} to scan partition list: {:?}", worker_index, partition_list);
                self.query_params.partitions = partition_list;
            }
            Err(err) => {
                debug!("get partition list failed in graph_partition_manager in source op {:?}", err);
                Err(ParsePbError::Unsupported(
                    format!("get partition list failed in graph_partition_manager in source op {:?}", err)
                        .to_string(),
                ))?
            }
        }
        Ok(())
    }
}

impl SourceOperator {
    pub fn gen_source(self, worker_index: usize) -> FnGenResult<Box<dyn Iterator<Item = Record> + Send>> {
        let graph = get_graph().ok_or(FnGenError::NullGraphError)?;
        match self.source_type {
            SourceType::Vertex => {
                let mut v_source = Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Vertex> + Send>;
                if let Some(ref seeds) = self.src {
                    if let Some(src) = seeds.get(&(worker_index as u64)) {
                        if !src.is_empty() {
                            v_source = graph.get_vertex(src, &self.query_params)?;
                        }
                    }
                } else if let Some(ref indexed_values) = self.primary_key_values {
                    if self.query_params.labels.len() != 1 {
                        Err(FnGenError::unsupported_error("indexed_scan with empty/multiple labels"))?
                    }
                    if let Some(v) = graph.index_scan_vertex(
                        &self.query_params.labels[0],
                        indexed_values,
                        &self.query_params,
                    )? {
                        v_source = Box::new(vec![v].into_iter())
                    }
                } else {
                    // parallel scan, and each worker should scan the partitions assigned to it in self.v_params.partitions
                    v_source = graph.scan_vertex(&self.query_params)?;
                };
                Ok(Box::new(v_source.map(move |v| Record::new(v, self.alias.clone()))))
            }
            SourceType::Edge => {
                let mut e_source = Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Edge> + Send>;
                if let Some(ref seeds) = self.src {
                    if let Some(src) = seeds.get(&(worker_index as u64)) {
                        if !src.is_empty() {
                            e_source = graph.get_edge(src, &self.query_params)?;
                        }
                    }
                } else {
                    // parallel scan, and each worker should scan the partitions assigned to it in self.e_params.partitions
                    e_source = graph.scan_edge(&self.query_params)?;
                }
                Ok(Box::new(e_source.map(move |e| Record::new(e, self.alias.clone()))))
            }
            SourceType::Table => Err(FnGenError::unsupported_error("data source of `Table` type"))?,
        }
    }
}

impl TryFrom<algebra_pb::Scan> for SourceOperator {
    type Error = ParsePbError;

    fn try_from(scan_pb: algebra_pb::Scan) -> Result<Self, Self::Error> {
        let scan_opt: algebra_pb::scan::ScanOpt = unsafe { ::std::mem::transmute(scan_pb.scan_opt) };
        let source_type = match scan_opt {
            algebra_pb::scan::ScanOpt::Vertex => SourceType::Vertex,
            algebra_pb::scan::ScanOpt::Edge => SourceType::Edge,
            algebra_pb::scan::ScanOpt::Table => SourceType::Table,
        };
        let alias = scan_pb
            .alias
            .map(|alias| KeyId::try_from(alias))
            .transpose()?;

        let query_params = QueryParams::try_from(scan_pb.params)?;

        Ok(SourceOperator { query_params, src: None, primary_key_values: None, alias, source_type })
    }
}
