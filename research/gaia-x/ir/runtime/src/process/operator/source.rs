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

use crate::graph::element::{Edge, Vertex};
use crate::graph::graph::QueryParams;
use crate::graph::partitioner::Partitioner;
use crate::graph::ID;
use crate::process::record::Record;
use ir_common::error::{str_to_dyn_error, ParsePbError};
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common::property;
use ir_common::generated::common::value;
use ir_common::NameOrId;
use pegasus::api::function::FnResult;
use std::collections::HashMap;
use std::sync::Arc;

pub enum SourceType {
    Vertex,
    Edge,
    Table,
}

/// Source Operator, fetching a source from the (graph) database
pub struct SourceOperator<'a> {
    params: QueryParams<'a>,
    src: Option<HashMap<u64, Vec<ID>>>,
    tag: Option<NameOrId>,
    source_type: SourceType,
}

impl<'a> SourceOperator<'a> {
    fn with(scan: algebra_pb::Scan) -> Self {
        let scan_opt: algebra_pb::scan::ScanOpt = unsafe { ::std::mem::transmute(scan.scan_opt) };
        let source_type = match scan_opt {
            algebra_pb::scan::ScanOpt::Vertex => SourceType::Vertex,
            algebra_pb::scan::ScanOpt::Edge => SourceType::Edge,
            algebra_pb::scan::ScanOpt::Table => SourceType::Table,
        };
        // TODO: check tag is none?
        SourceOperator {
            params: QueryParams::default(),
            src: None,
            tag: None,
            source_type,
        }
    }

    fn set_tag(&mut self, tag: NameOrId) {
        self.tag = Some(tag);
    }

    /// Assign source vertex ids for each worker to call get_vertex
    fn set_src(&mut self, ids: Vec<ID>, job_workers: usize, partitioner: Arc<dyn Partitioner>) {
        let mut partitions = HashMap::new();
        for id in ids {
            if let Ok(wid) = partitioner.get_partition(&id, job_workers) {
                partitions.entry(wid).or_insert_with(Vec::new).push(id);
            } else {
                debug!("get server id failed in graph_partition_manager in source op");
            }
        }

        self.src = Some(partitions);
    }

    /// Assign partition_list for each worker to call scan_vertex
    fn set_partitions(
        &mut self,
        job_workers: usize,
        worker_index: u32,
        partitioner: Arc<dyn Partitioner>,
    ) {
        if let Ok(partition_list) = partitioner.get_worker_partitions(job_workers, worker_index) {
            debug!(
                "Assign worker {:?} to scan partition list: {:?}",
                worker_index, partition_list
            );
            self.params.partitions = partition_list;
        } else {
            debug!("get partition list failed in graph_partition_manager in source op");
        }
    }
}

impl<'a> SourceOperator<'a> {
    pub fn gen_source(
        self,
        worker_index: usize,
    ) -> FnResult<Box<dyn Iterator<Item = Record> + Send>> {
        let graph = crate::get_graph().unwrap();
        match self.source_type {
            SourceType::Vertex => {
                let mut v_source =
                    Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Vertex> + Send>;
                if let Some(ref seeds) = self.src {
                    if let Some(src) = seeds.get(&(worker_index as u64)) {
                        if !src.is_empty() {
                            v_source = graph.get_vertex(src, &self.params)?;
                        }
                    }
                } else {
                    // parallel scan, and each worker should scan the partitions assigned to it in self.v_params.partitions
                    v_source = graph.scan_vertex(&self.params)?;
                };
                let tag = self.tag;
                Ok(Box::new(v_source.map(move |v| Record::new(v, tag.clone()))))
            }
            SourceType::Edge => {
                let mut e_source =
                    Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Edge> + Send>;
                if let Some(ref seeds) = self.src {
                    if let Some(src) = seeds.get(&(worker_index as u64)) {
                        if !src.is_empty() {
                            e_source = graph.get_edge(src, &self.params)?;
                        }
                    }
                } else {
                    // parallel scan, and each worker should scan the partitions assigned to it in self.e_params.partitions
                    e_source = graph.scan_edge(&self.params)?;
                }
                let tag = self.tag;
                Ok(Box::new(e_source.map(move |e| Record::new(e, tag.clone()))))
            }
            SourceType::Table => Err(str_to_dyn_error(
                "Source type of Table is not supported yet",
            )),
        }
    }
}

pub fn source_op_from(
    source_pb: &mut algebra_pb::logical_plan::Operator,
    job_workers: usize,
    worker_index: u32,
    partitioner: Arc<dyn Partitioner>,
) -> Result<SourceOperator<'static>, ParsePbError> {
    if let Some(opr) = source_pb.opr.take() {
        match opr {
            // TODO: no alias field in scan?
            algebra_pb::logical_plan::operator::Opr::Scan(scan) => {
                let mut source_op = SourceOperator::with(scan);
                source_op.set_partitions(job_workers, worker_index, partitioner);
                Ok(source_op)
            }
            algebra_pb::logical_plan::operator::Opr::IndexedScan(indexed_scan) => {
                let scan = indexed_scan.scan.ok_or("scan is missing in indexed_scan")?;
                let mut source_op = SourceOperator::with(scan);
                let global_ids = source_ids_from(indexed_scan.or_kv_equiv_pairs)?;
                source_op.set_src(global_ids, job_workers, partitioner);
                Ok(source_op)
            }
            _ => Err("Unsupported source op in pb_request")?,
        }
    } else {
        Err("Empty source op in pb_request")?
    }
}

// TODO: we only support global-ids as index for now;
fn source_ids_from(
    or_kv_equiv_pairs: Vec<algebra_pb::indexed_scan::KvEquivPairs>,
) -> Result<Vec<ID>, ParsePbError> {
    let mut global_ids = vec![];
    for or_kv_pair in or_kv_equiv_pairs {
        let kv_pair = or_kv_pair
            .pairs
            .get(0)
            .ok_or("kv_equiv_pair is empty in indexed_scan")?;
        let (key, value) = (kv_pair.key.as_ref(), kv_pair.value.as_ref());
        let key = key.ok_or("key is empty in kv_pair in indexed_scan")?;
        if let Some(property::Item::Id(_id_key)) = key.item.as_ref() {
            let value = value
                .ok_or("value is empty in kv_pair in indexed_scan")?
                .value
                .as_ref()
                .ok_or("value is empty in kv_pair in indexed_scan")?;
            // TODO(bingqing): confirm global id of i64?
            if let Some(value::Item::I64(v)) = value.item {
                global_ids.push(v as ID);
            } else {
                warn!("Parse source_id from indexed_scan failed");
            }
        } else {
            return Err("Only support IdKey as indexed field in scan for now")?;
        }
    }
    Ok(global_ids)
}
