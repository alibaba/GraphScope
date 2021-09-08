//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use crate::generated::gremlin as pb;
use crate::generated::gremlin::EntityType;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::{Requirement, Traverser};
use crate::structure::{Edge, QueryParams, Vertex, ID};
use crate::{FromPb, Partitioner};
use bit_set::BitSet;
use pegasus::BuildJobError;
use pegasus_common::downcast::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;

/// V(), E()
pub struct GraphVertexStep {
    pub symbol: StepSymbol,
    pub v_params: QueryParams<Vertex>,
    pub e_params: QueryParams<Edge>,
    src: Option<HashMap<u64, Vec<ID>>>,
    as_tags: BitSet,
    requirement: Requirement,
    return_type: EntityType,
}

impl_as_any!(GraphVertexStep);

impl GraphVertexStep {
    pub fn new(return_type: EntityType, req: Requirement) -> Self {
        let symbol = if return_type == EntityType::Vertex { StepSymbol::V } else { StepSymbol::E };
        GraphVertexStep {
            symbol,
            src: None,
            as_tags: BitSet::new(),
            requirement: req,
            v_params: QueryParams::default(),
            e_params: QueryParams::default(),
            return_type,
        }
    }

    pub fn set_return_type(&mut self, return_type: EntityType) {
        self.return_type = return_type;
    }

    /// Assign source vertex ids for each worker to call get_vertex
    pub fn set_src(&mut self, ids: Vec<ID>, job_workers: usize, partitioner: Arc<dyn Partitioner>) {
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
    pub fn set_partitions(
        &mut self, job_workers: usize, worker_index: u32, partitioner: Arc<dyn Partitioner>,
    ) {
        if let Ok(partition_list) = partitioner.get_worker_partitions(job_workers, worker_index) {
            debug!("Assign worker {:?} to scan partition list: {:?}", worker_index, partition_list);
            if self.return_type == EntityType::Vertex {
                self.v_params.partitions = partition_list
            } else {
                self.e_params.partitions = partition_list
            }
        } else {
            debug!("get partition list failed in graph_partition_manager in source op");
        }
    }

    pub fn set_requirement(&mut self, requirement: Requirement) {
        self.requirement = requirement;
    }
    pub fn set_tags(&mut self, tags: BitSet) {
        self.as_tags = tags;
    }
}

impl Step for GraphVertexStep {
    fn get_symbol(&self) -> StepSymbol {
        self.symbol
    }
}

impl GraphVertexStep {
    pub fn gen_source(self, worker_index: usize) -> Box<dyn Iterator<Item = Traverser> + Send> {
        let graph = crate::get_graph().unwrap();
        let mut v_source = Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Vertex> + Send>;
        let mut e_source = Box::new(std::iter::empty()) as Box<dyn Iterator<Item = Edge> + Send>;

        if self.return_type == EntityType::Vertex {
            if let Some(ref seeds) = self.src {
                if let Some(src) = seeds.get(&(worker_index as u64)) {
                    if !src.is_empty() {
                        v_source = graph
                            .get_vertex(src, &self.v_params)
                            .unwrap_or(Box::new(std::iter::empty()));
                    }
                }
            } else {
                // parallel scan, and each worker should scan the partitions assigned to it in self.v_params.partitions
                v_source = graph.scan_vertex(&self.v_params).unwrap_or(Box::new(std::iter::empty()))
            };
        } else {
            if let Some(ref seeds) = self.src {
                if let Some(src) = seeds.get(&(worker_index as u64)) {
                    if !src.is_empty() {
                        e_source = graph
                            .get_edge(src, &self.e_params)
                            .unwrap_or(Box::new(std::iter::empty()));
                    }
                }
            } else {
                // parallel scan, and each worker should scan the partitions assigned to it in self.e_params.partitions
                e_source = graph.scan_edge(&self.e_params).unwrap_or(Box::new(std::iter::empty()));
            }
        }

        if self.requirement.contains(Requirement::PATH)
            || self.requirement.contains(Requirement::LABELED_PATH)
        {
            let tags = self.as_tags;
            let requirement = self.requirement.clone();
            if self.return_type == EntityType::Vertex {
                Box::new(v_source.map(move |v| Traverser::with_path(v, &tags, requirement)))
            } else {
                Box::new(e_source.map(move |e| Traverser::with_path(e, &tags, requirement)))
            }
        } else {
            if self.return_type == EntityType::Vertex {
                Box::new(v_source.map(|v| Traverser::new(v)))
            } else {
                Box::new(e_source.map(|e| Traverser::new(e)))
            }
        }
    }
}

pub fn graph_step_from(
    gremlin_step: &mut pb::GremlinStep, job_workers: usize, worker_index: u32,
    partitioner: Arc<dyn Partitioner>,
) -> Result<GraphVertexStep, BuildJobError> {
    if let Some(option) = gremlin_step.step.take() {
        match option {
            pb::gremlin_step::Step::GraphStep(opt) => {
                let requirements_pb = unsafe { std::mem::transmute(opt.traverser_requirements) };
                let requirements = Requirement::from_pb(requirements_pb)?;
                let return_type = unsafe { std::mem::transmute(opt.return_type) };
                let mut step = GraphVertexStep::new(return_type, requirements);
                step.set_tags(gremlin_step.get_tags());
                let mut ids = vec![];
                for id_bytes in opt.ids {
                    let id = read_be_u128(&mut id_bytes.as_slice());
                    debug!("source id_bytes: {:?}, id: {:?}", id_bytes, id);
                    ids.push(id);
                }
                if return_type == EntityType::Vertex {
                    step.v_params = QueryParams::from_pb(opt.query_params)?;
                } else {
                    step.e_params = QueryParams::from_pb(opt.query_params)?;
                }
                if !ids.is_empty() {
                    step.set_src(ids, job_workers, partitioner);
                } else {
                    step.set_partitions(job_workers, worker_index, partitioner);
                }
                return Ok(step);
            }
            _ => (),
        }
    }
    Err("Unsupported source step in pb_request")?
}

fn read_be_u128(input: &mut &[u8]) -> u128 {
    let (int_bytes, rest) = input.split_at(std::mem::size_of::<u128>());
    *input = rest;
    u128::from_be_bytes(int_bytes.try_into().unwrap())
}