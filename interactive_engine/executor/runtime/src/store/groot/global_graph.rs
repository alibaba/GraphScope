//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.


use maxgraph_store::db::api::{GraphResult, ValueRef, ValueType, EdgeDirection, GraphConfigBuilder, GraphConfig, GraphDef, Records, PropertyId};
use std::sync::Arc;
use maxgraph_store::api::{GlobalGraphQuery, SnapshotId, PartitionVertexIds, LabelId, Condition, PropId, VertexId, PartitionId, PartitionLabeledVertexIds};
use store::{LocalStoreVertex, LocalStoreEdge};
use maxgraph_store::api::graph_schema::Schema;
use std::collections::{HashMap, HashSet};
use std::vec::IntoIter;
use maxgraph_store::api::prelude::Property;
use std::iter::FromIterator;
use itertools::Itertools;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::config::StoreConfig;
use maxgraph_store::db::graph::store::GraphStore;
use store::groot::global_graph_schema::GlobalGraphSchema;
use maxgraph_store::db::graph::entity::{RocksVertexImpl, RocksEdgeImpl};
use maxgraph_store::db::api::multi_version_graph::MultiVersionGraph;
use maxgraph_store::db::api::types::RocksEdge;
use maxgraph_store::db::storage::RawBytes;

pub struct GlobalGraph {
    graph_partitions: Arc<HashMap<PartitionId, Arc<GraphStore>>>,
    total_partition: u32,
    partition_to_server: HashMap<PartitionId, u32>,
}

unsafe impl Send for GlobalGraph {}
unsafe impl Sync for GlobalGraph {}

impl GlobalGraph {
    pub fn empty(total_partition: u32) -> Self {
        GlobalGraph {
            graph_partitions: Arc::new(HashMap::new()),
            total_partition,
            partition_to_server: HashMap::new(),
        }
    }

    pub fn add_partition(&mut self, partition_id: PartitionId, graph_store: Arc<GraphStore>) {
        Arc::get_mut(&mut self.graph_partitions).unwrap().insert(partition_id, graph_store);
    }

    pub fn update_partition_routing(&mut self, partition_id: PartitionId, server_id: u32) {
        self.partition_to_server.insert(partition_id, server_id);
    }

    fn get_limit(raw_limit: usize) -> usize {
        if raw_limit > 0 {
            raw_limit
        } else {
            usize::max_value()
        }
    }

    fn parse_condition(condition: Option<&Condition>) -> Option<&maxgraph_store::db::api::condition::Condition> {
        None
    }

    fn parse_property_id(prop_ids: Option<&Vec<PropId>>) -> Option<&Vec<PropertyId>> {
        None
    }
}

impl GlobalGraphQuery for GlobalGraph {
    type V = RocksVertexImpl;
    type E = RocksEdgeImpl;
    type VI = Box<dyn Iterator<Item=RocksVertexImpl> + Send>;
    type EI = Box<dyn Iterator<Item=RocksEdgeImpl> + Send>;

    fn get_out_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                          dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>> {
        let res = self.get_out_edges(si, src_ids, edge_labels, condition, dedup_prop_ids, None, limit).map(|(v, ei)| {
            let vi: Self::VI = Box::new(ei.map(|e| {
                let out_v_id = e.get_edge_id().get_dst_vertex_id();
                let out_v_label = e.get_edge_relation().get_dst_vertex_label_id();
                RocksVertexImpl::new(out_v_id, out_v_label, None, RawBytes::empty())
            }));
            (v, vi)
        });
        Box::new(res)
    }

    fn get_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                     dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let mut res: Vec<(VertexId, Self::EI)>  = Vec::new();
        for (partition_id, vertex_ids) in src_ids {
            if let Some(store) = self.graph_partitions.get(&partition_id) {
                for vertex_id in vertex_ids {
                    let mut vertex_out_edges: Records<RocksEdgeImpl> = Box::new(::std::iter::empty());
                    if edge_labels.is_empty() {
                        vertex_out_edges = Box::new(vertex_out_edges.chain(store.get_out_edges(si, vertex_id, None, None, None).unwrap()))
                    } else {
                        for edge_label in edge_labels {
                            vertex_out_edges = Box::new(vertex_out_edges.chain(store.get_out_edges(si, vertex_id, Some(*edge_label as i32), None, None).unwrap()))
                        }
                    }
                    res.push((vertex_id, Box::new(vertex_out_edges.map(|e| e.unwrap()).take(Self::get_limit(limit)))));
                }
            }
        }
        Box::new(res.into_iter())
    }

    fn get_in_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                         dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(i64, Self::VI)>> {
        let res = self.get_in_edges(si, src_ids, edge_labels, condition, dedup_prop_ids, None, limit).map(|(v, ei)| {
            let vi: Self::VI = Box::new(ei.map(|e| {
                let in_v_id = e.get_edge_id().get_src_vertex_id();
                let in_v_label = e.get_edge_relation().get_src_vertex_label_id();
                RocksVertexImpl::new(in_v_id, in_v_label, None, RawBytes::empty())
            }));
            (v, vi)
        });
        Box::new(res)
    }

    fn get_in_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                    dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let mut res: Vec<(VertexId, Self::EI)> = Vec::new();
        for (partition_id, vertex_ids) in src_ids {
            if let Some(store) = self.graph_partitions.get(&partition_id) {
                for vertex_id in vertex_ids {
                    let mut vertex_in_edges: Records<RocksEdgeImpl> = Box::new(::std::iter::empty());
                    if edge_labels.is_empty() {
                        vertex_in_edges = Box::new(vertex_in_edges.chain(store.get_in_edges(si, vertex_id, None, None, None).unwrap()))
                    } else {
                        for edge_label in edge_labels {
                            vertex_in_edges = Box::new(vertex_in_edges.chain(store.get_in_edges(si, vertex_id, Some(*edge_label as i32), None, None).unwrap()))
                        }
                    }
                    res.push((vertex_id, Box::new(vertex_in_edges.map(|e| e.unwrap()).take(Self::get_limit(limit)))));
                }
            }
        }
        Box::new(res.into_iter())
    }

    fn count_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(i64, usize)>> {
        Box::new(self.get_in_edges(si, src_ids, edge_labels, condition, None, None, 0)
            .map(|(vertex_id, ei)| (vertex_id, ei.count())))
    }

    fn count_in_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(i64, usize)>> {
        Box::new(self.get_out_edges(si, src_ids, edge_labels, condition, None, None, 0)
            .map(|(vertex_id, ei)| (vertex_id, ei.count())))
    }

    fn get_vertex_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>, output_prop_ids: Option<&Vec<PropId>>) -> Self::VI {
        let graph_partitions = self.graph_partitions.clone();
        Box::new(ids.into_iter().flat_map(move |(partition_id, label_id_vec)| {
            let graph_partitions = graph_partitions.clone();
            label_id_vec.into_iter().flat_map(move |(label_id, vids)| {
                let graph_partitions = graph_partitions.clone();
                vids.into_iter().filter_map(move |vid| {
                    match graph_partitions.get(&partition_id) {
                        None => None,
                        Some(partition) => {
                            partition.get_vertex(si, vid, label_id.map(|l| l as i32), None).unwrap()
                        }
                    }
                })
            })
        }))
    }

    fn get_edge_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>,  output_prop_ids: Option<&Vec<PropId>>)
        -> Self::EI {
        unimplemented!()
    }

    fn get_all_vertices(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>,
                        output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::VI {
        let condition = Self::parse_condition(condition);
        let output_property_ids = Self::parse_property_id(output_prop_ids);
        let partitions = if partition_ids.is_empty() {
            self.graph_partitions.keys().map(|k| *k).collect_vec()
        } else {
            partition_ids.clone()
        };
        let mut res: Self::VI = Box::new(::std::iter::empty());
        for pid in partitions {
            if let Some(partition) = self.graph_partitions.get(&pid) {
                if labels.is_empty() {
                    res = Box::new(res.chain(partition.scan_vertex(si,
                                          None,
                                          condition,
                                          output_property_ids).unwrap()
                        .take(Self::get_limit(limit))
                        .map(|v| v.unwrap())))
                } else {
                    for label in labels {
                        res = Box::new(res.chain(partition.scan_vertex(si,
                                                                       Some(*label as i32),
                                                                       condition,
                                                                       output_property_ids).unwrap()
                            .take(Self::get_limit(limit))
                            .map(|v| v.unwrap())))
                    }
                }
            }
        }
        res
    }

    fn get_all_edges(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>,
                     output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::EI {
        let condition = Self::parse_condition(condition);
        let output_property_ids = Self::parse_property_id(output_prop_ids);
        let partitions = if partition_ids.is_empty() {
            self.graph_partitions.keys().map(|k| *k).collect_vec()
        } else {
            partition_ids.clone()
        };
        let mut res: Self::EI = Box::new(::std::iter::empty());
        for pid in partitions {
            if let Some(partition) = self.graph_partitions.get(&pid) {
                if labels.is_empty() {
                    res = Box::new(
                        res.chain(partition.scan_edge(si,
                                                      None,
                                                      condition,
                                                      output_property_ids).unwrap()
                            .take(Self::get_limit(limit))
                            .map(|e| e.unwrap())))
                } else {
                    for label in labels {
                        res = Box::new(
                            res.chain(partition.scan_edge(si,
                                                          Some(*label as i32),
                                                          condition,
                                                          output_property_ids).unwrap()
                                .take(Self::get_limit(limit))
                                .map(|e| e.unwrap())))
                    }
                }
            }
        }
        res
    }

    fn count_all_vertices(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        self.get_all_vertices(si, labels, condition, None, None, 0, partition_ids).count() as u64
    }

    fn count_all_edges(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        self.get_all_edges(si, labels, condition, None, None, 0, partition_ids).count() as u64
    }

    fn translate_vertex_id(&self, vertex_id: VertexId) -> VertexId {
        vertex_id
    }

    fn get_schema(&self, si: i64) -> Option<Arc<dyn Schema>> {
        let partition = self.graph_partitions.values().nth(0)?;
        let graph_def = partition.get_graph_def().ok()?;
        Some(Arc::new(GlobalGraphSchema::new(graph_def)))
    }
}

impl GraphPartitionManager for GlobalGraph {
    fn get_partition_id(&self, vid: i64) -> i32 {
        let partition_count = self.total_partition;
        floor_mod(vid, partition_count as i64) as i32
    }

    fn get_server_id(&self, partition_id: u32) -> Option<u32> {
        self.partition_to_server.get(&partition_id).map(|x| *x)
    }

    fn get_process_partition_list(&self) -> Vec<u32> {
        self.graph_partitions.keys().into_iter().map(|x|*x).collect::<Vec<u32>>()
    }

    fn get_vertex_id_by_primary_key(&self, label_id: u32, key: &String) -> Option<(u32, i64)> {
        // TODO check
        None
    }
}

fn floor_div(x: i64, y: i64) -> i64 {
    let mut r = x / y;
// if the signs are different and modulo not zero, round down
    if (x ^ y) < 0 && (r * y != x) {
        r = r - 1;
    }
    r
}

fn floor_mod(x: i64, y: i64) -> i64 {
    x - floor_div(x, y) * y
}
