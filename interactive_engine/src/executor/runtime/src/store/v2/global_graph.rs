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


use maxgraph_store::db::api::{Vertex, Edge, GraphStorage, GraphResult, EdgeResultIter, PropIter, ValueRef, ValueType, EdgeDirection, VertexWrapper, VertexResultIter, GraphConfigBuilder, GraphConfig, GraphDef};
use std::sync::Arc;
use maxgraph_store::api::{GlobalGraphQuery, SnapshotId, PartitionVertexIds, LabelId, Condition, PropId, VertexId, PartitionId, PartitionLabeledVertexIds};
use maxgraph_store::db::graph::vertex::VertexImpl;
use maxgraph_store::db::graph::edge::EdgeImpl;
use store::v2::edge_iterator::EdgeIterator;
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
use store::v2::global_graph_schema::GlobalGraphSchema;

pub struct GlobalGraph {
    graph_partitions: HashMap<PartitionId, Arc<GraphStore>>,
    total_partition: u32,
    partition_to_server: HashMap<PartitionId, u32>,
}

unsafe impl Send for GlobalGraph {}
unsafe impl Sync for GlobalGraph {}

impl GlobalGraph {
    pub fn empty(total_partition: u32) -> Self {
        GlobalGraph {
            graph_partitions: HashMap::new(),
            total_partition,
            partition_to_server: HashMap::new(),
        }
    }

    pub fn add_partition(&mut self, partition_id: PartitionId, graph_store: Arc<GraphStore>) {
        self.graph_partitions.insert(partition_id, graph_store);
    }

    pub fn update_partition_routing(&mut self, partition_id: PartitionId, server_id: u32) {
        self.partition_to_server.insert(partition_id, server_id);
    }

    fn convert_label_id(label_id: Option<LabelId>) -> Option<i32> {
        match label_id {
            None => {None},
            Some(u_label_id) => { Some(u_label_id as i32) },
        }
    }

    fn convert_condition(condition: Option<&Condition>) -> Option<Arc<maxgraph_store::db::api::Condition>> {
        match condition {
            None => {None},
            Some(_) => {unimplemented!()},
        }
    }

    fn parse_vertex<V: Vertex>(vertex_wrapper: V, output_prop_ids: Option<&Vec<PropId>>)
        -> LocalStoreVertex {
        let mut vertex = LocalStoreVertex::new(vertex_wrapper.get_id(), vertex_wrapper.get_label() as u32);
        let mut property_iter = vertex_wrapper.get_properties_iter();
        let prop_set;
        let prop_filter = if let Some(prop_ids) = output_prop_ids {
            prop_set = HashSet::<&u32>::from_iter(prop_ids);
            Some(&prop_set)
        } else {
            None
        };
        while let Some((property_id, value_ref)) = property_iter.next() {
            if let Some(filter) = prop_filter {
                if !filter.contains(&(property_id as u32)) {
                    continue;
                }
            }
            vertex.add_property(property_id as u32, Self::parse_val_ref(value_ref).unwrap());
        }
        vertex
    }

    fn parse_edge<EE: Edge>(item: EE, output_prop_ids: Option<&Vec<PropId>>) -> LocalStoreEdge {
        let src_vertex = LocalStoreVertex::new(item.get_src_id(), item.get_kind().src_vertex_label_id as u32);
        let dst_vertex = LocalStoreVertex::new(item.get_dst_id(), item.get_kind().dst_vertex_label_id as u32);
        let mut edge = LocalStoreEdge::new(src_vertex, dst_vertex, item.get_kind().edge_label_id as u32, item.get_id().inner_id);
        let mut property_iter = item.get_properties_iter();
        let prop_set;
        let prop_filter = if let Some(prop_ids) = output_prop_ids {
            prop_set = HashSet::<&u32>::from_iter(prop_ids);
            Some(&prop_set)
        } else {
            None
        };
        while let Some((property_id, value_ref)) = property_iter.next() {
            if let Some(filter) = prop_filter {
                if !filter.contains(&(property_id as u32)) {
                    continue;
                }
            }
            edge.add_property(property_id as u32, Self::parse_val_ref(value_ref).unwrap());
        }
        return edge;
    }

    fn parse_val_ref(val_ref: ValueRef) -> GraphResult<Property> {
        let p = match val_ref.get_type() {
            ValueType::Bool => {
                Property::Bool(val_ref.get_bool()?)
            },
            ValueType::Char => {
                Property::Char(val_ref.get_char()?)
            },
            ValueType::Short => {
                Property::Short(val_ref.get_short()?)
            },
            ValueType::Int => {
                Property::Int(val_ref.get_int()?)
            },
            ValueType::Long => {
                Property::Long(val_ref.get_long()?)
            },
            ValueType::Float => {
                Property::Float(val_ref.get_float()?)
            },
            ValueType::Double => {
                Property::Double(val_ref.get_double()?)
            },
            ValueType::String => {
                Property::String(String::from(val_ref.get_str()?))
            },
            ValueType::Bytes => {
                Property::Bytes(Vec::from(val_ref.get_bytes()?))
            },
            ValueType::IntList => {
                Property::ListInt(val_ref.get_int_list()?.iter().collect())
            },
            ValueType::LongList => {
                Property::ListLong(val_ref.get_long_list()?.iter().collect())
            },
            ValueType::FloatList => {
                Property::ListFloat(val_ref.get_float_list()?.iter().collect())
            },
            ValueType::DoubleList => {
                Property::ListDouble(val_ref.get_double_list()?.iter().collect())
            },
            ValueType::StringList => {
                Property::ListString(val_ref.get_str_list()?.iter().map(String::from).collect())
            },
        };
        Ok(p)
    }

    fn get_edges_iter<'a>(&'a self, si: SnapshotId, partition_id: PartitionId, src_id: VertexId, label: Option<LabelId>,
                          condition: Option<&Condition>, direction: EdgeDirection) -> GraphResult<Option<Box<dyn EdgeResultIter<E=EdgeImpl> + 'a>>> {
        Ok(match self.graph_partitions.get(&partition_id) {
            None => {
                None
            },
            Some(partition) => {
                match direction {
                    EdgeDirection::In => {
                        Some(partition.get_in_edges(si, src_id, Self::convert_label_id(label), Self::convert_condition(condition))?)
                    },
                    EdgeDirection::Out => {
                        Some(partition.get_out_edges(si, src_id, Self::convert_label_id(label), Self::convert_condition(condition))?)
                    },
                    EdgeDirection::Both => {
                        unimplemented!()
                    },
                }
            },
        })
    }

    fn get_vertex(&self, si: SnapshotId, partition_id: PartitionId, id: VertexId, label_id: Option<LabelId>)
        -> GraphResult<Option<VertexWrapper<VertexImpl>>> {
        Ok(match self.graph_partitions.get(&partition_id) {
            None => {
                None
            },
            Some(partition) => {
                partition.get_vertex(si, id, Self::convert_label_id(label_id))?
            },
        })
    }

    fn get_edge_iter_vec<'a>(&'a self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>,
                             condition: Option<&Condition>, direction: EdgeDirection)
        -> GraphResult<Vec<(i64, Vec<Box<dyn EdgeResultIter<E=EdgeImpl> + 'a>>)>> {
        let mut res = vec![];
        for (partition_id, vertex_ids) in src_ids {
            for vertex_id in vertex_ids {
                let mut edge_iters = vec![];
                if edge_labels.is_empty() {
                    if let Some(iter_res) = self.get_edges_iter(si, partition_id, vertex_id, None, condition, direction)? {
                        edge_iters.push(iter_res);
                    };
                } else {
                    for label_id in edge_labels {
                        if let Some(iter_res) = self.get_edges_iter(si, partition_id, vertex_id, Some(*label_id), condition, direction)? {
                            edge_iters.push(iter_res);
                        }
                    }
                }
                res.push((vertex_id, edge_iters));
            }
        }
        Ok(res)
    }

    fn scan_vertex_iter<'a>(&'a self, si: SnapshotId, partition_id: PartitionId, label: Option<LabelId>, condition: Option<&Condition>)
                            -> GraphResult<Option<Box<dyn VertexResultIter<V=VertexImpl> + 'a>>> {
        Ok(match self.graph_partitions.get(&partition_id) {
            None => {
                None
            },
            Some(partition) => {
                Some(partition.query_vertices(si, Self::convert_label_id(label), Self::convert_condition(condition))?)
            },
        })
    }

    fn scan_vertex_iter_vec<'a>(&'a self, si: SnapshotId, labels: &Vec<LabelId>, partitions: &Vec<PartitionId>, condition: Option<&Condition>)
                                -> GraphResult<Vec<Box<dyn VertexResultIter<V=VertexImpl> + 'a>>> {
        let mut res = vec![];
        let partition_ids = if partitions.is_empty() {
            self.graph_partitions.keys().map(|x| *x).collect_vec()
        } else {
            partitions.clone()
        };
        for partition_id in partition_ids {
            if labels.is_empty() {
                if let Some(iter) = self.scan_vertex_iter(si, partition_id, None, condition)? {
                    res.push(iter);
                }
            } else {
                for label_id in labels {
                    if let Some(iter) = self.scan_vertex_iter(si, partition_id, Some(*label_id), condition)? {
                        res.push(iter);
                    }
                }
            }
        }
        Ok(res)
    }

    fn scan_edge_iter<'a>(&'a self, si: SnapshotId, partition_id: PartitionId, label: Option<LabelId>, condition: Option<&Condition>)
                            -> GraphResult<Option<Box<dyn EdgeResultIter<E=EdgeImpl> + 'a>>> {
        Ok(match self.graph_partitions.get(&partition_id) {
            None => {
                None
            },
            Some(partition) => {
                Some(partition.query_edges(si, Self::convert_label_id(label), Self::convert_condition(condition))?)
            },
        })
    }

    fn scan_edge_iter_vec<'a>(&'a self, si: SnapshotId, labels: &Vec<LabelId>, partitions: &Vec<PartitionId>, condition: Option<&Condition>)
                                -> GraphResult<Vec<Box<dyn EdgeResultIter<E=EdgeImpl> + 'a>>> {
        let mut res = vec![];
        let partition_ids = if partitions.is_empty() {
            self.graph_partitions.keys().map(|x| *x).collect_vec()
        } else {
            partitions.clone()
        };
        for partition_id in partition_ids {
            if labels.is_empty() {
                if let Some(iter) = self.scan_edge_iter(si, partition_id, None, condition)? {
                    res.push(iter);
                }
            } else {
                for label_id in labels {
                    if let Some(iter) = self.scan_edge_iter(si, partition_id, Some(*label_id), condition)? {
                        res.push(iter);
                    }
                }
            }
        }
        Ok(res)
    }

    fn get_limit(raw_limit: usize) -> usize {
        if raw_limit > 0 {
            raw_limit
        } else {
            usize::max_value()
        }
    }
}

impl GlobalGraphQuery for GlobalGraph {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = IntoIter<LocalStoreVertex>;
    type EI = IntoIter<LocalStoreEdge>;

    fn get_out_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                          dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition, EdgeDirection::Out).unwrap();
        Box::new(
            res.into_iter().map(|(vertex_id, edge_iter_vec)|
                (
                    vertex_id,
                    EdgeIterator::new(&edge_iter_vec)
                        .map(|item| LocalStoreVertex::new(item.get_dst_id(), item.get_kind().dst_vertex_label_id as u32))
                        .take(Self::get_limit(limit))
                        .collect::<Vec<LocalStoreVertex>>().into_iter()
                )
            ).collect::<Vec<(i64, IntoIter<LocalStoreVertex>)>>().into_iter()
        )
    }

    fn get_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                     dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition, EdgeDirection::Out).unwrap();
        Box::new(res.into_iter().map(|(vertex_id, edge_iter_vec)| {
            (
                vertex_id,
                EdgeIterator::new(&edge_iter_vec)
                    .map(|e| Self::parse_edge(e, output_prop_ids))
                    .take(Self::get_limit(limit))
                    .collect::<Vec<LocalStoreEdge>>()
                    .into_iter()
            )
        }).collect::<Vec<(i64, IntoIter<LocalStoreEdge>)>>().into_iter())
    }

    fn get_in_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                         dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(i64, Self::VI)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition, EdgeDirection::In).unwrap();
        Box::new(
            res.into_iter().map(|(vertex_id, edge_iter_vec)|
                (
                    vertex_id,
                    EdgeIterator::new(&edge_iter_vec)
                        .map(|item| LocalStoreVertex::new(item.get_src_id(), item.get_kind().src_vertex_label_id as u32))
                        .take(Self::get_limit(limit))
                        .collect::<Vec<LocalStoreVertex>>().into_iter()
                )
            ).collect::<Vec<(i64, IntoIter<LocalStoreVertex>)>>().into_iter()
        )
    }

    fn get_in_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                    dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition, EdgeDirection::In).unwrap();
        Box::new(res.into_iter().map(|(vertex_id, edge_iter_vec)| {
            (
                vertex_id,
                EdgeIterator::new(&edge_iter_vec)
                    .map(|e| Self::parse_edge(e, output_prop_ids))
                    .take(Self::get_limit(limit))
                    .collect::<Vec<LocalStoreEdge>>()
                    .into_iter()
            )
        }).collect::<Vec<(i64, IntoIter<LocalStoreEdge>)>>().into_iter())
    }

    fn count_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(i64, usize)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition, EdgeDirection::Out).unwrap();
        Box::new(res.into_iter().map(|(vertex_id, edge_iter_vec)| {
            (
                vertex_id,
                EdgeIterator::new(&edge_iter_vec).count()
            )
        }).collect::<Vec<(i64, usize)>>().into_iter())
    }

    fn count_in_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(i64, usize)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition, EdgeDirection::In).unwrap();
        Box::new(res.into_iter().map(|(vertex_id, edge_iter_vec)| {
            (
                vertex_id,
                EdgeIterator::new(&edge_iter_vec).count()
            )
        }).collect::<Vec<(i64, usize)>>().into_iter())
    }

    fn get_vertex_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>, output_prop_ids: Option<&Vec<PropId>>) -> Self::VI {
        let mut id_iter = ids.into_iter().flat_map(move |(partition, label_id_vec)| {
            label_id_vec.into_iter().flat_map(move |(label_id, ids)| {
                ids.into_iter().map(move |id| {
                    (partition, label_id, id)
                })
            })
        });
        let mut res = vec![];
        while let Some((partition, label_id, id)) = id_iter.next() {
            if let Some(v) = self.get_vertex(si, partition, id, label_id).unwrap() {
                res.push(Self::parse_vertex(v, output_prop_ids));
            }
        }
        return res.into_iter();
    }

    fn get_edge_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>,  output_prop_ids: Option<&Vec<PropId>>)
        -> Self::EI {
        unimplemented!()
    }

    fn get_all_vertices(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>,
                        output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::VI {
        let iter_vec = self.scan_vertex_iter_vec(si, labels, partition_ids, condition).unwrap();
        let real_limit = Self::get_limit(limit);
        let mut res = vec![];
        for mut iter in iter_vec {
            while let Some(v) = iter.next() {
                res.push(Self::parse_vertex(v, output_prop_ids));
                if res.len() >= real_limit {
                    return res.into_iter();
                }
            }
        }
        return res.into_iter();
    }

    fn get_all_edges(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>,
                     output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::EI {
        let iter_vec = self.scan_edge_iter_vec(si, labels, partition_ids, condition).unwrap();
        let real_limit = Self::get_limit(limit);
        let mut res = vec![];
        for mut iter in iter_vec {
            while let Some(e) = iter.next() {
                res.push(Self::parse_edge(e, output_prop_ids));
                if res.len() >= real_limit {
                    return res.into_iter();
                }
            }
        }
        return res.into_iter();
    }

    fn count_all_vertices(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        let iter_vec = self.scan_vertex_iter_vec(si, labels, partition_ids, condition).unwrap();
        let mut count = 0;
        for mut iter in iter_vec {
            while let Some(v) = iter.next() {
                count += 1;
            }
        }
        return count;
    }

    fn count_all_edges(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        let iter_vec = self.scan_edge_iter_vec(si, labels, partition_ids, condition).unwrap();
        let mut count = 0;
        for mut iter in iter_vec {
            while let Some(v) = iter.next() {
                count += 1;
            }
        }
        return count;
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
