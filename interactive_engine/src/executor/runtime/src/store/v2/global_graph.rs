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


use maxgraph_store::db::api::{Vertex, Edge, GraphStorage, GraphResult, EdgeResultIter, PropIter, ValueRef, ValueType};
use std::sync::Arc;
use maxgraph_store::api::{GlobalGraphQuery, SnapshotId, PartitionVertexIds, LabelId, Condition, PropId, VertexId, PartitionId};
use maxgraph_store::db::graph::vertex::VertexImpl;
use maxgraph_store::db::graph::edge::EdgeImpl;
use store::v2::global_vertex_iterator::GlobalVertexIteratorImpl;
use store::v2::global_edge_iterator::GlobalEdgeIteratorImpl;
use store::v2::edge_iterator::EdgeIterator;
use store::{LocalStoreVertex, LocalStoreEdge};
use maxgraph_store::api::graph_schema::Schema;
use std::collections::HashMap;
use std::vec::IntoIter;
use maxgraph_store::api::prelude::Property;

pub struct GlobalGraph<V, E>
    where V: 'static + Vertex,
          E: 'static + Edge {
    graph_partitions: HashMap<PartitionId, Arc<dyn GraphStorage<V=V, E=E>>>,
}

unsafe impl<V, E> Send for GlobalGraph<V, E> where V: 'static + Vertex, E: 'static + Edge {}
unsafe impl<V, E> Sync for GlobalGraph<V, E> where V: 'static + Vertex, E: 'static + Edge {}

impl<V, E> GlobalGraph<V, E>
    where V: 'static + Vertex,
          E: 'static + Edge {

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

    fn get_out_edges<'a>(&'a self, si: SnapshotId, partition_id: PartitionId, src_id: VertexId, label: Option<LabelId>,
                         condition: Option<&Condition>) -> Option<GraphResult<Box<dyn EdgeResultIter<E=E> + 'a>>> {
        match self.graph_partitions.get(&partition_id) {
            None => {
                None
            },
            Some(partition) => {
                Some(partition.get_out_edges(si, src_id, Self::convert_label_id(label), Self::convert_condition(condition)))
            },
        }
    }

    fn get_edge_iter_vec<'a>(&'a self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>,
                             condition: Option<&Condition>) -> Vec<(i64, Vec<Box<dyn EdgeResultIter<E=E> + 'a>>)> {
        let mut res = vec![];
        for (partition_id, vertex_ids) in src_ids {
            for vertex_id in vertex_ids {
                let mut edge_iters = vec![];
                if edge_labels.is_empty() {
                    if let Some(iter_res) = self.get_out_edges(si, partition_id, vertex_id, None, condition) {
                        edge_iters.push(iter_res.unwrap());
                    };
                } else {
                    for label_id in edge_labels {
                        if let Some(iter_res) = self.get_out_edges(si, partition_id, vertex_id, Some(*label_id), condition) {
                            edge_iters.push(iter_res.unwrap());
                        }
                    }
                }
                res.push((vertex_id, edge_iters));
            }
        }
        res
    }
}

impl<V, E> GlobalGraphQuery for GlobalGraph<V, E>
    where V: 'static + Vertex,
          E: 'static + Edge {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = IntoIter<LocalStoreVertex>;
    type EI = IntoIter<LocalStoreEdge>;

    fn get_out_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                          dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition);
        Box::new(
            res.into_iter().map(|(vertex_id, edge_iter_vec)|
                (
                    vertex_id,
                    EdgeIterator::new(edge_iter_vec)
                         .map(|item| LocalStoreVertex::new(item.get_dst_id(), item.get_kind().dst_vertex_label_id as u32))
                         .collect::<Vec<LocalStoreVertex>>().into_iter()
                )
            ).collect::<Vec<(i64, IntoIter<LocalStoreVertex>)>>().into_iter()
        )
    }

    fn get_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>,
                     dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let res = self.get_edge_iter_vec(si, src_ids, edge_labels, condition);
        Box::new(res.into_iter().map(|(vertex_id, edge_iter_vec)| {
            (
                vertex_id,
                EdgeIterator::new(edge_iter_vec).map(|item| {
                    let src_vertex = LocalStoreVertex::new(item.get_src_id(), item.get_kind().src_vertex_label_id as u32);
                    let dst_vertex = LocalStoreVertex::new(item.get_dst_id(), item.get_kind().dst_vertex_label_id as u32);
                    let mut edge = LocalStoreEdge::new(src_vertex, dst_vertex, item.get_kind().edge_label_id as u32, item.get_id().inner_id);
                    let mut property_iter = item.get_properties_iter();
                    while let Some((property_id, value_ref)) = property_iter.next() {
                        edge.add_property(property_id as u32, Self::parse_val_ref(value_ref).unwrap());
                    }
                    return edge;
                }).collect::<Vec<LocalStoreEdge>>().into_iter()
            )
        }).collect::<Vec<(i64, IntoIter<LocalStoreEdge>)>>().into_iter())
    }

    fn get_in_vertex_ids(&self, si: i64, dst_ids: Vec<(u32, Vec<i64>)>, edge_labels: &Vec<u32>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<u32>>, limit: usize) -> Box<dyn Iterator<Item=(i64, Self::VI)>> {
        unimplemented!()
    }

    fn get_in_edges(&self, si: i64, dst_ids: Vec<(u32, Vec<i64>)>, edge_labels: &Vec<u32>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>, limit: usize) -> Box<dyn Iterator<Item=(i64, Self::EI)>> {
        unimplemented!()
    }

    fn count_out_edges(&self, si: i64, src_ids: Vec<(u32, Vec<i64>)>, edge_labels: &Vec<u32>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(i64, usize)>> {
        unimplemented!()
    }

    fn count_in_edges(&self, si: i64, dst_ids: Vec<(u32, Vec<i64>)>, edge_labels: &Vec<u32>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(i64, usize)>> {
        unimplemented!()
    }

    fn get_vertex_properties(&self, si: i64, ids: Vec<(u32, Vec<(Option<u32>, Vec<i64>)>)>, output_prop_ids: Option<&Vec<u32>>) -> Self::VI {
        unimplemented!()
    }

    fn get_edge_properties(&self, si: i64, ids: Vec<(u32, Vec<(Option<u32>, Vec<i64>)>)>, output_prop_ids: Option<&Vec<u32>>) -> Self::EI {
        unimplemented!()
    }

    fn get_all_vertices(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>, limit: usize, partition_ids: &Vec<u32>) -> Self::VI {
        unimplemented!()
    }

    fn get_all_edges(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<u32>>, output_prop_ids: Option<&Vec<u32>>, limit: usize, partition_ids: &Vec<u32>) -> Self::EI {
        unimplemented!()
    }

    fn count_all_vertices(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        unimplemented!()
    }

    fn count_all_edges(&self, si: i64, labels: &Vec<u32>, condition: Option<&Condition>, partition_ids: &Vec<u32>) -> u64 {
        unimplemented!()
    }

    fn translate_vertex_id(&self, vertex_id: i64) -> i64 {
        unimplemented!()
    }

    fn get_schema(&self, si: i64) -> Option<Arc<dyn Schema>> {
        unimplemented!()
    }
}

