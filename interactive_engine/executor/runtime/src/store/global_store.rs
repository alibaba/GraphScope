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

use maxgraph_store::api::*;
use maxgraph_store::schema::prelude::Relation;
use maxgraph_store::api::prelude::Property;
use maxgraph_common::proto::store_api::{GetVertexsRequest, BatchVertexEdgeRequest, BatchVertexEdgeResponse, BatchVertexCountResponse, GetOutEdgesRequest, GetInEdgesRequest};

use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraExtendEntity};
use dataflow::manager::filter::FilterManager;
use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::dedup::DedupManager;
use dataflow::builder::MessageCollector;

use store::remote_store_service::RemoteStoreServiceManager;

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::cell::RefCell;
use futures::stream::Stream;
use futures::future::Future;
use tokio_core::reactor::Core;
use store::utils::parse_property_entity_list;
use maxgraph_common::proto::gremlin_query;
use grpcio::ClientSStreamReceiver;
use maxgraph_common::proto::message::LogicalCompare;
use protobuf::RepeatedField;
use store::{LocalStoreVertex, LocalStoreEdge};
use alloc::vec::IntoIter;
use itertools::Itertools;
use maxgraph_common::proto::remote_api::VerticesRequest;
use maxgraph_common::proto::message::EdgeDirection;
use dataflow::common::iterator::{IteratorList, LocalStoreVertexIterator, LocalStoreEdgeIterator, GlobalStoreAdjVertexIdResultIterator, GlobalStoreAdjEdgeResultIterator};
use store::global_schema::LocalGraphSchema;

pub struct GlobalStore<V, VI, E, EI>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    store_service: Arc<RemoteStoreServiceManager>,
    graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
    local_opt_flag: bool,
}

impl<V, VI, E, EI> Clone for GlobalStore<V, VI, E, EI>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    fn clone(&self) -> Self {
        GlobalStore {
            store_service: self.store_service.clone(),
            graph: self.graph.clone(),
            local_opt_flag: self.local_opt_flag.clone(),
        }
    }
}

impl<V, VI, E, EI> GlobalStore<V, VI, E, EI>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    pub fn new(store_service: Arc<RemoteStoreServiceManager>,
               graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
               local_opt_flag: bool, ) -> Self {
        GlobalStore {
            store_service,
            graph,
            local_opt_flag,
        }
    }

    fn get_process_index(&self, vid: i64) -> i64 {
        let partition_id = self.graph.as_ref().get_partition_id(vid);
        return self.get_process_index_by_partition_id(partition_id);
    }

    fn get_process_index_by_partition_id(&self, partition_id: u32) -> i64 {
        if let Some(process_index) = self.store_service.as_ref().get_partition_process_list().get(&partition_id) {
            *process_index as i64
        } else {
            0
        }
    }

    fn process_partitions(&self, partition_ids: &Vec<u32>, worker_partition_map: &mut HashMap<i64, Vec<u32>>) {
        if !partition_ids.is_empty() {
            let partition_process_list = self.store_service.as_ref().get_partition_process_list();
            for partition_id in partition_ids {
                if let Some(process_index) = partition_process_list.get(partition_id) {
                    worker_partition_map.entry(*process_index as i64).or_insert(vec![]).push(*partition_id);
                }
            }
        }
    }

    fn get_local_out_iter<'a>(&self,
                              si: i64,
                              src_id: i64,
                              label_list: &Vec<u32>,
                              condition: Option<&Condition>,
                              partition_id: u32,
                              out_list: &mut Vec<EI>, ) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if label_list.is_empty() {
                let edge_iter = partition.get_out_edges(si, src_id, None);
                out_list.push(edge_iter);
            } else {
                for label_id in label_list.iter() {
                    let edge_iter = partition.get_out_edges(si, src_id, Some(*label_id));
                    out_list.push(edge_iter);
                }
            }
        }
    }

    fn get_local_in_iter<'a>(&self,
                             si: i64,
                             dst_id: i64,
                             label_list: &Vec<u32>,
                             condition: Option<&Condition>,
                             partition_id: u32,
                             in_list: &mut Vec<EI>, ) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if label_list.is_empty() {
                let edge_iter = partition.get_in_edges(si, dst_id, None);
                in_list.push(edge_iter);
            } else {
                for label_id in label_list.iter() {
                    let edge_iter = partition.get_in_edges(si, dst_id, Some(*label_id));
                    in_list.push(edge_iter);
                }
            }
        }
    }

    fn get_local_out_count(&self,
                           si: i64,
                           src_id: i64,
                           label_list: &Vec<u32>,
                           condition: Option<&Condition>,
                           partition_id: u32,
                           vertex_count_list: &mut HashMap<i64, usize>) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            let curr_count = {
                if label_list.is_empty() {
                    partition
                        .get_out_edges(si, src_id, None)
                        .count()
                } else {
                    let mut labeled_count = 0;
                    for labelid in label_list.iter() {
                        labeled_count += partition
                            .get_out_edges(si, src_id, Some(*labelid))
                            .count();
                    }
                    labeled_count
                }
            };
            let count_val = vertex_count_list.entry(src_id).or_insert(0);
            *count_val += curr_count;
        } else {
            _info!("cannot get partition {:?}", partition_id);
        }
    }

    fn get_local_in_count(&self,
                          si: i64,
                          dst_id: i64,
                          label_list: &Vec<u32>,
                          condition: Option<&Condition>,
                          partition_id: u32,
                          vertex_count_list: &mut HashMap<i64, usize>) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            let curr_count = {
                if label_list.is_empty() {
                    partition
                        .get_in_edges(si, dst_id, None)
                        .count()
                } else {
                    let mut labeled_count = 0;
                    for labelid in label_list.iter() {
                        labeled_count += partition
                            .get_in_edges(si, dst_id, Some(*labelid))
                            .count();
                    }
                    labeled_count
                }
            };
            let count_val = vertex_count_list.entry(dst_id).or_insert(0);
            *count_val += curr_count;
        } else {
            _info!("cannot get partition {:?}", partition_id);
        }
    }

    fn get_local_vertex_property_iter(&self,
                                      si: i64,
                                      id: i64,
                                      label: Option<u32>,
                                      partition_id: u32,
                                      vertex_list: &mut Vec<V>, ) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if let Some(v) = partition.get_vertex(si, id, label) {
                vertex_list.push(v);
            }
        }
    }

    fn query_local_vertex_iter<'a>(&self,
                                   si: i64,
                                   label_list: &Vec<u32>,
                                   partition_id: u32,
                                   vertex_list: &mut Vec<VI>, ) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if label_list.is_empty() {
                vertex_list.push(partition.as_ref().scan(si, None));
            } else {
                for label_id in label_list.iter() {
                    vertex_list.push(partition.as_ref().scan(si, Some(*label_id)));
                }
            }
        }
    }

    fn query_local_edge_iter<'a>(&self,
                                 si: i64,
                                 label_list: &Vec<u32>,
                                 partition_id: u32,
                                 edge_list: &mut Vec<EI>, )
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if label_list.is_empty() {
                edge_list.push(partition.as_ref().scan_edges(si, None));
            } else {
                for label_id in label_list.iter() {
                    edge_list.push(partition.as_ref().scan_edges(si, Some(*label_id)));
                }
            }
        }
    }

    fn query_local_vertex_count<'a>(&self,
                                    si: i64,
                                    label_list: &Vec<u32>,
                                    condition: Option<&Condition>,
                                    partition_id: u32,
                                    vertex_count: &mut u64, ) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if label_list.is_empty() {
                *vertex_count += partition.as_ref().vertex_count();
            } else {
                for label_id in label_list.iter() {
                    *vertex_count += partition.as_ref().estimate_vertex_count(Some(*label_id));
                }
            }
        }
    }

    fn query_local_edge_count<'a>(&self,
                                  si: i64,
                                  label_list: &Vec<u32>,
                                  condition: Option<&Condition>,
                                  partition_id: u32,
                                  edge_count: &mut u64, ) {
        if let Some(partition) = self.graph.get_partition(partition_id) {
            if label_list.is_empty() {
                *edge_count += partition.as_ref().edge_count();
            } else {
                for label_id in label_list.iter() {
                    *edge_count += partition.as_ref().estimate_edge_count(Some(*label_id));
                }
            }
        }
    }
}

impl<V, VI, E, EI> GlobalGraphQuery for GlobalStore<V, VI, E, EI>
    where V: 'static + Vertex,
          VI: 'static + Iterator<Item=V>,
          E: 'static + Edge,
          EI: 'static + Iterator<Item=E> {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = LocalStoreVertexIterator<V, VI, E, EI>;
    type EI = LocalStoreEdgeIterator<E, EI>;

    fn get_out_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>> {
        let mut result = vec![];
        let mut all_out_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for (partition_id, ids) in src_ids {
            for src_id in ids {
                if self.local_opt_flag {
                    let mut out_list = all_out_list.entry(src_id).or_insert(vec![]);
                    self.get_local_out_iter(si, src_id, edge_labels, condition, partition_id, &mut out_list);
                } else {
                    let process_index = self.get_process_index_by_partition_id(partition_id);
                    self.store_service.as_ref().build_batch_in_out_request(si,
                                                                           edge_labels,
                                                                           &mut worker_req_list,
                                                                           src_id,
                                                                           process_index,
                                                                           condition,
                                                                           dedup_prop_ids,
                                                                           Some(&vec![]),
                                                                           limit,
                                                                           true,
                                                                           &vec![]);
                }
            }
        }
        let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                     |client, req| {
                                                                                         client.get_batch_out(req)
                                                                                     }, );

        //    self.store_service.as_ref().parse_out_vertices_result(&mut result_stream_list, self.graph.get_schema(si).unwrap(), &mut all_out_list);
        for (src_id, out_list) in all_out_list.drain() {
            let adj_iter = LocalStoreVertexIterator::from_getting_adj_vertex_ids(None, limit, None, IteratorList::new(out_list), Some(EdgeDirection::DIR_OUT));
            result.push((src_id, adj_iter));
        }
        return Box::new(GlobalStoreAdjVertexIdResultIterator::new(result.into_iter()));
    }

    fn get_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let mut result = vec![];
        let mut all_out_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for (partition_id, ids) in src_ids {
            for src_id in ids {
                if self.local_opt_flag {
                    let out_list = all_out_list.entry(src_id).or_insert(vec![]);
                    self.get_local_out_iter(si, src_id, edge_labels, condition, partition_id, out_list);
                } else {
                    let process_index = self.get_process_index_by_partition_id(partition_id);
                    self.store_service.as_ref().build_batch_in_out_request(si,
                                                                           edge_labels,
                                                                           &mut worker_req_list,
                                                                           src_id,
                                                                           process_index,
                                                                           condition,
                                                                           dedup_prop_ids,
                                                                           output_prop_ids,
                                                                           limit,
                                                                           false,
                                                                           &vec![]);
                }
            }
        }
        let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                     |client, req| {
                                                                                         client.get_batch_out(req)
                                                                                     }, );
        for (src_id, out_list) in all_out_list.drain() {
            let adj_iter = LocalStoreEdgeIterator::new(None, limit, None, output_prop_ids.cloned(), IteratorList::new(out_list));
            result.push((src_id, adj_iter));
        }
        return Box::new(GlobalStoreAdjEdgeResultIterator::new(result.into_iter()));
    }

    fn get_in_vertex_ids(&self, si: SnapshotId, dst_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>> {
        let mut result = vec![];
        let mut all_in_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for (partition_id, ids) in dst_ids {
            if self.local_opt_flag {
                for dst_id in ids {
                    let in_list = all_in_list.entry(dst_id).or_insert(vec![]);
                    self.get_local_in_iter(si, dst_id, edge_labels, condition, partition_id, in_list);
                }
            } else {
                let process_index = self.get_process_index_by_partition_id(partition_id);
                self.store_service.as_ref().build_batch_in_out_request_by_partition(si,
                                                                                    edge_labels,
                                                                                    &mut worker_req_list,
                                                                                    &ids,
                                                                                    process_index,
                                                                                    condition,
                                                                                    dedup_prop_ids,
                                                                                    Some(&vec![]),
                                                                                    limit,
                                                                                    true,
                                                                                    partition_id);
            }
        }
        let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                     |client, req| {
                                                                                         client.get_batch_in(req)
                                                                                     });
        //    self.store_service.as_ref().parse_in_vertices_result(&mut result_stream_list, self.graph.get_schema(si).unwrap(), &mut all_in_list);
        for (dst_id, in_list) in all_in_list.drain() {
            let adj_iter = LocalStoreVertexIterator::from_getting_adj_vertex_ids(None, limit, None, IteratorList::new(in_list), Some(EdgeDirection::DIR_IN));
            result.push((dst_id, adj_iter));
        }
        return Box::new(GlobalStoreAdjVertexIdResultIterator::new(result.into_iter()));
    }

    fn get_in_edges(&self, si: SnapshotId, dst_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>> {
        let mut result = vec![];
        let mut all_in_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for (partition_id, ids) in dst_ids {
            if self.local_opt_flag {
                for dst_id in ids {
                    let in_list = all_in_list.entry(dst_id).or_insert(vec![]);
                    self.get_local_in_iter(si, dst_id, edge_labels, condition, partition_id, in_list);
                }
            } else {
                let process_index = self.get_process_index_by_partition_id(partition_id);
                self.store_service.as_ref().build_batch_in_out_request_by_partition(si,
                                                                                    edge_labels,
                                                                                    &mut worker_req_list,
                                                                                    &ids,
                                                                                    process_index,
                                                                                    condition,
                                                                                    dedup_prop_ids,
                                                                                    Some(&vec![]),
                                                                                    limit,
                                                                                    true,
                                                                                    partition_id);
            }
        }
        let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                     |client, req| {
                                                                                         client.get_batch_in(req)
                                                                                     });
        //   self.store_service.as_ref().parse_in_edges_result(&mut result_stream_list, self.graph.get_schema(si).unwrap(), &mut all_in_list);
        for (dst_id, in_list) in all_in_list.drain() {
            let adj_iter = LocalStoreEdgeIterator::new(None, limit, None, output_prop_ids.cloned(), IteratorList::new(in_list));
            result.push((dst_id, adj_iter));
        }
        return Box::new(GlobalStoreAdjEdgeResultIterator::new(result.into_iter()));
    }

    fn count_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(VertexId, usize)>> {
        let mut result = vec![];
        let mut all_count_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for (partition_id, ids) in src_ids {
            for src_id in ids {
                if self.local_opt_flag {
                    self.get_local_out_count(si, src_id, edge_labels, condition, partition_id, &mut all_count_list);
                } else {
                    let process_index = self.get_process_index_by_partition_id(partition_id);
                    self.store_service.as_ref().build_batch_in_out_request(si,
                                                                           edge_labels,
                                                                           &mut worker_req_list,
                                                                           src_id,
                                                                           process_index,
                                                                           condition,
                                                                           None,
                                                                           Some(&vec![]),
                                                                           0,
                                                                           false,
                                                                           &vec![]);
                }
            }
        }
        let mut result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                         |client, req| {
                                                                                             client.get_batch_out_cnt(req)
                                                                                         }, );

        self.store_service.as_ref().parse_vertex_count_result(&mut result_stream_list, &mut all_count_list);
        for (id, count) in all_count_list.drain() {
            result.push((id, count));
        }
        return Box::new(result.into_iter());
    }

    fn count_in_edges(&self, si: SnapshotId, dst_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(VertexId, usize)>> {
        let mut result = vec![];
        let mut all_count_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for (partition_id, ids) in dst_ids {
            if self.local_opt_flag {
                for dst_id in ids {
                    self.get_local_in_count(si, dst_id, edge_labels, condition, partition_id, &mut all_count_list);
                }
            } else {
                let process_index = self.get_process_index_by_partition_id(partition_id);
                self.store_service.as_ref().build_batch_in_out_request_by_partition(si,
                                                                                    edge_labels,
                                                                                    &mut worker_req_list,
                                                                                    &ids,
                                                                                    process_index,
                                                                                    condition,
                                                                                    None,
                                                                                    Some(&vec![]),
                                                                                    0,
                                                                                    false,
                                                                                    partition_id);
            }
        }
        let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                         |client, req| {
                                                                                             client.get_batch_in_cnt(req)
                                                                                         });
        //  self.store_service.as_ref().parse_vertex_count_result(&mut result_stream_list, &mut all_count_list);
        for (id, count) in all_count_list.drain() {
            result.push((id, count));
        }
        return Box::new(result.into_iter());
    }

    fn get_vertex_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>, output_prop_ids: Option<&Vec<PropId>>) -> Self::VI {
        let mut result = vec![];
        let mut worker_req_list = HashMap::new();
        for (partition_id, labeled_ids) in ids {
            for (label, vids) in labeled_ids {
                for id in vids {
                    if self.local_opt_flag {
                        self.get_local_vertex_property_iter(si, id, label, partition_id, &mut result);
                    } else {
                        let process_index = self.get_process_index_by_partition_id(partition_id);
                        self.store_service.as_ref().build_get_vertices_request(si,
                                                                               &mut worker_req_list,
                                                                               id,
                                                                               label,
                                                                               process_index,
                                                                               output_prop_ids);
                    }
                }
            }
        }
        if !worker_req_list.is_empty() {
            let mut result_stream_list = Vec::with_capacity(worker_req_list.len());
            for (process_index, mut label_req_list) in worker_req_list.into_iter() {
                if let Some(client) = self.store_service.get_client_list().get(&process_index) {
                    for (_, req) in label_req_list.drain() {
                        if let Ok(vertex_stream) = client.get_client().get_vertices(&req) {
                            result_stream_list.push(vertex_stream);
                        }
                    }
                }
            }
            //   self.store_service.as_ref().parse_vertex_result(&mut result_stream_list, self.graph.get_schema(si).unwrap(), &mut result);
        }
        return LocalStoreVertexIterator::from_getting_vertex_properties(result.into_iter(), output_prop_ids.cloned());
    }

    fn get_edge_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>, output_prop_ids: Option<&Vec<PropId>>) -> Self::EI {
        unimplemented!()
    }

    fn get_all_vertices(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::VI {
        let mut result = vec![];
        let mut worker_req_list = HashMap::new();
        if self.local_opt_flag {
            let local_partition_list = if partition_ids.is_empty() {self.graph.get_partitions()} else {partition_ids.clone()};
            for partition_id in local_partition_list.iter() {
                self.query_local_vertex_iter(si, labels, *partition_id, &mut result);
            }
        } else {
            let mut worker_partition_list = HashMap::new();
            self.process_partitions(partition_ids, &mut worker_partition_list);
            for (process_index, partition_ids) in worker_partition_list.iter() {
                self.store_service.as_ref().build_query_request(si,
                                                                labels,
                                                                &mut worker_req_list,
                                                                *process_index,
                                                                condition,
                                                                dedup_prop_ids,
                                                                output_prop_ids,
                                                                limit,
                                                                partition_ids);
            }

            let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                         |client, req| {
                                                                                             client.query_vertices(req)
                                                                                         });
            //    self.store_service.as_ref().parse_vertex_result(&mut result_stream_list, self.graph.get_schema(si).unwrap(), &mut result);
        }
        return LocalStoreVertexIterator::from_getting_all_vertices(None, limit, None, output_prop_ids.cloned(), IteratorList::new(result));
    }

    fn get_all_edges(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::EI {
        let mut result = vec![];
        if self.local_opt_flag {
            let local_partition_list = if partition_ids.is_empty() {self.graph.get_partitions()} else {partition_ids.clone()};
            for partition_id in local_partition_list.iter() {
                self.query_local_edge_iter(si, labels, *partition_id, &mut result);
            }
        } else {
            let mut worker_req_list = HashMap::new();
            let mut worker_partition_list = HashMap::new();
            self.process_partitions(partition_ids, &mut worker_partition_list);
            for (process_index, partition_ids) in worker_partition_list.iter() {
                self.store_service.as_ref().build_query_request(si,
                                                                labels,
                                                                &mut worker_req_list,
                                                                *process_index,
                                                                condition,
                                                                dedup_prop_ids,
                                                                output_prop_ids,
                                                                limit,
                                                                partition_ids);
            }

            let result_stream_list = self.store_service.as_ref().process_remote_requests(&mut worker_req_list,
                                                                                         |client, req| {
                                                                                             client.query_edges(req)
                                                                                         });
            //   self.store_service.as_ref().parse_graph_edge_result(&mut result_stream_list, self.graph.get_schema(si).unwrap(), &mut result);
        }
        return LocalStoreEdgeIterator::new(None, limit, None, output_prop_ids.cloned(), IteratorList::new(result));
    }

    fn count_all_vertices(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, partition_ids: &Vec<PartitionId>) -> u64 {
        let mut result = 0;
        if self.local_opt_flag {
            let local_partition_list = if partition_ids.is_empty() {self.graph.get_partitions()} else {partition_ids.clone()};
            for partition_id in local_partition_list.iter() {
                self.query_local_vertex_count(si, labels, condition, *partition_id, &mut result);
            }
        } else {
            let mut worker_req_list = HashMap::new();
            let mut worker_partition_list = HashMap::new();
            self.process_partitions(partition_ids, &mut worker_partition_list);
            for (process_index, partition_ids) in worker_partition_list.iter() {
                self.store_service.as_ref().build_query_count_request(si, labels, &mut worker_req_list, *process_index, condition, partition_ids);
            }
            if !worker_req_list.is_empty() {
                let mut result_stream_list = Vec::with_capacity(worker_req_list.len());
                for (process_index, req) in worker_req_list.into_iter() {
                    if let Some(client) = self.store_service.get_client_list().get(&process_index) {
                        if let Ok(vertex_stream) = client.get_client().vertex_count(&req) {
                            result_stream_list.push(vertex_stream);
                        }
                    }
                }
                self.store_service.as_ref().parse_count_result(&mut result_stream_list, &mut result);
            }
        }
        return result;
    }

    fn count_all_edges(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, partition_ids: &Vec<PartitionId>) -> u64 {
        let mut result = 0;
        if self.local_opt_flag {
            let local_partition_list = if partition_ids.is_empty() {self.graph.get_partitions()} else {partition_ids.clone()};
            for partition_id in local_partition_list.iter() {
                self.query_local_edge_count(si, labels, condition, *partition_id, &mut result);
            }
        } else {
            let mut worker_req_list = HashMap::new();
            let mut worker_partition_list = HashMap::new();
            self.process_partitions(partition_ids, &mut worker_partition_list);

            for (process_index, partition_ids) in worker_partition_list.iter() {
                self.store_service.as_ref().build_query_count_request(si, labels, &mut worker_req_list, *process_index, condition, partition_ids);
            }
            if !worker_req_list.is_empty() {
                let mut result_stream_list = Vec::with_capacity(worker_req_list.len());
                for (process_index, req) in worker_req_list.into_iter() {
                    if let Some(client) = self.store_service.get_client_list().get(&process_index) {
                        if let Ok(vertex_stream) = client.get_client().edge_count(&req) {
                            result_stream_list.push(vertex_stream);
                        }
                    }
                }
                self.store_service.as_ref().parse_count_result(&mut result_stream_list, &mut result);
            }
        }
        return result;
    }

    fn translate_vertex_id(&self, vertex_id: VertexId) -> VertexId {
        vertex_id
    }

    fn get_schema(&self, si: i64) -> Option<Arc<maxgraph_store::api::graph_schema::Schema>> {
        if self.local_opt_flag {
            if let Some(schema) = self.graph.get_schema(si) {
                return Some(Arc::new(LocalGraphSchema::new(schema.clone())));
            }
        }
        // TODO: consider remote graph
        None
    }
}
