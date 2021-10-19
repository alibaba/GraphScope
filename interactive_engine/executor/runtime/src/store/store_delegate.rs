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
use maxgraph_store::schema::Schema;
use maxgraph_store::schema::prelude::Relation;
use maxgraph_store::api::prelude::Property;
use maxgraph_common::proto::store_api::{GetVertexsRequest, BatchVertexEdgeRequest, BatchVertexEdgeResponse, BatchVertexCountResponse, GetOutEdgesRequest, GetInEdgesRequest};

use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraExtendEntity};
use dataflow::manager::filter::FilterManager;
use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::dedup::DedupManager;
use dataflow::builder::MessageCollector;

use store::store_service::StoreServiceManager;

use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::cell::RefCell;
use futures::stream::Stream;
use futures::future::Future;
use tokio_core::reactor::Core;
use maxgraph_common::proto::store_api_grpc::StoreServiceClient;
use store::utils::parse_property_entity_list;
use maxgraph_common::proto::gremlin_query;
use grpcio::ClientSStreamReceiver;
use maxgraph_common::proto::message::LogicalCompare;
use protobuf::RepeatedField;
use store::{LocalStoreVertex, LocalStoreEdge};
use alloc::vec::IntoIter;
use itertools::Itertools;

pub struct StoreDelegate<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    store_service: Arc<StoreServiceManager>,
    graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
    partition_list: Vec<u32>,
}

impl<V, VI, E, EI> Clone for StoreDelegate<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    fn clone(&self) -> Self {
        StoreDelegate {
            store_service: self.store_service.clone(),
            graph: self.graph.clone(),
            partition_list: self.partition_list.to_vec(),
        }
    }
}

impl<V, VI, E, EI> StoreDelegate<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(store_service: Arc<StoreServiceManager>,
               graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
               partition_list: Vec<u32>) -> Self {
        StoreDelegate {
            store_service,
            graph,
            partition_list,
        }
    }

    pub fn get_store_service(&self) -> &Arc<StoreServiceManager> {
        &self.store_service
    }

    fn get_process_index(&self, vid: i64) -> i64 {
        let partition = self.graph.as_ref().get_partition_id(vid);
        let partition_per_process = self.store_service.as_ref().get_partition_per_process();
        if partition_per_process == 0 {
            return 0;
        } else {
            return (partition as u64 / partition_per_process) as i64;
        }
    }

    fn get_range_limit(&self,
                       range_limit: usize,
                       filter_manager: &FilterManager,
                       dedup_none_flag: bool) -> usize {
        if filter_manager.is_empty() && dedup_none_flag {
            return 0;
        }
        return range_limit;
    }

    pub fn process_out_vertex<'a>(&self,
                                  data: Vec<RawMessage>,
                                  filter_manager: &FilterManager,
                                  before_requirement: &RequirementManager,
                                  after_requirement: &RequirementManager,
                                  mut dedup_manager: Option<Arc<DedupManager>>,
                                  range_limit: usize,
                                  si: SnapshotId,
                                  label_list: &Vec<u32>,
                                  collector: &mut Box<'a + MessageCollector>,
                                  remote_graph_flag: bool)
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut worker_req_list = HashMap::new();
        let mut seq_extend_list = Rc::new(RefCell::new(HashMap::new()));
        let mut seq_id = 0;
        for d in data.into_iter() {
            let mut m = before_requirement.process_requirement(d);
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    let partition_id = self.get_partition_id(m.get_id());
                    let process_index = self.get_process_index(m.get_id());
                    if remote_graph_flag {
                        if process_index == self.store_service.as_ref().get_worker_index() {
                            self.process_partition_graph_out_vertex(after_requirement,
                                                                    &mut dedup_manager,
                                                                    range_limit,
                                                                    si,
                                                                    label_list,
                                                                    collector,
                                                                    &mut m);
                        } else {
                            self.store_service.as_ref().build_batch_request(si,
                                                                            label_list,
                                                                            &mut worker_req_list,
                                                                            seq_id,
                                                                            &mut m,
                                                                            process_index,
                                                                            false,
                                                                            self.get_range_limit(range_limit,
                                                                                                 filter_manager,
                                                                                                 dedup_manager.is_none()));

                            seq_extend_list.borrow_mut().insert(seq_id, (m.get_id(), m.get_label_id(), m.take_extend_entity()));
                            seq_id += 1;
                        }
                    } else {
                        if process_index == self.store_service.as_ref().get_worker_index() && self.partition_list.contains(&partition_id) {
                            self.process_partition_graph_out_vertex(after_requirement,
                                                                    &mut dedup_manager,
                                                                    range_limit,
                                                                    si,
                                                                    label_list,
                                                                    collector,
                                                                    &mut m);
                        }
                    }
                }
                _ => {}
            }
        }

        self.store_service.as_ref().process_remote_graph(filter_manager,
                                                         after_requirement,
                                                         &mut dedup_manager,
                                                         range_limit,
                                                         collector,
                                                         &mut worker_req_list,
                                                         &mut seq_extend_list,
                                                         |client, req| {
                                                             client.get_batch_out_target(req)
                                                         },
                                                         |_, _, res| {
                                                             let target = res.get_target_vid();
                                                             RawMessage::from_vertex_id(target.get_typeId(), target.get_id())
                                                         },
                                                         true,
                                                         si,
                                                         self.graph.get_schema(si).unwrap())
    }

    pub fn process_out_edge<'a>(&self, data: Vec<RawMessage>,
                                filter_manager: &FilterManager,
                                before_requirement: &RequirementManager,
                                after_requirement: &RequirementManager,
                                mut dedup_manager: Option<Arc<DedupManager>>,
                                range_limit: usize,
                                si: SnapshotId,
                                label_list: &Vec<u32>,
                                collector: &mut Box<'a + MessageCollector>,
                                remote_graph_flag: bool)
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut worker_req_list = HashMap::new();
        let mut seq_extend_list = Rc::new(RefCell::new(HashMap::new()));
        let mut seq_id = 0;
        for d in data.into_iter() {
            let mut m = before_requirement.process_requirement(d);
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    let partition_id = self.get_partition_id(m.get_id());
                    let process_index = self.get_process_index(m.get_id());
                    if remote_graph_flag {
                        if process_index == self.store_service.as_ref().get_worker_index() {
                            self.process_partition_graph_out_edge(filter_manager,
                                                                  after_requirement,
                                                                  &mut dedup_manager,
                                                                  range_limit,
                                                                  si,
                                                                  label_list,
                                                                  collector,
                                                                  &mut m);
                        } else {
                            self.store_service.as_ref().build_batch_request(si,
                                                                            label_list,
                                                                            &mut worker_req_list,
                                                                            seq_id,
                                                                            &mut m,
                                                                            process_index,
                                                                            true,
                                                                            self.get_range_limit(range_limit,
                                                                                                 filter_manager,
                                                                                                 dedup_manager.is_none()));

                            seq_extend_list.borrow_mut().insert(seq_id, (m.get_id(), m.get_label_id(), m.take_extend_entity()));
                            seq_id += 1;
                        }
                    } else {
                        if process_index == self.store_service.as_ref().get_worker_index() && self.partition_list.contains(&partition_id) {
                            self.process_partition_graph_out_edge(filter_manager,
                                                                  after_requirement,
                                                                  &mut dedup_manager,
                                                                  range_limit,
                                                                  si,
                                                                  label_list,
                                                                  collector,
                                                                  &mut m);
                        }
                    }
                }
                _ => {}
            }
        }

        self.store_service.as_ref().process_remote_graph(filter_manager,
                                                         after_requirement,
                                                         &mut dedup_manager,
                                                         range_limit,
                                                         collector,
                                                         &mut worker_req_list,
                                                         &mut seq_extend_list,
                                                         |client, req| {
                                                             client.get_batch_out_target(req)
                                                         },
                                                         |id, label_id, res| {
                                                             let target = res.get_target_vid();
                                                             RawMessage::from_edge_id(res.get_edge_id(),
                                                                                      res.get_edge_label_id(),
                                                                                      true,
                                                                                      id,
                                                                                      label_id,
                                                                                      target.get_id(),
                                                                                      target.get_typeId())
                                                         },
                                                         true,
                                                         si,
                                                         self.graph.get_schema(si).unwrap())
    }

    pub fn process_in_vertex<'a>(&self, data: Vec<RawMessage>,
                                 filter_manager: &FilterManager,
                                 before_requirement: &RequirementManager,
                                 after_requirement: &RequirementManager,
                                 mut dedup_manager: Option<Arc<DedupManager>>,
                                 range_limit: usize,
                                 si: SnapshotId,
                                 label_list: &Vec<u32>,
                                 collector: &mut Box<'a + MessageCollector>,
                                 remote_graph_flag: bool)
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut worker_req_list = HashMap::new();
        let mut seq_extend_list = Rc::new(RefCell::new(HashMap::new()));
        let mut seq_id = 0;
        for d in data.into_iter() {
            let mut m = before_requirement.process_requirement(d);
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    let mut curr_limit_count = 0;
                    let mut range_flag = false;
                    for partition_id in self.partition_list.iter() {
                        if let Some(partition_graph) = self.graph.get_partition(*partition_id) {
                            if self.process_partition_graph_in_vertex(after_requirement, &mut dedup_manager, range_limit, si, label_list, collector, &mut curr_limit_count, &mut m, partition_graph) {
                                range_flag = true;
                                break;
                            }
                        }
                    }
                    if range_flag {
                        continue;
                    }
                    if remote_graph_flag {
                        for partitionid in self.graph.get_partitions().into_iter() {
                            if !self.partition_list.contains(&partitionid) {
                                if let Some(partition_graph) = self.graph.get_partition(partitionid) {
                                    if self.process_partition_graph_in_vertex(after_requirement, &mut dedup_manager, range_limit, si, label_list, collector, &mut curr_limit_count, &mut m, partition_graph) {
                                        range_flag = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if range_flag {
                            continue;
                        }
                        for (key, _) in self.store_service.get_client_list().iter() {
                            if self.store_service.as_ref().get_worker_index() != *key {
                                self.store_service.as_ref()
                                    .build_batch_request(si,
                                                         label_list,
                                                         &mut worker_req_list,
                                                         seq_id,
                                                         &mut m,
                                                         *key,
                                                         false,
                                                         self.get_range_limit(range_limit,
                                                                              filter_manager,
                                                                              dedup_manager.is_none()));
                            }
                        }

                        seq_extend_list.borrow_mut().insert(seq_id, (m.get_id(), m.get_label_id(), m.take_extend_entity()));
                        seq_id += 1;
                    }
                }
                _ => {}
            }
        }

        self.store_service.as_ref()
            .process_remote_graph(filter_manager,
                                  after_requirement,
                                  &mut dedup_manager,
                                  range_limit,
                                  collector,
                                  &mut worker_req_list,
                                  &mut seq_extend_list,
                                  |client, req| {
                                      client.get_batch_in_target(req)
                                  },
                                  |id, label_id, res| {
                                      let target = res.get_target_vid();
                                      RawMessage::from_vertex_id(target.get_typeId(), target.get_id())
                                  },
                                  false,
                                  si,
                                  self.graph.get_schema(si).unwrap());
    }

    pub fn process_in_edge<'a>(&self,
                               data: Vec<RawMessage>,
                               filter_manager: &FilterManager,
                               before_requirement: &RequirementManager,
                               after_requirement: &RequirementManager,
                               mut dedup_manager: Option<Arc<DedupManager>>,
                               range_limit: usize,
                               si: SnapshotId,
                               label_list: &Vec<u32>,
                               collector: &mut Box<'a + MessageCollector>,
                               remote_graph_flag: bool)
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut worker_req_list = HashMap::new();
        let mut seq_extend_list = Rc::new(RefCell::new(HashMap::new()));
        let mut seq_id = 0;
        for d in data.into_iter() {
            let mut m = before_requirement.process_requirement(d);
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    let mut curr_limit_count = 0;
                    let mut range_flag = false;
                    for partition_id in self.partition_list.iter() {
                        if let Some(partition_graph) = self.graph.get_partition(*partition_id) {
                            if self.process_partition_graph_in_edge(filter_manager,
                                                                    after_requirement,
                                                                    &mut dedup_manager,
                                                                    range_limit,
                                                                    si,
                                                                    label_list,
                                                                    collector,
                                                                    &mut curr_limit_count,
                                                                    &mut m,
                                                                    partition_graph) {
                                range_flag = true;
                                break;
                            }
                        }
                    }
                    if range_flag {
                        continue;
                    }
                    if remote_graph_flag {
                        for partitionid in self.graph.get_partitions().into_iter() {
                            if !self.partition_list.contains(&partitionid) {
                                if let Some(partition_graph) = self.graph.get_partition(partitionid) {
                                    if self.process_partition_graph_in_edge(filter_manager,
                                                                            after_requirement,
                                                                            &mut dedup_manager,
                                                                            range_limit,
                                                                            si,
                                                                            label_list,
                                                                            collector,
                                                                            &mut curr_limit_count,
                                                                            &mut m,
                                                                            partition_graph) {
                                        range_flag = true;
                                        break;
                                    }
                                }
                            }
                        }
                        if range_flag {
                            continue;
                        }
                        for (key, _) in self.store_service.get_client_list().iter() {
                            if self.store_service.as_ref().get_worker_index() != *key {
                                self.store_service.as_ref()
                                    .build_batch_request(si,
                                                         label_list,
                                                         &mut worker_req_list,
                                                         seq_id,
                                                         &mut m,
                                                         *key,
                                                         true,
                                                         self.get_range_limit(range_limit,
                                                                              filter_manager,
                                                                              dedup_manager.is_none()));
                            }
                        }

                        seq_extend_list.borrow_mut().insert(seq_id, (m.get_id(), m.get_label_id(), m.take_extend_entity()));
                        seq_id += 1;
                    }
                }
                _ => {}
            }
        }

        self.store_service.process_remote_graph(filter_manager,
                                                after_requirement,
                                                &mut dedup_manager,
                                                range_limit,
                                                collector,
                                                &mut worker_req_list,
                                                &mut seq_extend_list,
                                                |client, req| {
                                                    client.get_batch_in_target(req)
                                                },
                                                |id, label_id, res| {
                                                    let target = res.get_target_vid();
                                                    RawMessage::from_edge_id(res.get_edge_id(),
                                                                             res.get_edge_label_id(),
                                                                             false,
                                                                             target.get_id(),
                                                                             target.get_typeId(),
                                                                             id,
                                                                             label_id)
                                                },
                                                false,
                                                si,
                                                self.graph.get_schema(si).unwrap())
    }

    pub fn query_vertex_properties(&self,
                                   data: &Vec<RawMessage>,
                                   prop_ids: &Vec<i32>,
                                   si: i64) -> Result<HashMap<i64, Vec<(i32, Property)>>, String>
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut vertex_props_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        for m in data.iter() {
            if vertex_props_list.contains_key(&m.get_id()) {
                continue;
            }
            let process_index = self.get_process_index(m.get_id());
            if process_index == self.store_service.as_ref().get_worker_index() {
                let mut prop_value_list = vec![];
                if let Some(v) = self.graph.get_vertex(si, m.get_id(), Some(m.get_label_id() as u32)) {
                    if prop_ids.is_empty() {
                        for (propid, prop) in v.get_properties() {
                            prop_value_list.push((propid as i32, prop));
                        }
                    } else {
                        for propid in prop_ids {
                            if let Some(prop) = v.get_property(*propid as u32) {
                                prop_value_list.push((*propid, prop));
                            }
                        }
                    }
                }
                vertex_props_list.insert(m.get_id(), prop_value_list);
            } else {
                let label_vertex_req_list = worker_req_list.entry(process_index).or_insert(HashMap::new());
                let vertex_req = label_vertex_req_list.entry(m.get_label_id()).or_insert(GetVertexsRequest::new());
                vertex_req.set_type_id(m.get_label_id());
                vertex_req.set_snapshot_id(si);
                vertex_req.mut_ids().push(m.get_id());
                if !prop_ids.is_empty() {
                    if vertex_req.get_prop_ids().is_empty() {
                        for prop_id in prop_ids.iter() {
                            vertex_req.mut_prop_ids().push(*prop_id);
                        }
                    }
                }
            }
        }
        if !worker_req_list.is_empty() {
            let mut result_stream_list = vec![];
            let all_prop_flag = prop_ids.is_empty();
            for (process_index, mut label_req_list) in worker_req_list.into_iter() {
                if let Some(client) = self.store_service.get_client_list().get(&process_index) {
                    for (_, req) in label_req_list.drain() {
                        if let Ok(vertex_stream) = client.get_client().get_vertexs(&req) {
                            result_stream_list.push(vertex_stream);
                        }
                    }
                }
            }

            let schema = self.graph.get_schema(si).unwrap();
            for res_stream in result_stream_list.drain(..) {
                let collect = res_stream.collect();
                if let Ok(val_list) = collect.wait() {
                    for mut v in val_list.into_iter() {
                        let vid = v.take_id();
                        let props = v.take_pros();
                        if let Some(prop_val_list) = parse_property_entity_list(vid.get_typeId() as u32, props, &schema) {
                            vertex_props_list.insert(vid.get_id(), prop_val_list);
                        }
                    }
                }
            }
        }
        return Ok(vertex_props_list);
    }

    pub fn query_vertex_out_count(&self,
                                  data: &Vec<RawMessage>,
                                  label_list: &Vec<u32>,
                                  si: i64,
                                  logical_compare: &Vec<LogicalCompare>,
                                  filter_manager: &FilterManager,
                                  remote_graph_flag: bool) -> Result<HashMap<i64, i64>, String> {
        let mut vertex_count_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        let mut vertex_id_list = HashSet::new();
        for m in data.iter() {
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    if vertex_id_list.contains(&m.get_id()) {
                        continue;
                    }
                    vertex_id_list.insert(m.get_id());
                    let partition_id = self.get_partition_id(m.get_id());
                    if self.partition_list.contains(&partition_id) {
                        self.process_vertex_local_out_count(label_list, si, filter_manager, &mut vertex_count_list, m);
                        continue;
                    }
                    if remote_graph_flag {
                        let process_index = self.get_process_index(m.get_id());
                        if self.store_service.get_worker_index() == process_index {
                            self.process_vertex_local_out_count(label_list, si, filter_manager, &mut vertex_count_list, m);
                        } else {
                            let batch_req = worker_req_list.entry(process_index).or_insert(BatchVertexEdgeRequest::new());
                            let mut vid = gremlin_query::VertexId::new();
                            vid.set_typeId(m.get_label_id());
                            vid.set_id(m.get_id());
                            batch_req.mut_vertex_id().push(vid);
                            batch_req.set_snapshot_id(si);
                            if batch_req.get_label_list().is_empty() && !label_list.is_empty() {
                                batch_req.set_label_list(label_list.to_vec());
                            }
                            if batch_req.get_logical_compare().is_empty() && !logical_compare.is_empty() {
                                batch_req.set_logical_compare(RepeatedField::from_vec(logical_compare.to_vec()));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        if !worker_req_list.is_empty() {
            let mut stream_result_list = Vec::with_capacity(worker_req_list.len());
            for (key, req) in worker_req_list.drain() {
                if let Some(stream_result) = self.store_service.query_vertex_out_count(key, &req) {
                    if let Ok(stream) = stream_result {
                        stream_result_list.push(stream);
                    } else {
                        return Err("connect to store server fail".to_owned());
                    }
                } else {
                    return Err(format!("there's no store server with key {:?}", key));
                }
            }
            self.parse_vertex_count_result(&mut vertex_count_list,
                                           &mut stream_result_list);
        }

        return Ok(vertex_count_list);
    }

    fn get_partition_id(&self, src_id: VertexId) -> PartitionId {
        let mut ret = src_id % (self.store_service.get_partition_num() as VertexId);
        if ret < 0 {
            ret += self.store_service.get_partition_num() as i64;
        }
        ret as PartitionId
    }

    fn parse_vertex_count_result(&self,
                                 vertex_count_list: &mut HashMap<i64, i64>,
                                 stream_result_list: &mut Vec<ClientSStreamReceiver<BatchVertexCountResponse>>) {
        for res_stream in stream_result_list.into_iter() {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for v in val_list.into_iter() {
                    let vid = v.get_vid();
                    let count_val = v.get_count_val();
                    let curr_count = vertex_count_list.entry(vid).or_insert(0);
                    *curr_count += count_val;
                }
            }
        }
    }

    fn process_vertex_local_out_count(&self,
                                      label_list: &Vec<u32>,
                                      si: i64,
                                      filter_manager: &FilterManager,
                                      vertex_count_list: &mut HashMap<i64, i64>,
                                      m: &RawMessage) {
        let mut count_value = 0;
        if label_list.is_empty() {
            count_value += self.graph
                .get_out_edges(si, m.get_id(), None)
                .filter(|e| filter_manager.filter_native_edge(e))
                .count() as i64;
        } else {
            for labelid in label_list.iter() {
                count_value += self.graph
                    .get_out_edges(si, m.get_id(), Some(*labelid))
                    .filter(|e| filter_manager.filter_native_edge(e))
                    .count() as i64;
            }
        }
        vertex_count_list.insert(m.get_id(), count_value);
    }

    pub fn query_vertex_in_count(&self,
                                 data: &Vec<RawMessage>,
                                 label_list: &Vec<u32>,
                                 si: i64,
                                 logical_compare: &Vec<LogicalCompare>,
                                 filter_manager: &FilterManager,
                                 remote_graph_flag: bool) -> Result<HashMap<i64, i64>, String> {
        let mut vertex_count_list = HashMap::new();
        let mut worker_req_list = HashMap::new();
        let mut vertex_id_list = HashSet::new();
        for m in data.iter() {
            match m.get_message_type() {
                RawMessageType::VERTEX => {
                    if vertex_id_list.contains(&m.get_id()) {
                        continue;
                    }
                    vertex_id_list.insert(m.get_id());
                    let mut count_val = 0;
                    for partition_id in self.partition_list.iter() {
                        if let Some(graph_partition) = self.graph.get_partition(*partition_id) {
                            let curr_count = {
                                if label_list.is_empty() {
                                    graph_partition
                                        .get_in_edges(si, m.get_id(), None)
                                        .filter(|e| filter_manager.filter_native_edge(e))
                                        .count() as i64
                                } else {
                                    let mut labeled_count = 0;
                                    for labelid in label_list.iter() {
                                        labeled_count += graph_partition
                                            .get_in_edges(si, m.get_id(), Some(*labelid))
                                            .filter(|e| filter_manager.filter_native_edge(e))
                                            .count() as i64;
                                    }
                                    labeled_count
                                }
                            };
                            count_val += curr_count;
                        }
                    }

                    if remote_graph_flag {
                        let curr_partition_list = self.graph.get_partitions();
                        for partition_id in curr_partition_list.into_iter() {
                            if !self.partition_list.contains(&partition_id) {
                                if let Some(graph_partition) = self.graph.get_partition(partition_id) {
                                    let curr_count = {
                                        if label_list.is_empty() {
                                            graph_partition
                                                .get_in_edges(si, m.get_id(), None)
                                                .filter(|e| filter_manager.filter_native_edge(e))
                                                .count() as i64
                                        } else {
                                            let mut labeled_count = 0;
                                            for labelid in label_list.iter() {
                                                labeled_count += graph_partition
                                                    .get_in_edges(si, m.get_id(), Some(*labelid))
                                                    .filter(|e| filter_manager.filter_native_edge(e))
                                                    .count() as i64;
                                            }
                                            labeled_count
                                        }
                                    };
                                    count_val += curr_count;
                                }
                            }
                        }

                        for (key, _) in self.store_service.get_client_list().iter() {
                            if *key != self.store_service.get_worker_index() {
                                let batch_req = worker_req_list.entry(*key).or_insert(BatchVertexEdgeRequest::new());
                                let mut vid = gremlin_query::VertexId::new();
                                vid.set_typeId(m.get_label_id());
                                vid.set_id(m.get_id());
                                batch_req.set_snapshot_id(si);
                                batch_req.mut_vertex_id().push(vid);
                                if batch_req.get_label_list().is_empty() && !label_list.is_empty() {
                                    batch_req.set_label_list(label_list.to_vec());
                                }
                                if batch_req.get_logical_compare().is_empty() && !logical_compare.is_empty() {
                                    batch_req.set_logical_compare(RepeatedField::from_vec(logical_compare.to_vec()));
                                }
                            }
                        }
                    }
                    vertex_count_list.insert(m.get_id(), count_val);
                }
                _ => {}
            }
        }

        if !worker_req_list.is_empty() {
            let mut stream_result_list = Vec::with_capacity(worker_req_list.len());
            for (key, req) in worker_req_list.drain() {
                if let Some(stream_result) = self.store_service.query_vertex_in_count(key, &req) {
                    if let Ok(stream) = stream_result {
                        stream_result_list.push(stream);
                    } else {
                        return Err("connect to store server fail".to_owned());
                    }
                } else {
                    return Err(format!("there's no store server with key {:?}", key));
                }
            }
            self.parse_vertex_count_result(&mut vertex_count_list, &mut stream_result_list)
        }

        return Ok(vertex_count_list);
    }

    fn process_partition_graph_out_vertex<'a>(&self,
                                              after_requirement: &RequirementManager,
                                              dedup_manager: &mut Option<Arc<DedupManager>>,
                                              range_limit: usize,
                                              si: i64,
                                              label_list: &Vec<u32>,
                                              collector: &mut Box<'a + MessageCollector>,
                                              m: &mut RawMessage)
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut curr_limit_count = 0;
        if label_list.is_empty() {
            for e in self.graph.get_out_edges(si, m.get_id(), None) {
                if self.process_edge_out_vertex(after_requirement,
                                                dedup_manager,
                                                range_limit,
                                                collector,
                                                &mut curr_limit_count,
                                                m,
                                                e) {
                    return;
                }
            }
        } else {
            for label_id in label_list.iter() {
                for e in self.graph.get_out_edges(si, m.get_id(), Some(*label_id)) {
                    if self.process_edge_out_vertex(after_requirement,
                                                    dedup_manager,
                                                    range_limit,
                                                    collector,
                                                    &mut curr_limit_count,
                                                    m,
                                                    e) {
                        return;
                    }
                }
            }
        }
    }

    fn process_partition_graph_out_edge<'a>(&self,
                                            filter_manager: &FilterManager,
                                            after_requirement: &RequirementManager,
                                            dedup_manager: &mut Option<Arc<DedupManager>>,
                                            range_limit: usize,
                                            si: i64,
                                            label_list: &Vec<u32>,
                                            collector: &mut Box<'a + MessageCollector>,
                                            m: &mut RawMessage)
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        let mut curr_limit_count = 0;
        if label_list.is_empty() {
            for e in self.graph.get_out_edges(si, m.get_id(), None) {
                if self.process_edge_message(filter_manager,
                                             after_requirement,
                                             dedup_manager,
                                             range_limit,
                                             collector,
                                             &mut curr_limit_count,
                                             m,
                                             e,
                                             true) {
                    return;
                }
            }
        } else {
            for label_id in label_list.iter() {
                for e in self.graph.get_out_edges(si, m.get_id(), Some(*label_id)) {
                    if self.process_edge_message(filter_manager,
                                                 after_requirement,
                                                 dedup_manager,
                                                 range_limit,
                                                 collector,
                                                 &mut curr_limit_count,
                                                 m,
                                                 e,
                                                 true) {
                        return;
                    }
                }
            }
        }
    }

    fn process_partition_graph_in_edge<'a>(&self,
                                           filter_manager: &FilterManager,
                                           after_requirement: &RequirementManager,
                                           dedup_manager: &mut Option<Arc<DedupManager>>,
                                           range_limit: usize,
                                           si: i64,
                                           label_list: &Vec<u32>,
                                           collector: &mut Box<'a + MessageCollector>,
                                           curr_limit_count: &mut usize,
                                           m: &mut RawMessage,
                                           partition_graph: Arc<MVGraphQuery<V=V, E=E, VI=VI, EI=EI>>) -> bool
        where V: Vertex,
              VI: Iterator<Item=V>,
              E: Edge,
              EI: Iterator<Item=E> {
        if label_list.is_empty() {
            for e in partition_graph.as_ref().get_in_edges(si, m.get_id(), None) {
                if self.process_edge_message(filter_manager,
                                             after_requirement,
                                             dedup_manager,
                                             range_limit,
                                             collector,
                                             curr_limit_count,
                                             m,
                                             e,
                                             false) {
                    return true;
                }
            }
        } else {
            for label_id in label_list.iter() {
                for e in partition_graph.get_in_edges(si, m.get_id(), Some(*label_id)) {
                    if self.process_edge_message(filter_manager,
                                                 after_requirement,
                                                 dedup_manager,
                                                 range_limit,
                                                 collector,
                                                 curr_limit_count,
                                                 m,
                                                 e,
                                                 false) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    fn process_partition_graph_in_vertex<'a>(&self,
                                             after_requirement: &RequirementManager,
                                             dedup_manager: &mut Option<Arc<DedupManager>>,
                                             range_limit: usize,
                                             si: i64,
                                             label_list: &Vec<u32>,
                                             collector: &mut Box<'a + MessageCollector>,
                                             curr_limit_count: &mut usize,
                                             m: &mut RawMessage,
                                             partition_graph: Arc<MVGraphQuery<V=V, E=E, VI=VI, EI=EI>>) -> bool
        where V: Vertex,
              E: Edge,
              VI: Iterator<Item=V>,
              EI: Iterator<Item=E> {
        if label_list.is_empty() {
            for e in partition_graph.as_ref().get_in_edges(si, m.get_id(), None) {
                if self.process_edge_in_vertex(after_requirement,
                                               dedup_manager,
                                               range_limit,
                                               collector,
                                               curr_limit_count,
                                               m,
                                               e) {
                    return true;
                }
            }
        } else {
            for label_id in label_list.iter() {
                for e in partition_graph.as_ref().get_in_edges(si, m.get_id(), Some(*label_id)) {
                    if self.process_edge_in_vertex(after_requirement,
                                                   dedup_manager,
                                                   range_limit,
                                                   collector,
                                                   curr_limit_count,
                                                   m,
                                                   e) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    fn process_edge_message<'a>(&self,
                                filter_manager: &FilterManager,
                                after_requirement: &RequirementManager,
                                dedup_manager: &mut Option<Arc<DedupManager>>,
                                range_limit: usize,
                                collector: &mut Box<'a + MessageCollector>,
                                curr_limit_count: &mut usize,
                                m: &mut RawMessage,
                                e: E,
                                out_flag: bool) -> bool {
        if !filter_manager.filter_native_edge(&e) {
            return false;
        }
        if let Some(dedup) = dedup_manager {
            if !dedup.check_dedup(e.get_edge_id()) {
                return false;
            }
        }
        if range_limit > 0 && *curr_limit_count >= range_limit {
            return true;
        }
        *curr_limit_count += m.get_bulk() as usize;
        let mut result = RawMessage::from_edge(e, out_flag, true);
        after_requirement.process_extend_entity(&m, &mut result);
        collector.collect_message(after_requirement.process_requirement(result));

        return false;
    }

    fn process_edge_out_vertex<'a>(&self,
                                   after_requirement: &RequirementManager,
                                   dedup_manager: &mut Option<Arc<DedupManager>>,
                                   range_limit: usize,
                                   collector: &mut Box<'a + MessageCollector>,
                                   curr_limit_count: &mut usize,
                                   m: &mut RawMessage,
                                   e: E) -> bool {
        if let Some(dedup) = dedup_manager {
            if !dedup.check_dedup(e.get_dst_id()) {
                return false;
            }
        }
        if range_limit > 0 && *curr_limit_count >= range_limit {
            return true;
        }
        *curr_limit_count += m.get_bulk() as usize;
        let mut result = RawMessage::from_vertex_id(e.get_dst_label_id() as i32, e.get_dst_id());
        after_requirement.process_extend_entity(&m, &mut result);
        collector.collect_message(after_requirement.process_requirement(result));

        return false;
    }

    fn process_edge_in_vertex<'a>(&self,
                                  after_requirement: &RequirementManager,
                                  dedup_manager: &mut Option<Arc<DedupManager>>,
                                  range_limit: usize,
                                  collector: &mut Box<'a + MessageCollector>,
                                  curr_limit_count: &mut usize,
                                  m: &mut RawMessage,
                                  e: E) -> bool {
        if let Some(dedup) = dedup_manager {
            if !dedup.check_dedup(e.get_src_id()) {
                return false;
            }
        }
        if range_limit > 0 && *curr_limit_count >= range_limit {
            return true;
        }
        *curr_limit_count += m.get_bulk() as usize;
        let mut result = RawMessage::from_vertex_id(e.get_src_label_id() as i32, e.get_src_id());
        after_requirement.process_extend_entity(&m, &mut result);
        collector.collect_message(after_requirement.process_requirement(result));

        return false;
    }
}

impl<V, VI, E, EI> MVGraphQuery for StoreDelegate<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    type V = LocalStoreVertex;
    type E = LocalStoreEdge;
    type VI = IntoIter<LocalStoreVertex>;
    type EI = IntoIter<LocalStoreEdge>;

    fn get_vertex(&self, si: i64, id: i64, label: Option<u32>) -> Option<Self::V> {
        let schema = self.graph.get_schema(si).unwrap();
        let process_index = self.get_process_index(id);
        if process_index == self.store_service.as_ref().get_worker_index() {
            if let Some(v) = self.graph.as_ref().get_vertex(si, id, label) {
                let mut store_vertex = LocalStoreVertex::new(v.get_id(), v.get_label_id());
                for (pid, pval) in v.get_properties() {
                    store_vertex.add_property(pid, pval);
                }
                return Some(store_vertex);
            }
        } else {
            if let Some(client) = self.store_service.get_client_list().get(&process_index) {
                let mut vertex_req = GetVertexsRequest::new();
                vertex_req.set_snapshot_id(si);
                vertex_req.mut_ids().push(id);
                if let Some(labelid) = label {
                    vertex_req.set_type_id(labelid as i32);
                }
                if let Ok(res_stream) = client.get_client().get_vertexs(&vertex_req) {
                    let collect = res_stream.collect();
                    if let Ok(val_list) = collect.wait() {
                        for mut res_vertex in val_list.into_iter() {
                            let vid = res_vertex.get_id();
                            let mut store_vertex = LocalStoreVertex::new(vid.get_id(), vid.get_typeId() as u32);
                            if let Some(proplist) = parse_property_entity_list(vid.get_typeId() as u32, res_vertex.take_pros(), &schema) {
                                store_vertex.add_properties(proplist);
                            }
                            return Some(store_vertex);
                        }
                    }
                }
            }
        }
        return None;
    }

    fn get_out_edges(&self, si: i64, src_id: i64, label: Option<u32>) -> Self::EI {
        let process_index = self.get_process_index(src_id);
        let mut out_list = vec![];
        if process_index == self.store_service.as_ref().get_worker_index() {
            for e in self.graph.as_ref()
                .get_out_edges(si, src_id, label) {
                let src = LocalStoreVertex::new(e.get_src_id(), e.get_src_label_id());
                let dst = LocalStoreVertex::new(e.get_dst_id(), e.get_dst_label_id());
                let mut store_edge = LocalStoreEdge::new(src, dst, e.get_label_id(), e.get_edge_id());
                for (pid, pval) in e.get_properties() {
                    store_edge.add_property(pid, pval);
                }
                out_list.push(store_edge);
            }
        } else {
            if let Some(client) = self.store_service.get_client_list().get(&process_index) {
                let mut out_edge_req = GetOutEdgesRequest::new();
                out_edge_req.set_src_id(src_id);
                out_edge_req.set_snapshot_id(si);
                if let Some(labelid) = label {
                    out_edge_req.set_type_id(labelid as i32);
                }
                let schema = self.graph.get_schema(si).unwrap();
                if let Ok(res_stream) = client.get_client().get_out_edges(&out_edge_req) {
                    let collect = res_stream.collect();
                    if let Ok(val_list) = collect.wait() {
                        for mut res_edge in val_list.into_iter() {
                            let src_id = res_edge.get_src_id();
                            let dst_id = res_edge.get_dst_id();
                            let edge_label_id = res_edge.get_type_id() as u32;
                            let edge_id = res_edge.get_edge_id();
                            let src = LocalStoreVertex::new(src_id.get_id(), src_id.get_typeId() as u32);
                            let dst = LocalStoreVertex::new(dst_id.get_id(), dst_id.get_typeId() as u32);
                            let mut edge = LocalStoreEdge::new(src, dst, edge_label_id, edge_id);
                            if let Some(proplist) = parse_property_entity_list(res_edge.get_type_id() as u32,
                                                                               res_edge.take_pros(),
                                                                               &schema) {
                                for (pid, pval) in proplist.into_iter() {
                                    edge.add_property(pid as u32, pval);
                                }
                            }
                            out_list.push(edge);
                        }
                    }
                }
            }
        }

        return out_list.into_iter();
    }

    fn get_in_edges(&self, si: i64, dst_id: i64, label: Option<u32>) -> Self::EI {
        let mut in_list = vec![];
        for e in self.graph.as_ref().get_in_edges(si, dst_id, label) {
            let src = LocalStoreVertex::new(e.get_src_id(), e.get_src_label_id());
            let dst = LocalStoreVertex::new(e.get_dst_id(), e.get_dst_label_id());
            let mut store_edge = LocalStoreEdge::new(src, dst, e.get_label_id(), e.get_edge_id());
            for (pid, pval) in e.get_properties() {
                store_edge.add_property(pid, pval);
            }
            in_list.push(store_edge);
        }
        let worker_index = self.store_service.get_worker_index();
        for (key, client) in self.store_service.get_client_list() {
            if *key != worker_index {
                let mut in_edge_req = GetInEdgesRequest::new();
                in_edge_req.set_dst_id(dst_id);
                in_edge_req.set_snapshot_id(si);
                if let Some(labelid) = label {
                    in_edge_req.set_type_id(labelid as i32);
                }
                let schema = self.graph.get_schema(si).unwrap();
                if let Ok(res_stream) = client.get_client().get_in_edges(&in_edge_req) {
                    let collect = res_stream.collect();
                    if let Ok(val_list) = collect.wait() {
                        for mut res_edge in val_list.into_iter() {
                            let src_id = res_edge.get_src_id();
                            let dst_id = res_edge.get_dst_id();
                            let edge_label_id = res_edge.get_type_id() as u32;
                            let edge_id = res_edge.get_edge_id();
                            let src = LocalStoreVertex::new(src_id.get_id(), src_id.get_typeId() as u32);
                            let dst = LocalStoreVertex::new(dst_id.get_id(), dst_id.get_typeId() as u32);
                            let mut edge = LocalStoreEdge::new(src, dst, edge_label_id, edge_id);
                            if let Some(proplist) = parse_property_entity_list(res_edge.get_type_id() as u32,
                                                                               res_edge.take_pros(),
                                                                               &schema) {
                                for (pid, pval) in proplist.into_iter() {
                                    edge.add_property(pid as u32, pval);
                                }
                            }
                            in_list.push(edge);
                        }
                    }
                }
            }
        }

        return in_list.into_iter();
    }

    fn scan(&self, si: i64, label: Option<u32>) -> Self::VI {
        self.graph.as_ref()
            .scan(si, label)
            .map(|v| {
                let mut vertex = LocalStoreVertex::new(v.get_id(), v.get_label_id());
                for (pid, pval) in v.get_properties() {
                    vertex.add_property(pid, pval);
                }
                return vertex;
            })
            .collect_vec()
            .into_iter()
    }

    fn scan_edges(&self, si: i64, label: Option<u32>) -> Self::EI {
        self.graph.scan_edges(si, label)
            .map(|e| {
                let src = LocalStoreVertex::new(e.get_src_id(), e.get_src_label_id());
                let dst = LocalStoreVertex::new(e.get_dst_id(), e.get_dst_label_id());
                let mut store_edge = LocalStoreEdge::new(src, dst, e.get_label_id(), e.get_edge_id());
                for (pid, pval) in e.get_properties() {
                    store_edge.add_property(pid, pval);
                }
                store_edge
            })
            .collect_vec()
            .into_iter()
    }

    fn count_out_edges(&self, si: i64, src_id: i64, label: Option<u32>) -> usize {
        self.graph.count_out_edges(si, src_id, label)
    }

    fn count_in_edges(&self, si: i64, dst_id: i64, label: Option<u32>) -> usize {
        self.graph.count_in_edges(si, dst_id, label)
    }

    fn edge_count(&self) -> u64 {
        self.graph.edge_count()
    }

    fn vertex_count(&self) -> u64 {
        self.graph.vertex_count()
    }

    fn estimate_vertex_count(&self, label: Option<u32>) -> u64 {
        self.graph.estimate_vertex_count(label)
    }

    fn estimate_edge_count(&self, label: Option<u32>) -> u64 {
        self.graph.estimate_edge_count(label)
    }
}
