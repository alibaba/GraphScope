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

use std::collections::{HashMap, HashSet};
use std::cell::RefCell;
use futures::stream::Stream;
use futures::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use ::byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};
use tokio_core::reactor::Core;
use tokio::runtime::current_thread::Runtime;

use store::store_client::StoreClientManager;
use store::StoreOperatorType;

use dataflow::builder::MessageCollector;
use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraExtendEntity, PropertyEntity};
use dataflow::message::primitive::Write;
use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::filter::FilterManager;
use dataflow::manager::dedup::DedupManager;

use maxgraph_store::api::prelude::{MVGraph, Vertex, Edge, Property};
use maxgraph_store::api::{SnapshotId, MVGraphQuery};
use maxgraph_common::proto::store_api::{BatchVertexEdgeRequest, BatchVertexEdgeResponse, GetVertexsRequest, BatchVertexCountResponse};
use maxgraph_common::proto::gremlin_query::VertexId;
use maxgraph_common::proto::store_api_grpc::StoreServiceClient;
use maxgraph_store::schema::Schema;
use maxgraph_store::schema::prelude::DataType;

use futures::future;
use server::RuntimeAddress;
use std::io::{Cursor, BufRead};
use store::utils::parse_property_entity_list;
use utils::{PROP_ID_LABEL, PROP_ID};
use itertools::Itertools;
use maxgraph_common::proto::message::ErrorCode;
use crossbeam_queue::SegQueue;

pub struct StoreServiceManager {
    client_list: HashMap<i64, StoreClientManager>,
    partition_num: u64,
    partition_per_process: u64,
    worker_index: i64,
}

impl Clone for StoreServiceManager {
    fn clone(&self) -> Self {
        StoreServiceManager {
            client_list: self.client_list.clone(),
            partition_num: self.partition_num,
            partition_per_process: self.partition_per_process,
            worker_index: self.worker_index,
        }
    }
}

impl StoreServiceManager {
    pub fn new(worker_index: u32,
               partition_num: u32,
               store_ip_list: Vec<RuntimeAddress>) -> Self {
        if store_ip_list.is_empty() {
            StoreServiceManager {
                client_list: HashMap::new(),
                partition_num: partition_num as u64,
                partition_per_process: 1,
                worker_index: worker_index as i64,
            }
        } else {
            let mut client_list = HashMap::new();
            let mut index = 0;
            let store_count = store_ip_list.len() as u64;
            for address in store_ip_list {
                client_list.insert(index, StoreClientManager::new(address.get_ip().to_string(), address.get_port().clone()));
                index += 1;
            }
            StoreServiceManager {
                client_list,
                partition_num: partition_num as u64,
                partition_per_process: partition_num as u64 / store_count,
                worker_index: worker_index as i64,
            }
        }
    }

    pub fn empty() -> Self {
        StoreServiceManager {
            client_list: HashMap::new(),
            partition_num: 0,
            partition_per_process: 0,
            worker_index: 0,
        }
    }

    pub fn process_remote_graph<'a, F, MF: Copy>(&self,
                                                 filter_manager: &FilterManager,
                                                 after_requirement: &RequirementManager,
                                                 dedup_manager: &mut Option<Arc<DedupManager>>,
                                                 range_limit: usize,
                                                 collector: &mut Box<'a + MessageCollector>,
                                                 worker_req_list: &mut HashMap<i64, BatchVertexEdgeRequest>,
                                                 seq_extend_list: &mut Rc<RefCell<HashMap<i64, (i64, i32, Option<ExtraExtendEntity>)>>>,
                                                 rpc_func: F,
                                                 message_func: MF,
                                                 out_flag: bool,
                                                 si: i64,
                                                 schema: Arc<Schema>)
        where F: Fn(&StoreServiceClient, &BatchVertexEdgeRequest) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<BatchVertexEdgeResponse>>,
              MF: Fn(i64, i32, &BatchVertexEdgeResponse) -> RawMessage + 'static {
        if !worker_req_list.is_empty() {
            let mut result_stream_list = vec![];
            let worker_seq_count = worker_req_list.len();
            for (k, req) in worker_req_list.drain() {
                if let Some(client_manager) = self.client_list.get(&k) {
                    if let Some(res_stream) = {
                        let client = client_manager.get_client();
                        if let Ok(receiver) = rpc_func(client, &req) {
                            Some(receiver)
                        } else {
                            None
                        }
                    } {
                        result_stream_list.push(res_stream);
                    } else {
                        let mut error_message = RawMessage::new(RawMessageType::ERROR);
                        error_message.set_value(ValuePayload::String("get result from remote store fail".to_string()));
                        collector.collect_message(error_message);
                    }
                } else {
                    error!("Cant get client for worker id {:?}", k);
                }
            }

            let seq_count_list = Rc::new(RefCell::new(HashMap::new()));
            for res_stream in result_stream_list.drain(..) {
                let collect = res_stream.collect();
                if let Ok(val_list) = collect.wait() {
                    for mut v in val_list.into_iter() {
                        let seq = v.get_seq();
                        if let Some((id, label_id, extend_entity)) = seq_extend_list.borrow_mut().get(&seq) {
                            let mut message = message_func(*id, *label_id, &v);
                            if let Some(ref mut dedup) = dedup_manager {
                                if !dedup.check_dedup(message.get_id()) {
                                    continue;
                                }
                            }
                            if v.get_edge_props().len() > 0 {
                                if let Some(prop_list) = parse_property_entity_list(v.get_edge_label_id() as u32,
                                                                                    v.take_edge_props(),
                                                                                    &schema) {
                                    for (pid, pval) in prop_list.into_iter() {
                                        message.add_native_property(pid, pval);
                                    }
                                }
                            }
                            if !filter_manager.filter_message(&message) {
                                continue;
                            }
                            if range_limit > 0 {
                                let mut seq_count_list_mut = seq_count_list.borrow_mut();
                                let curr_count = seq_count_list_mut.entry(seq).or_insert(0);
                                if *curr_count > range_limit {
                                    continue;
                                }
                                *curr_count += 1;
                            }
                            if let Some(entity) = extend_entity {
                                message.set_extend_entity(entity.clone());
                            }
                            collector.collect_message(after_requirement.process_requirement(message));
                        }
                    }
                }
            }
        }
    }

    pub fn build_batch_request(&self,
                               si: i64,
                               label_list: &Vec<u32>,
                               worker_req_list: &mut HashMap<i64, BatchVertexEdgeRequest>,
                               seq_id: i64,
                               m: &mut RawMessage,
                               process_index: i64,
                               edge_prop_flag: bool,
                               range_limit: usize) {
        let batch_req = worker_req_list.entry(process_index).or_insert(BatchVertexEdgeRequest::new());
        if !label_list.is_empty() && batch_req.get_label_list().is_empty() {
            batch_req.set_label_list(label_list.to_vec());
        }
        batch_req.set_edge_prop_flag(edge_prop_flag);
        batch_req.set_limit_count(range_limit as u64);

        let mut vid = VertexId::new();
        vid.set_id(m.get_id());
        vid.set_typeId(m.get_label_id());
        batch_req.mut_vertex_id().push(vid);
        batch_req.mut_seq().push(seq_id);
        batch_req.set_snapshot_id(si);
    }

    pub fn get_partition_per_process(&self) -> u64 {
        self.partition_per_process
    }

    pub fn get_worker_index(&self) -> i64 {
        self.worker_index
    }
    pub fn get_partition_num(&self) -> u64 {
        self.partition_num
    }

    pub fn get_client_list(&self) -> &HashMap<i64, StoreClientManager> {
        &self.client_list
    }

    pub fn query_vertex_out_count(&self,
                                  key: i64,
                                  req: &BatchVertexEdgeRequest)
                                  -> Option<::grpcio::Result<::grpcio::ClientSStreamReceiver<BatchVertexCountResponse>>> {
        if let Some(client) = self.client_list.get(&key) {
            return Some(client.get_client().get_batch_out_count(req));
        }

        return None;
    }

    pub fn query_vertex_in_count(&self,
                                 key: i64,
                                 req: &BatchVertexEdgeRequest)
                                 -> Option<::grpcio::Result<::grpcio::ClientSStreamReceiver<BatchVertexCountResponse>>> {
        if let Some(client) = self.client_list.get(&key) {
            return Some(client.get_client().get_batch_in_count(req));
        }

        return None;
    }
}
