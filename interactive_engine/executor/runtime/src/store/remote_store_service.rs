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

use store::remote_store_client::StoreClientManager;
use store::StoreOperatorType;

use dataflow::builder::MessageCollector;
use dataflow::message::{RawMessage, RawMessageType, ValuePayload, ExtraExtendEntity, PropertyEntity};
use dataflow::message::primitive::Write;
use dataflow::manager::requirement::RequirementManager;
use dataflow::manager::filter::FilterManager;
use dataflow::manager::dedup::DedupManager;

use maxgraph_store::api::prelude::{MVGraph, Vertex, Edge, Property};
use maxgraph_store::api::{SnapshotId, MVGraphQuery};
use maxgraph_common::proto::gremlin_query::VertexId;
use maxgraph_common::proto::remote_api_grpc::RemoteStoreServiceClient;
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
use maxgraph_common::proto::store_api::GraphEdgeReponse;
use maxgraph_common::proto::remote_api::{BatchVerticesEdgesRequest, VerticesRequest, BatchVerticesEdgesResponse, BatchVerticesCountResponse, QueryRequest, QueryCountRequest, OutputCondition, CountResponse};
use maxgraph_common::proto::message::LogicalCompare;
use maxgraph_common::proto::gremlin_query::VertexResponse;
use protobuf::RepeatedField;
use store::LocalStoreEdge;
use store::LocalStoreVertex;
use maxgraph_store::api::Condition;

pub struct RemoteStoreServiceManager {
    client_list: HashMap<i64, StoreClientManager>,
    partition_process_list: HashMap<u32, u32>,
}

impl Clone for RemoteStoreServiceManager {
    fn clone(&self) -> Self {
        RemoteStoreServiceManager {
            client_list: self.client_list.clone(),
            partition_process_list: self.partition_process_list.clone(),
        }
    }
}

impl RemoteStoreServiceManager {
    pub fn new(partition_process_list: HashMap<u32, u32>,
               store_ip_list: Vec<RuntimeAddress>) -> Self {
        if store_ip_list.is_empty() {
            RemoteStoreServiceManager {
                client_list: HashMap::new(),
                partition_process_list: HashMap::new(),
            }
        } else {
            let mut client_list = HashMap::new();
            let mut index = 0;
            let store_count = store_ip_list.len() as u64;
            for address in store_ip_list {
                client_list.insert(index, StoreClientManager::new(address.get_ip().to_string(), address.get_port().clone()));
                index += 1;
            }
            RemoteStoreServiceManager {
                client_list,
                partition_process_list
            }
        }
    }

    pub fn empty() -> Self {
        RemoteStoreServiceManager {
            client_list: HashMap::new(),
            partition_process_list: HashMap::new(),
        }
    }

    pub fn get_partition_process_list(&self) -> &HashMap<u32, u32> {
        &self.partition_process_list
    }

    pub fn get_client_list(&self) -> &HashMap<i64, StoreClientManager> {
        &self.client_list
    }

    // TODO: modify batch out request as: query vertices group by partition_ids
    pub fn build_batch_in_out_request(&self,
                                      si: i64,
                                      label_list: &Vec<u32>,
                                      worker_req_list: &mut HashMap<i64, BatchVerticesEdgesRequest>,
                                      vid: i64,
                                      process_index: i64,
                                      condition: Option<&Condition>,
                                      dedup_props: Option<&Vec<u32>>,
                                      output_prop_ids: Option<&Vec<u32>>,
                                      range_limit: usize,
                                      vertex_flag: bool,
                                      partition_ids: &Vec<u32>) {
        let batch_req = worker_req_list.entry(process_index).or_insert(BatchVerticesEdgesRequest::new());
        if !label_list.is_empty() && batch_req.get_label_list().is_empty() {
            batch_req.set_label_list(label_list.to_vec());
        }
        if let Some(condition) = condition{
            if batch_req.get_logical_compare().is_empty() {
                let logical_compare = self.condition_to_logical_compare(condition);
                batch_req.set_logical_compare(RepeatedField::from_vec(logical_compare.to_vec()));
            }
        }
        if batch_req.get_partition_ids().is_empty() && !partition_ids.is_empty() {
            batch_req.set_partition_ids(partition_ids.to_vec());
        }
        if !batch_req.has_conditions() {
            let mut result_condition_req = OutputCondition::new();
            result_condition_req.set_limit(range_limit as u64);
            if let Some(dedup_props_val) = dedup_props {
                result_condition_req.set_dedup_prop_ids(dedup_props_val.to_vec());
            }
            if let Some(output_prop_ids) = output_prop_ids {
                result_condition_req.set_prop_ids(output_prop_ids.to_vec());
            } else {
                result_condition_req.set_prop_flag(true);
            }
            batch_req.set_conditions(result_condition_req);
        }
        if vertex_flag {
            batch_req.set_vertex_flag(true);
        }
        batch_req.mut_vertex_id().push(vid);
        batch_req.set_snapshot_id(si);
    }

    // TODO: to confirm if there would be different vertex sets in different partitions (in-direction)
    pub fn build_batch_in_out_request_by_partition(&self,
                                      si: i64,
                                      label_list: &Vec<u32>,
                                      worker_req_list: &mut HashMap<i64, BatchVerticesEdgesRequest>,
                                      vids: &Vec<i64>,
                                      process_index: i64,
                                      condition: Option<&Condition>,
                                      dedup_props: Option<&Vec<u32>>,
                                      output_prop_ids: Option<&Vec<u32>>,
                                      range_limit: usize,
                                      vertex_flag: bool,
                                      partition_id: u32) {
        let batch_req = worker_req_list.entry(process_index).or_insert(BatchVerticesEdgesRequest::new());
        if !label_list.is_empty() && batch_req.get_label_list().is_empty() {
            batch_req.set_label_list(label_list.to_vec());
        }
        if let Some(condition) = condition{
            if batch_req.get_logical_compare().is_empty() {
                let logical_compare = self.condition_to_logical_compare(condition);
                batch_req.set_logical_compare(RepeatedField::from_vec(logical_compare.to_vec()));
            }
        }
        if batch_req.get_vertex_id().is_empty() && !vids.is_empty() {
            batch_req.set_vertex_id(vids.to_vec());
        }
        if !batch_req.has_conditions() {
            let mut result_condition_req = OutputCondition::new();
            result_condition_req.set_limit(range_limit as u64);
            if let Some(dedup_props_val) = dedup_props {
                result_condition_req.set_dedup_prop_ids(dedup_props_val.to_vec());
            }
            if let Some(output_prop_ids) = output_prop_ids {
                result_condition_req.set_prop_ids(output_prop_ids.to_vec());
            } else {
                result_condition_req.set_prop_flag(true);
            }
            batch_req.set_conditions(result_condition_req);
        }
        if vertex_flag {
            batch_req.set_vertex_flag(true);
        }
        batch_req.mut_partition_ids().push(partition_id);
        batch_req.set_snapshot_id(si);
    }

    pub fn build_get_vertices_request(&self,
                                      si: i64,
                                      worker_req_list: &mut HashMap<i64, HashMap<Option<u32>, VerticesRequest>>,
                                      vid: i64,
                                      label: Option<u32>,
                                      process_index: i64,
                                      output_prop_ids: Option<&Vec<u32>>) {
        let label_vertex_req_list = worker_req_list.entry(process_index).or_insert(HashMap::new());
        let vertex_req = label_vertex_req_list.entry(label).or_insert(VerticesRequest::new());
        if let Some(label)=label {
            vertex_req.set_type_id(label);
        }
        vertex_req.set_snapshot_id(si);
        vertex_req.mut_ids().push(vid);
        if let Some(output_prop_ids) = output_prop_ids {
            if !output_prop_ids.is_empty() {
                if vertex_req.get_prop_ids().is_empty() {
                    for prop_id in output_prop_ids.iter() {
                        vertex_req.mut_prop_ids().push(*prop_id as i32);
                    }
                }
            }
        } else {
            vertex_req.set_prop_flag(true);
        }
    }

    pub fn build_query_request(&self,
                               si: i64,
                               label_list: &Vec<u32>,
                               worker_req_list: &mut HashMap<i64, QueryRequest>,
                               process_index: i64,
                               condition: Option<&Condition>,
                               dedup_props: Option<&Vec<u32>>,
                               output_prop_ids: Option<&Vec<u32>>,
                               range_limit: usize,
                               partition_ids: &Vec<u32>) {
        let batch_req = worker_req_list.entry(process_index).or_insert(QueryRequest::new());
        if !label_list.is_empty() && batch_req.get_label_id().is_empty() {
            batch_req.set_label_id(label_list.to_vec());
        }
        if let Some(condition) = condition{
            if batch_req.get_logical_compare().is_empty() {
                let logical_compare = self.condition_to_logical_compare(condition);
                batch_req.set_logical_compare(RepeatedField::from_vec(logical_compare.to_vec()));
            }
        }
        if !batch_req.has_conditions() {
            let mut result_condition_req = OutputCondition::new();
            result_condition_req.set_limit(range_limit as u64);
            if let Some(dedup_props_val) = dedup_props {
                result_condition_req.set_dedup_prop_ids(dedup_props_val.to_vec());
            }
            if let Some(output_prop_ids) = output_prop_ids {
                result_condition_req.set_prop_ids(output_prop_ids.to_vec());
            } else {
                result_condition_req.set_prop_flag(true);
            }
            batch_req.set_conditions(result_condition_req);
        }
        if !partition_ids.is_empty() && batch_req.get_partition_ids().is_empty(){
            batch_req.set_partition_ids(partition_ids.to_vec());
        }
        batch_req.set_snapshot_id(si);
    }

    pub fn build_query_count_request(&self,
                               si: i64,
                               label_list: &Vec<u32>,
                               worker_req_list: &mut HashMap<i64, QueryCountRequest>,
                               process_index: i64,
                               condition: Option<&Condition>,
                               partition_ids: &Vec<u32>) {
        let batch_req = worker_req_list.entry(process_index).or_insert(QueryCountRequest::new());
        if !label_list.is_empty() && batch_req.get_label_id().is_empty() {
            batch_req.set_label_id(label_list.to_vec());
        }
        if let Some(condition) = condition{
            if batch_req.get_logical_compare().is_empty() {
                let logical_compare = self.condition_to_logical_compare(condition);
                batch_req.set_logical_compare(RepeatedField::from_vec(logical_compare.to_vec()));
            }
        }
        if !partition_ids.is_empty() && batch_req.get_partition_ids().is_empty(){
            batch_req.set_partition_ids(partition_ids.to_vec());
        }
        batch_req.set_snapshot_id(si);
    }

    pub fn process_remote_requests<'a, F: Copy, T, U>(&self,
                                                      worker_req_list: &mut HashMap<i64, T>,
                                                      rpc_func: F, )
                                                      -> Vec<::grpcio::ClientSStreamReceiver<U>>
        where F: Fn(&RemoteStoreServiceClient, &T) -> ::grpcio::Result<::grpcio::ClientSStreamReceiver<U>> {
        let mut result_stream_list = Vec::with_capacity(worker_req_list.len());
        if !worker_req_list.is_empty() {
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
                        error!("connect to store server fail {:?}", k);
                    }
                } else {
                    error!("Cant get client for worker id {:?}", k);
                }
            }
        }
        return result_stream_list;
    }

    pub fn parse_in_edges_result(&self,
                                 result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<BatchVerticesEdgesResponse>>,
                                 schema: Arc<Schema>,
                                 all_in_list: &mut HashMap<i64, Vec<LocalStoreEdge>>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for mut v in val_list.into_iter() {
                    let edge = self.parse_edge_result(&mut v, &schema);
                    let in_list = all_in_list.entry(v.get_dst_id().get_id()).or_insert(vec![]);
                    in_list.push(edge);
                }
            }
        }
    }

    pub fn parse_out_edges_result(&self,
                                  result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<BatchVerticesEdgesResponse>>,
                                  schema: Arc<Schema>,
                                  all_out_list: &mut HashMap<i64, Vec<LocalStoreEdge>>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for mut v in val_list.into_iter() {
                    let edge = self.parse_edge_result(&mut v, &schema);
                    let out_list = all_out_list.entry(v.get_src_id().get_id()).or_insert(vec![]);
                    out_list.push(edge);
                }
            }
        }
    }

    fn parse_edge_result(&self,
                         e: &mut BatchVerticesEdgesResponse,
                         schema: &Arc<Schema>, ) -> LocalStoreEdge {
        let src = LocalStoreVertex::new(e.get_src_id().get_id(), e.get_src_id().get_typeId() as u32);
        let dst = LocalStoreVertex::new(e.get_dst_id().get_id(), e.get_dst_id().get_typeId() as u32);
        let mut edge = LocalStoreEdge::new(src, dst, e.get_type_id() as u32, e.get_edge_id());
        if e.get_pros().len() > 0 {
            if let Some(prop_list) = parse_property_entity_list(e.get_type_id() as u32,
                                                                e.take_pros(),
                                                                schema) {
                for (pid, pval) in prop_list.into_iter() {
                    edge.add_property(pid as u32, pval);
                }
            }
        }
        edge
    }

    pub fn parse_in_vertices_result(&self,
                                    result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<BatchVerticesEdgesResponse>>,
                                    schema: Arc<Schema>,
                                    all_in_list: &mut HashMap<i64, Vec<LocalStoreVertex>>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for v in val_list.into_iter() {
                    let vertex = LocalStoreVertex::new(v.get_src_id().get_id(), v.get_src_id().get_typeId() as u32);
                    let in_list = all_in_list.entry(v.get_dst_id().get_id()).or_insert(vec![]);
                    in_list.push(vertex);
                }
            }
        }
    }

    pub fn parse_out_vertices_result(&self,
                                     result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<BatchVerticesEdgesResponse>>,
                                     schema: Arc<Schema>,
                                     all_out_list: &mut HashMap<i64, Vec<LocalStoreVertex>>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for v in val_list.into_iter() {
                    let vertex = LocalStoreVertex::new(v.get_dst_id().get_id(), v.get_dst_id().get_typeId() as u32);
                    let out_list = all_out_list.entry(v.get_src_id().get_id()).or_insert(vec![]);
                    out_list.push(vertex);
                }
            }
        }
    }

    pub fn parse_vertex_count_result(&self,
                                     result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<BatchVerticesCountResponse>>,
                                     vertex_count_list: &mut HashMap<i64, usize>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for v in val_list.into_iter() {
                    let vid = v.get_vid();
                    let count_val = v.get_count_val() as usize;
                    let curr_count = vertex_count_list.entry(vid).or_insert(0);
                    *curr_count += count_val;
                }
            }
        }
    }

    pub fn parse_graph_edge_result(&self,
                                   result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<GraphEdgeReponse>>,
                                   schema: Arc<Schema>,
                                   edge_list: &mut Vec<LocalStoreEdge>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for mut e in val_list.into_iter() {
                    let src = LocalStoreVertex::new(e.get_src_id().get_id(), e.get_src_id().get_typeId() as u32);
                    let dst = LocalStoreVertex::new(e.get_dst_id().get_id(), e.get_dst_id().get_typeId() as u32);
                    let mut edge = LocalStoreEdge::new(src, dst, e.get_type_id() as u32, e.get_edge_id());
                    if e.get_pros().len() > 0 {
                        if let Some(prop_list) = parse_property_entity_list(e.get_type_id() as u32,
                                                                            e.take_pros(),
                                                                            &schema) {
                            for (pid, pval) in prop_list.into_iter() {
                                edge.add_property(pid as u32, pval);
                            }
                        }
                    }
                    edge_list.push(edge);
                }
            }
        }
    }

    pub fn parse_vertex_result(&self,
                               result_stream_list: &mut Vec<::grpcio::ClientSStreamReceiver<VertexResponse>>,
                               schema: Arc<Schema>,
                               vertex_list: &mut Vec<LocalStoreVertex>, ) {
        for res_stream in result_stream_list.drain(..) {
            let collect = res_stream.collect();
            if let Ok(val_list) = collect.wait() {
                for mut res_vertex in val_list.into_iter() {
                    let vid = res_vertex.get_id();
                    let mut vertex = LocalStoreVertex::new(vid.get_id(), vid.get_typeId() as u32);
                    if let Some(proplist) = parse_property_entity_list(vid.get_typeId() as u32, res_vertex.take_pros(), &schema) {
                        vertex.add_properties(proplist);
                    }
                    vertex_list.push(vertex);
                }
            }
        }
    }

    pub fn parse_count_result(&self,
                                     result_list: &mut Vec<CountResponse>,
                                     vertex_count: &mut u64) {
        for res in result_list.drain(..) {
            *vertex_count +=  res.get_count_val() as u64;
        }
    }

    fn condition_to_logical_compare(&self, condition: &Condition) -> Vec<LogicalCompare> {
        //TODO
        return vec![LogicalCompare::new()];
    }
}
