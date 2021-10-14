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

use std::sync::Arc;
use std::time::Instant;
use std::error::Error;

use grpcio::{RpcContext, UnarySink, ServerStreamingSink, WriteFlags};
use protobuf::Message;

use maxgraph_common::proto::gremlin_service_grpc::*;
use maxgraph_common::proto::query_flow::*;
use maxgraph_common::proto::message::{OperationResponse};
use maxgraph_common::proto::message::QueryResponse;
use maxgraph_common::proto::message::RemoveDataflowRequest;
use maxgraph_common::util::time::*;
use maxgraph_common::util::log::log_query;
use maxgraph_common::util::log::QueryEvent;
use maxgraph_common::util::log::QueryType;
use maxgraph_store::config::StoreConfig;
use maxgraph_store::api::prelude::*;

use dataflow::io::remove_odps_id_cache;


use server::query_manager::{QueryManager, DataflowInfo};
use rpc::SinkError;
use rpc::SinkResult;
use rpc::SinkAll;
use dataflow::message::{ValuePayload, RawMessage, RawMessageType};
use std::collections::HashMap;
use execution::build_empty_router;

pub struct MaxGraphServiceImpl {
    store_config: Arc<StoreConfig>,
    queries: QueryManager,
}

impl MaxGraphServiceImpl {
    pub fn new_service(
        store_config: Arc<StoreConfig>,
        query_manager: QueryManager,
    ) -> ::grpcio::Service {
        let service = MaxGraphServiceImpl {
            store_config,
            queries: query_manager,
        };
        create_max_graph_service(service)
    }
}

impl Clone for MaxGraphServiceImpl {
    fn clone(&self) -> Self {
        MaxGraphServiceImpl {
            store_config: self.store_config.clone(),
            queries: self.queries.clone(),
        }
    }
}

impl MaxGraphService for MaxGraphServiceImpl {
    fn query(&mut self, ctx: RpcContext, _req: QueryFlow, sink: ServerStreamingSink<QueryResponse>) {
        let resp = error_response("unimplemented".to_owned());
        sink.sink_all(&ctx, resp);
    }

    fn prepare(&mut self, ctx: RpcContext, _req: QueryFlow, sink: UnarySink<OperationResponse>) {
        sink.sink_error(&ctx, "unimplemented".to_owned());
    }

    fn query2(&mut self, ctx: RpcContext, _req: Query, sink: ServerStreamingSink<QueryResponse>) {
        let resp = error_response("unimplemented".to_owned());
        sink.sink_all(&ctx, resp);
    }

    fn execute(&mut self, ctx: RpcContext, _req: QueryFlow, sink: ServerStreamingSink<QueryResponse>) {
        let resp = error_response("unimplemented".to_owned());
        sink.sink_all(&ctx, resp);
    }

    fn remove(&mut self, ctx: RpcContext, _req: RemoveDataflowRequest, sink: UnarySink<OperationResponse>) {
        sink.sink_error(&ctx, "unimplemented".to_owned());
    }
}


#[inline]
fn last_operator_is_global(req: &QueryFlow, operator_type: OperatorType) -> bool {
    let last_operator_id = req.get_query_plan().get_operator_id_list().last();
    let last_unary_op = req.get_query_plan().get_unary_op().last();
    if last_unary_op.is_none() || last_operator_id.is_none() {
        return false;
    }
    let base = last_unary_op.unwrap().get_base();
    base.get_operator_type() == operator_type && base.get_id() == *(last_operator_id.unwrap())
}


/// Data stream to response vector.
fn to_response<I: IntoIterator<Item=Vec<u8>>>(messages: I) -> Vec<(QueryResponse, WriteFlags)> {
    let mut responses = vec![];
    let mut resp = QueryResponse::new();
    for payload in messages.into_iter() {
        resp.mut_value().push(payload);
        if resp.value.len() > 10 {
            responses.push((resp, WriteFlags::default()));
            resp = QueryResponse::new();
        }
    }
    if !resp.value.is_empty() {
        responses.push((resp, WriteFlags::default()));
    }
    responses
}

/// Returning a 0 i64 value.
fn zero_response() -> (QueryResponse, WriteFlags) {
    let p = RawMessage::from_value(ValuePayload::Long(0));
    let mut resp = QueryResponse::new();
    let empty_fn = Arc::new(build_empty_router());
    resp.mut_value().push(p.to_proto(Some(empty_fn.as_ref())).write_to_bytes().expect("write zero property"));
    (resp, WriteFlags::default())
}

/// Returning an empty map.
fn empty_map_response() -> (QueryResponse, WriteFlags) {
    let map_message = RawMessage::from_value_type(ValuePayload::Map(vec![]), RawMessageType::MAP);
    let mut resp = QueryResponse::new();
    let empty_fn = Arc::new(build_empty_router());
    resp.mut_value().push(map_message.to_proto(Some(empty_fn.as_ref())).write_to_bytes().expect("write zero property"));
    (resp, WriteFlags::default())
}

/// Create error QueryResponse with a message string.
#[inline]
pub fn error_response(msg: String) -> Vec<(QueryResponse, WriteFlags)> {
    let mut resp = QueryResponse::new();
    resp.set_error_code(-1);
    resp.set_message(msg);
    vec![(resp, Default::default())]
}

#[inline]
fn input_batch_level_to_num(level: &InputBatchLevel) -> usize {
    match level {
        InputBatchLevel::VerySmall => 4,
        InputBatchLevel::Small => 16,
        InputBatchLevel::Medium => 64,
        InputBatchLevel::Large => 256,
        InputBatchLevel::VeryLarge => 1024,
    }
}

/// sink error message when create rpc_timely client failed.
#[inline]
fn sink_error_msg(graph_name: &str, worker_id: u32, query_id: &str, sink: ServerStreamingSink<QueryResponse>, ctx: &RpcContext) {
    log_query(graph_name, worker_id,
              query_id, QueryType::Execute,
              QueryEvent::ExecutorFinish {
                  latency_nano: 0,
                  result_num: 0,
                  success: false,
              });
    let err_msg = "Create rpc_timely client failed, please retry again later";
    error!("{}", err_msg);
    sink.sink_error(ctx, err_msg.to_owned());
}
