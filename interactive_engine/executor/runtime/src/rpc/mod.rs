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


use maxgraph_common::proto::query_flow::*;
//use maxgraph_common::proto::message;
//use maxgraph_common::proto::message::VariantType;
use maxgraph_common::proto::message::{QueryResponse, OperationResponse};
use maxgraph_common::proto::gremlin_service::CancelDataflowResponse;
use grpcio::{UnarySink, ServerStreamingSink, RpcContext, WriteFlags};
use futures::{Future, Sink, stream::iter_ok};

pub mod rpc_pegasus;

//pub use self::rpc_timely::ctrl_service::MaxGraphCtrlServiceImpl;
//pub use self::rpc_timely::maxgraph_service::MaxGraphServiceImpl;
//pub use self::rpc_timely::async_maxgraph_service::AsyncMaxGraphServiceImpl;
//
//pub use self::rpc_pegasus::ctrl_service::MaxGraphCtrlServiceImpl;
//pub use self::rpc_pegasus::async_maxgraph_service::AsyncMaxGraphServiceImpl;

//use utils::value::{value_to_raw_message, value_bytes_to_raw_message};

/// Rpc messages which can represent error info and can be created from a error message string.
trait FromErrorMessage {
    fn from_str(msg: String) -> Self;
}

impl FromErrorMessage for OperationResponse {
    fn from_str(msg: String) -> Self {
        let mut resp = OperationResponse::new();
        resp.set_success(false);
        resp.set_message(msg);
        resp
    }
}

impl FromErrorMessage for CancelDataflowResponse {
    fn from_str(msg: String) -> Self {
        let mut resp = CancelDataflowResponse::new();
        resp.set_success(false);
        resp.set_message(msg);
        resp
    }
}

impl FromErrorMessage for QueryResponse {
    fn from_str(msg: String) -> Self {
        let mut resp = QueryResponse::new();
        resp.set_error_code(-1);
        resp.set_message(msg);
        resp
    }
}

/// A sink which responses a `FromErrorMessage`.
trait SinkError {
    fn sink_error(self, ctx: &RpcContext, msg: String);
}

/// A sink which responses a single result.
trait SinkResult<T> {
    fn sink_result(self, ctx: &RpcContext, data: T);
}

/// /// A sink which responses streaming results.
trait SinkAll<T> {
    fn sink_all<I>(self, ctx: &RpcContext, data: I)
    where I: IntoIterator<Item = (T, WriteFlags)>,
          <I as IntoIterator>::IntoIter: 'static + Send;
}

impl<T: 'static + FromErrorMessage + Send> SinkError for UnarySink<T> {
    fn sink_error(self, ctx: &RpcContext, msg: String) {
        let resp = T::from_str(msg);
        let f = self.success(resp)
            .map_err(|e| warn!("failed to reply: {:?}", e));
        ctx.spawn(f);
    }
}

impl<T> SinkResult<T> for UnarySink<T> {
    fn sink_result(self, ctx: &RpcContext, resp: T) {
        let f = self.success(resp)
            .map_err(|e| warn!("failed to reply: {:?}", e));
        ctx.spawn(f);
    }
}


impl<T: 'static + FromErrorMessage + Send> SinkError for ServerStreamingSink<T> {
    fn sink_error(self, ctx: &RpcContext, msg: String) {
        let resp = T::from_str(msg);
        let f = self.send_all(iter_ok::<_, grpcio::Error>(Some((resp, WriteFlags::default()))))
            .map(|_| {})
            .map_err(|e| warn!("failed to query: {:?}", e));
        ctx.spawn(f);
    }
}

impl<T: 'static + Send> SinkAll<T> for ServerStreamingSink<T> {
    fn sink_all<I>(self, ctx: &RpcContext, data: I)
    where I: IntoIterator<Item = (T, WriteFlags)>,
          <I as IntoIterator>::IntoIter: 'static + Send
    {
        let f = self.send_all(iter_ok::<_, grpcio::Error>(data))
            .map(|_| {})
            .map_err(|e| warn!("failed to query: {:?}", e));
        ctx.spawn(f);
    }
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


#[inline]
pub fn to_response_new<I: IntoIterator<Item = Vec<u8>>>(messages: I) -> (QueryResponse, WriteFlags) {
    let mut resp = QueryResponse::new();
    for payload in messages.into_iter() {
        resp.mut_value().push(payload);
    }
    (resp, WriteFlags::default())
}
