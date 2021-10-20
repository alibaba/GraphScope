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

use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

use maxgraph_common::proto::gremlin_service::*;
use maxgraph_common::proto::gremlin_service_grpc::*;

use super::*;
use server::query_manager::QueryManager;

use pegasus::Pegasus;
use rpc::SinkResult;
use rpc::SinkError;

use grpcio::RpcContext;
use grpcio::UnarySink;
use store::task_partition_manager::TaskPartitionManager;

#[derive(Clone)]
pub struct MaxGraphCtrlServiceImpl {
    queries: QueryManager,
    pegasus_runtime: Arc<Option<Pegasus>>,
}

impl MaxGraphCtrlServiceImpl {
    pub fn new_service(queries: QueryManager, pegasus_runtime: Arc<Option<Pegasus>>) -> ::grpcio::Service {
        let service = MaxGraphCtrlServiceImpl { queries, pegasus_runtime };
        create_max_graph_ctrl_service(service)
    }
}

impl MaxGraphCtrlService for MaxGraphCtrlServiceImpl {
    fn show_process_list(&mut self, ctx: RpcContext, _: ShowProcessListRequest, sink: UnarySink<ShowProcessListResponse>) {
        info!("receive showProcessList request");
        let mut resp = ShowProcessListResponse::new();
        for (query_id, script, elapsed_nano, dataflow_id, front_id) in self.queries.dump() {
            let mut q = RunningQuery::new();
            q.set_query_id(query_id);
            q.set_script(script);
            q.set_elapsed_nano(elapsed_nano as i64);
            q.set_dataflow_id(dataflow_id);
            q.set_front_id(front_id);
            resp.mut_queries().push(q);
        }
        sink.sink_result(&ctx, resp);
    }

    fn cancel_dataflow(&mut self, ctx: RpcContext, req: CancelDataflowRequest, sink: UnarySink<CancelDataflowResponse>) {
        let query_id = req.query_id;
        info!("receive cancel dataflow [{}]", query_id);
        if let Some(dataflow_info) = self.queries.get_dataflow_info(&query_id) {
            let timeout = dataflow_info.get_dataflow_timeout().unwrap();
            timeout.store(0, Ordering::Relaxed);
            sink_success_response(sink, ctx);
        } else {
            sink.sink_error(&ctx, "Query not found".to_owned());
        }
    }

    fn cancel_dataflow_by_front(&mut self, ctx: RpcContext, req: CancelDataflowByFrontRequest, sink: UnarySink<CancelDataflowResponse>) {
        let dataflows = self.queries.get_dataflow_of_front(req.get_front_id());
        info!("cancel dataflow of front {}: {:?}", req.get_front_id(), dataflows);

        for (query_id, dataflow_info) in dataflows {
            let timeout = dataflow_info.get_dataflow_timeout().unwrap();
            timeout.store(0, Ordering::Relaxed);
        }
        sink_success_response(sink, ctx);
    }
}

#[inline]
fn sink_success_response(sink: UnarySink<CancelDataflowResponse>, ctx: RpcContext) {
    let mut resp = CancelDataflowResponse::new();
    resp.set_success(true);
    sink.sink_result(&ctx, resp);
}
