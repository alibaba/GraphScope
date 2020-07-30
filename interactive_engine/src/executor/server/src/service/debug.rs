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

#![allow(dead_code)]
#![allow(unused_variables)]
use maxgraph_common::proto::debug::*;
use maxgraph_common::proto::debug_grpc::*;
use maxgraph_common::proto::common::*;
use maxgraph_common::proto::schema::*;
use grpcio::*;
use crate::store::Store;
use futures::{Future};
use std::sync::Arc;

pub struct DebugService {
    store: Arc<Store>,
}

impl DebugService {
    pub fn new(store: Arc<Store>) -> Self {
        DebugService {
            store,
        }
    }
}

impl Clone for DebugService {
    fn clone(&self) -> Self {
        DebugService {
            store: self.store.clone(),
        }
    }
}

impl DebugServiceApi for DebugService {
    fn get_server_info(&mut self, ctx: RpcContext, _req: Empty, sink: UnarySink<ServerInfo>) {
        let mut server_info = ServerInfo::new();
        let config = self.store.get_config();
        server_info.set_partition_num(config.partition_num);
        server_info.set_work_id(config.worker_id);
        server_info.set_download_thread_count(config.download_thread_count);
        server_info.set_load_thread_count(config.load_thread_count);
        server_info.set_graph_name(config.graph_name.clone());
        server_info.set_local_data_root(config.local_data_root.clone());
        server_info.set_zk_url(config.zk_url.clone());
        let f = sink.success(server_info)
            .map_err(|e| { error!("get server info failed, {:?}", e); });
        ctx.spawn(f);
    }

    fn get_graph_info(&mut self, ctx: RpcContext, _req: Empty, sink: UnarySink<GraphInfo>) {
        unimplemented!()
    }

    fn get_vertex(&mut self, ctx: RpcContext, req: GetVertexRequest, sink: ServerStreamingSink<VertexProto>) {
        unimplemented!()
    }

    fn scan_vertex(&mut self, ctx: RpcContext, req: ScanVertexRequest, sink: ServerStreamingSink<VertexProto>) {
        unimplemented!()
    }

    fn get_out_edges(&mut self, ctx: RpcContext, req: GetOutEdgesRequest, sink: ServerStreamingSink<EdgeProto>) {
        unimplemented!()
    }

    fn get_in_edges(&mut self, ctx: RpcContext, req: GetInEdgesRequest, sink: ServerStreamingSink<EdgeProto>) {
        unimplemented!()
    }

    fn get_schema(&mut self, ctx: RpcContext, req: GetSchemaRequest, sink: UnarySink<SchemaProto>) {
        unimplemented!()
    }
}
