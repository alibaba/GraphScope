//
//! Copyright 2022 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
//!

use std::ffi::CStr;
use std::net::{SocketAddr, ToSocketAddrs};
use std::os::raw::{c_char, c_void};
use std::sync::Arc;

use maxgraph_store::db::api::GraphConfigBuilder;
use maxgraph_store::db::common::bytes::util::parse_pb;
use maxgraph_store::db::common::unsafe_util::to_mut;
use maxgraph_store::db::graph::store::GraphStore;
use maxgraph_store::db::proto::model::ConfigPb;

use crate::executor::gaia::engine_ports_response::EnginePortsResponse;
use crate::executor::gaia::gaia_server::GaiaServer;
use pegasus_network::config::ServerAddr;

pub type EngineHandle = *const c_void;
pub type GraphHandle = *const c_void;

#[no_mangle]
pub extern "C" fn initialize(config_bytes: *const u8, len: usize) -> EngineHandle {
    let config_buf = unsafe { ::std::slice::from_raw_parts(config_bytes, len) };
    let config_pb = parse_pb::<ConfigPb>(config_buf).expect("parse config pb failed");
    let mut config_builder = GraphConfigBuilder::new();
    config_builder.set_storage_options(config_pb.get_configs().clone());
    let config = Arc::new(config_builder.build());
    let handle = Box::new(GaiaServer::new(config));
    Box::into_raw(handle) as EngineHandle
}

#[no_mangle]
pub extern "C" fn addPartition(engine_handle: EngineHandle, partition_id: i32, graph_handle: GraphHandle) {
    let engine_ptr = unsafe { to_mut(&*(engine_handle as *const GaiaServer)) };
    let graph_ptr = unsafe { Arc::from_raw(&*(graph_handle as *const GraphStore)) };
    engine_ptr.add_partition(partition_id as u32, graph_ptr);
}

#[no_mangle]
pub extern "C" fn updatePartitionRouting(engine_handle: EngineHandle, partition_id: i32, server_id: i32) {
    let engine_ptr = unsafe { to_mut(&*(engine_handle as *const GaiaServer)) };
    engine_ptr.update_partition_routing(partition_id as u32, server_id as u32);
}

#[no_mangle]
pub extern "C" fn startEngine(engine_handle: EngineHandle) -> Box<EnginePortsResponse> {
    let engine_ptr = unsafe { to_mut(&*(engine_handle as *const GaiaServer)) };
    match engine_ptr.start() {
        Ok((engine_port, server_port)) => EnginePortsResponse::new(engine_port as i32, server_port as i32),
        Err(e) => {
            let msg = format!("{:?}", e);
            EnginePortsResponse::new_with_error(&msg)
        }
    }
}

#[no_mangle]
pub extern "C" fn stopEngine(engine_handle: EngineHandle) {
    let engine_ptr = unsafe { to_mut(&*(engine_handle as *const GaiaServer)) };
    engine_ptr.stop();
}

#[no_mangle]
pub extern "C" fn updatePeerView(engine_handle: EngineHandle, peer_view_string_raw: *const c_char) {
    let slice = unsafe { CStr::from_ptr(peer_view_string_raw) }.to_bytes();
    let peer_view_string = std::str::from_utf8(slice).unwrap();
    let peer_view = peer_view_string
        .split(",")
        .map(|item| {
            let mut fields = item.split("#");
            let id = fields.next().unwrap().parse::<u64>().unwrap();
            let addr_str = fields.next().unwrap();
            let mut fileds = addr_str.split(":");
            let hostname = fileds.next().unwrap();
            let port = fileds.next().unwrap().parse::<u16>().unwrap();
            (id, ServerAddr::new(String::from(hostname), port))
        })
        .collect::<Vec<(u64, ServerAddr)>>();
    let engine_ptr = unsafe { to_mut(&*(engine_handle as *const GaiaServer)) };
    engine_ptr.update_peer_view(peer_view);
}
