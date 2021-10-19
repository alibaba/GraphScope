use std::os::raw::{c_void, c_char};
use maxgraph_store::db::common::bytes::util::parse_pb;
use maxgraph_store::db::api::GraphConfigBuilder;
use std::sync::Arc;
use maxgraph_store::db::common::unsafe_util::to_mut;
use maxgraph_store::db::graph::store::GraphStore;
use std::ffi::CStr;
use std::net::{SocketAddr, ToSocketAddrs};
use crate::executor::gaia::gaia_server::GaiaServer;
use crate::executor::gaia::engine_ports_response::EnginePortsResponse;
use maxgraph_store::db::proto::model::ConfigPb;

pub type EngineHandle = *const c_void;
pub type GraphHandle = *const c_void;

#[no_mangle]
pub extern fn initialize(config_bytes: *const u8, len: usize) -> EngineHandle {
    let config_buf = unsafe { ::std::slice::from_raw_parts(config_bytes, len) };
    let config_pb = parse_pb::<ConfigPb>(config_buf).expect("parse config pb failed");
    let mut config_builder = GraphConfigBuilder::new();
    config_builder.set_storage_options(config_pb.get_configs().clone());
    let config = Arc::new(config_builder.build());
    let handle = Box::new(GaiaServer::new(config));
    Box::into_raw(handle) as EngineHandle
}

#[no_mangle]
pub extern fn addPartition(engine_handle: EngineHandle, partition_id: i32, graph_handle: GraphHandle) {
    let engine_ptr = unsafe {
        to_mut(&*(engine_handle as *const GaiaServer))
    };
    let graph_ptr = unsafe {
        Arc::from_raw(&*(graph_handle as *const GraphStore))
    };
    engine_ptr.add_partition(partition_id as u32, graph_ptr);
}

#[no_mangle]
pub extern fn updatePartitionRouting(engine_handle: EngineHandle, partition_id: i32, server_id: i32) {
    let engine_ptr = unsafe {
        to_mut(&*(engine_handle as *const GaiaServer))
    };
    engine_ptr.update_partition_routing(partition_id as u32, server_id as u32);
}

#[no_mangle]
pub extern fn startEngine(engine_handle: EngineHandle) -> Box<EnginePortsResponse> {
    let engine_ptr = unsafe {
        to_mut(&*(engine_handle as *const GaiaServer))
    };
    match engine_ptr.start() {
        Ok((engine_port, server_port)) => {
            EnginePortsResponse::new(engine_port as i32, server_port as i32)
        },
        Err(e) => {
            let msg = format!("{:?}", e);
            EnginePortsResponse::new_with_error(&msg)
        },
    }
}

#[no_mangle]
pub extern fn stopEngine(engine_handle: EngineHandle) {
    let engine_ptr = unsafe {
        to_mut(&*(engine_handle as *const GaiaServer))
    };
    engine_ptr.stop();
}

#[no_mangle]
pub extern fn updatePeerView(engine_handle: EngineHandle, peer_view_string_raw: *const c_char) {
    let slice =  unsafe { CStr::from_ptr(peer_view_string_raw) }.to_bytes();
    let peer_view_string = std::str::from_utf8(slice).unwrap();
    let peer_view = peer_view_string.split(",").map(|item| {
        let mut fields = item.split("#");
        let id = fields.next().unwrap().parse::<u64>().unwrap();
        let addr_str = fields.next().unwrap();
        let mut addr_iter = addr_str.to_socket_addrs().expect(format!("parse addr failed [{}]", addr_str).as_str());
        (id, addr_iter.next().unwrap())
    }).collect::<Vec<(u64, SocketAddr)>>();
    let engine_ptr = unsafe {
        to_mut(&*(engine_handle as *const GaiaServer))
    };
    engine_ptr.update_peer_view(peer_view);
}
