use std::ffi::{c_void, CStr};
use maxgraph_store::db::common::bytes::util::parse_pb;
use maxgraph_store::db::api::GraphConfigBuilder;
use std::sync::Arc;
use maxgraph_store::api::PartitionId;
use maxgraph_store::db::common::unsafe_util::to_mut;
use maxgraph_store::db::graph::store::GraphStore;
use std::os::raw::c_char;
use std::str;
use crate::executor::pegasus::executor_server::ExecutorServer;
use crate::executor::pegasus::jna_server_response::{JnaEngineServerResponse, JnaRpcServerPortResponse};
use maxgraph_store::db::proto::model::ConfigPb;

pub type ExecutorHandle = *const c_void;
pub type GraphHandle = *const c_void;

#[no_mangle]
pub extern fn openExecutorServer(config_bytes: *const u8, len: usize) -> ExecutorHandle {
    let buf = unsafe { ::std::slice::from_raw_parts(config_bytes, len) };
    let proto = parse_pb::<ConfigPb>(buf).expect("parse config pb failed");
    let mut config_builder = GraphConfigBuilder::new();
    config_builder.set_storage_options(proto.get_configs().clone());
    let config = Arc::new(config_builder.build());
    let handle = Box::new(ExecutorServer::new(config).unwrap());
    let ret = Box::into_raw(handle);
    ret as ExecutorHandle
}

#[no_mangle]
pub extern fn addGraphPartition(executor_handle: ExecutorHandle, partition_id: PartitionId, graph_handle: GraphHandle) {
    let executor_ptr = unsafe {
        to_mut(&*(executor_handle as *const ExecutorServer))
    };
    let graph_ptr = unsafe {
        Arc::from_raw(&*(graph_handle as *const GraphStore))
    };
    executor_ptr.add_graph_partition(partition_id, graph_ptr);
}

#[no_mangle]
pub extern fn addPartitionWorkerMapping(executor_handle: ExecutorHandle, partition_id: PartitionId, worker_id: i32) {
    let executor_ptr = unsafe {
        to_mut(&*(executor_handle as *const ExecutorServer))
    };
    executor_ptr.add_partition_worker_mapping(partition_id, worker_id);
}

#[no_mangle]
pub extern fn startEngineServer(executor_handle: ExecutorHandle) -> Box<JnaEngineServerResponse> {
    let executor_ptr = unsafe {
        to_mut(&*(executor_handle as *const ExecutorServer))
    };
    match executor_ptr.start() {
        Ok(addr) => {
            JnaEngineServerResponse::new_success(addr)
        },
        Err(e) => {
            JnaEngineServerResponse::new_fail(format!("{:?}", e))
        },
    }
}

#[no_mangle]
pub extern fn startRpcServer(executor_handle: ExecutorHandle) -> Box<JnaRpcServerPortResponse> {
    let executor_ptr = unsafe {
        to_mut(&*(executor_handle as *const ExecutorServer))
    };
    let (ctrl_and_async_service_port, gremlin_port) = executor_ptr.start_rpc();
    Box::new(JnaRpcServerPortResponse::new(gremlin_port as i32,
                                  ctrl_and_async_service_port as i32,
                                  ctrl_and_async_service_port as i32))
}

#[no_mangle]
pub extern fn connectEngineServerList(executor_handle: ExecutorHandle, addrs: *const c_char) {
    let slice =  unsafe { CStr::from_ptr(addrs) }.to_bytes();
    let addrs_str = str::from_utf8(slice).unwrap();
    let address_list =  addrs_str.split(",").map(|s| s.to_string()).collect::<Vec<String>>();
    let executor_ptr = unsafe {
        to_mut(&*(executor_handle as *const ExecutorServer))
    };
    executor_ptr.engine_connect(address_list);
}

#[no_mangle]
pub extern fn stopEngineServer(_handle: ExecutorHandle) {}

#[no_mangle]
pub extern fn stopRpcServer(_handle: ExecutorHandle) {}
