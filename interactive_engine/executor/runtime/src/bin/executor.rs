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

#![allow(bare_trait_objects)]
extern crate futures;
extern crate grpcio;
#[macro_use]
extern crate log;
extern crate log4rs;
extern crate maxgraph_common;
extern crate maxgraph_runtime;
extern crate maxgraph_store;
extern crate maxgraph_server;
extern crate protobuf;
extern crate structopt;
extern crate pegasus;

use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use std::env;

use maxgraph_common::proto::query_flow::*;
use maxgraph_common::proto::hb::*;
use maxgraph_runtime::server::manager::*;
use maxgraph_runtime::server::query_manager::*;
use maxgraph_common::util::log4rs::init_log4rs;
use maxgraph_store::config::{StoreConfig, VINEYARD_GRAPH};
use maxgraph_store::api::prelude::*;
use protobuf::Message;
use maxgraph_common::util;
use maxgraph_runtime::{server::RuntimeInfo};
use grpcio::{ServerBuilder, EnvBuilder};
use grpcio::ChannelBuilder;
use grpcio::Environment;
use grpcio::Server;
use maxgraph_runtime::rpc::*;
use maxgraph_common::proto::data::*;

use rpc_pegasus::ctrl_service::MaxGraphCtrlServiceImpl as PegasusCtrlService;
use rpc_pegasus::async_maxgraph_service::AsyncMaxGraphServiceImpl as PegasusAsyncService;
use rpc_pegasus::maxgraph_service::MaxGraphServiceImpl as PegasusService;
use std::sync::atomic::AtomicBool;
use maxgraph_runtime::utils::get_lambda_service_client;
use maxgraph_runtime::store::remote_store_service::RemoteStoreServiceManager;
use maxgraph_store::api::graph_partition::{GraphPartitionManager};
use maxgraph_server::StoreContext;

fn main() {
    if let Some(_) = env::args().find(|arg| arg == "--show-build-info") {
        util::get_build_info();
        return;
    }
    init_log4rs();
    let mut store_config = {
        let args: Vec<String> = std::env::args().collect();
        if args.len() <= 5 && args[1] == "--config" {
            StoreConfig::init_from_file(&args[2], &args[4])
        } else {
            StoreConfig::init()
        }
    };
    let (alive_id, partitions) = get_init_info(&store_config);
    info!("partitions: {:?}", partitions);
    store_config.update_alive_id(alive_id);
    info!("{:?}", store_config);

    let worker_num = store_config.timely_worker_per_process;
    let store_config = Arc::new(store_config);
    if store_config.graph_type.to_lowercase().eq(VINEYARD_GRAPH) {
        info!("Start executor with vineyard graph object id {:?}", store_config.vineyard_graph_id);
        use maxgraph_runtime::store::ffi::FFIGraphStore;
        let ffi_store = FFIGraphStore::new(store_config.vineyard_graph_id, worker_num as i32);
        let partition_manager = ffi_store.get_partition_manager();
        run_main(store_config, Arc::new(ffi_store), Arc::new(partition_manager));
    } else {
        unimplemented!("only start vineyard graph from executor")
    }
}

fn run_main<V, VI, E, EI>(store_config: Arc<StoreConfig>,
            graph: Arc<GlobalGraphQuery<V=V, E=E, VI=VI, EI=EI>>,
            partition_manager: Arc<GraphPartitionManager>)
    where V: Vertex + 'static,
          VI: Iterator<Item=V> + Send + 'static,
          E: Edge + 'static,
          EI: Iterator<Item=E> + Send + 'static {
    let process_partition_list = partition_manager.get_process_partition_list();
    let runtime_info = Arc::new(Mutex::new(RuntimeInfo::new(store_config.timely_worker_per_process,
                                                            process_partition_list)));
    let runtime_info_clone = runtime_info.clone();
    let (hb_resp_sender, hb_resp_receiver) = channel();
    let query_manager = QueryManager::new();
    let server_manager: Box<dyn ServerManager<Data=Vec<u8>>>;
    let ctrl_service;
    let async_maxgraph_service;
    let maxgraph_service;
    let initial_timeout = 60;
    info!("Start pegasus ........");
    let remote_store_service_manager = Arc::new(RwLock::new(Some(RemoteStoreServiceManager::empty())));
    info!("is lambda enabled: {}", store_config.lambda_enabled);
    let lambda_service_client = if store_config.lambda_enabled { get_lambda_service_client() } else { None };
    let signal = Arc::new(AtomicBool::new(false));

    let pegasus_server_manager = PegasusServerManager::new(hb_resp_receiver, runtime_info, remote_store_service_manager.clone(), signal.clone());
    let pegasus_runtime = pegasus_server_manager.get_server();
    let task_partition_manager = pegasus_server_manager.get_task_partition_manager();

    server_manager = Box::new(pegasus_server_manager);
    let _manager_guards = ServerManager::start_server(server_manager, store_config.clone(), Box::new(recover_prepare)).unwrap();

    // waiting for initial task_partition_manager
    let mut timeout_count = 0;
    loop {
        match task_partition_manager.read() {
            Ok(_) => break,
            Err(_) => thread::sleep(Duration::from_secs(1)),
        }
        timeout_count += 1;
        if timeout_count == initial_timeout {
            panic!("waiting for initial task_partition_manager timeout.")
        }
    };

    ctrl_service = PegasusCtrlService::new_service(query_manager.clone(), pegasus_runtime.clone());
    async_maxgraph_service = PegasusAsyncService::new_service(store_config.clone(),
                                                              pegasus_runtime.clone(),
                                                              query_manager.clone(),
                                                              remote_store_service_manager,
                                                              lambda_service_client,
                                                              signal,
                                                              graph.clone(),
                                                              partition_manager.clone(),
                                                              task_partition_manager);
    maxgraph_service = PegasusService::new_service(store_config.clone(), query_manager.clone());
    let ctrl_and_async_server = start_ctrl_and_async_service(0, ctrl_service, async_maxgraph_service).expect("Start ctrl and async service error.");
    info!("async maxgraph service and control service bind to: {:?}", ctrl_and_async_server.bind_addrs());
    let ctrl_and_async_service_port = ctrl_and_async_server.bind_addrs()[0].1;
    let store_context = StoreContext::new(graph, partition_manager);
    start_rpc_service(runtime_info_clone, store_config, maxgraph_service, ctrl_and_async_service_port, hb_resp_sender, store_context);
    thread::sleep(Duration::from_secs(u64::max_value()));
    ::std::mem::drop(ctrl_and_async_server)
}

fn recover_prepare(prepared: &[u8]) -> Result<Vec<u8>, String> {
    ::protobuf::parse_from_bytes::<QueryFlow>(prepared)
        .map_err(|err| err.to_string())
        .and_then(move |desc| {
            info!("parse {} bytes to {:?} ", prepared.len(), desc);
            Ok(desc.write_to_bytes().expect("query flow to bytes"))
        })
}

fn start_rpc_service<VV, VVI, EE, EEI>(runtime_info: Arc<Mutex<RuntimeInfo>>,
                                         store_config: Arc<StoreConfig>,
                                         maxgraph_service: grpcio::Service,
                                         ctrl_and_async_service_port: u16,
                                         hb_resp_sender: Sender<Arc<ServerHBResp>>,
                                         store_context: StoreContext<VV, VVI, EE, EEI>)
    where VV: 'static + Vertex,
          VVI: 'static + Iterator<Item=VV>,
          EE: 'static + Edge,
          EEI: 'static + Iterator<Item=EE>
{
    // build hb information
    let mut hb_providers = Vec::new();
    let mut hb_resp_senders = Vec::new();
    let hb_provider = move |ref mut server_hb_req: &mut ServerHBReq| {
        server_hb_req.set_runtimeReq(build_runtime_req(runtime_info.clone()));
        server_hb_req.mut_endpoint().set_runtimCtrlAndAsyncPort(ctrl_and_async_service_port as i32);
    };

    hb_providers.push(Box::new(hb_provider));
    hb_resp_senders.push(hb_resp_sender);

    let store_config_clone = store_config.clone();
    let rpc_service_generator = move |_store| {
        let service = maxgraph_service;
        Some(service)
    };

    maxgraph_server::init_with_rpc_service(store_config_clone, rpc_service_generator, hb_providers, hb_resp_senders, store_context);
}

fn start_ctrl_and_async_service(port: u16, ctrl_service: grpcio::Service, async_maxgraph_service: grpcio::Service) -> Result<Server, String> {
    let env = Arc::new(Environment::new(1));
    let server_builder = ServerBuilder::new(env.clone())
        .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
        .register_service(async_maxgraph_service)
        .register_service(ctrl_service)
        .bind("0.0.0.0", port);
    let mut server = server_builder.build().map_err(|err| format!("Error when build rpc server: {:?}", err))?;
    server.start();
    Ok(server)
}

fn build_runtime_req(runtime_info: Arc<Mutex<RuntimeInfo>>) -> RuntimeHBReq {
    let hb_req = runtime_info.lock().expect("Lock runtime hb req failed");

    let mut runtime_req = RuntimeHBReq::new();
    runtime_req.set_serverStatus(hb_req.get_server_status());
    runtime_req.set_runtimePort(hb_req.get_server_port() as i32);
    runtime_req.set_worker_num_per_process(hb_req.get_worker_num_per_process());
    runtime_req.set_process_partition_list(hb_req.get_process_partition_list().to_vec());
    info!("Build runtime request {:?} in heartbeat", &runtime_req);

    runtime_req
}

/// return: (aliveId, partiiton assignments)
fn get_init_info(config: &StoreConfig) -> (u64, Vec<PartitionId>) {
    use maxgraph_server::client::ZKClient;
    use maxgraph_common::proto::data_grpc::*;
    use maxgraph_common::util::ip;

    let zk_url = format!("{}/{}", config.zk_url, config.graph_name);
    let zk = ZKClient::new(&zk_url, config.zk_timeout_ms, config.get_zk_auth());
    let addr = zk.get_coordinator_addr();

    let channel = ChannelBuilder::new(Arc::new(EnvBuilder::new().build()))
        .connect(addr.to_string().as_str());

    let client = ServerDataApiClient::new(channel);

    let mut request = GetExecutorAliveIdRequest::new();
    request.set_serverId(config.worker_id);
    request.set_ip(ip::get_local_ip());
    let response = client.get_executor_alive_id(&request).unwrap();
    let alive_id = response.get_aliveId();
    let mut request = GetPartitionAssignmentRequest::new();
    request.set_serverId(config.worker_id);
    let response = client.get_partition_assignment(&request).unwrap();
    let partitions = response.get_partitionId().to_vec();
    (alive_id, partitions)
}
