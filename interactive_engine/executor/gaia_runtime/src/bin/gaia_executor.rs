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
extern crate gaia_pegasus;
extern crate gs_gremlin;
extern crate log4rs;
extern crate maxgraph_common;
extern crate maxgraph_runtime;
extern crate maxgraph_server;
extern crate maxgraph_store;
extern crate pegasus;
extern crate pegasus_server;
extern crate protobuf;
extern crate structopt;

use gaia_runtime::server::init_with_rpc_service;
use gaia_runtime::server::manager::GaiaServerManager;
use grpcio::ChannelBuilder;
use grpcio::EnvBuilder;
use gs_gremlin::{InitializeJobCompiler, QueryVineyard};
use maxgraph_common::proto::data::*;
use maxgraph_common::proto::hb::*;
use maxgraph_common::proto::query_flow::*;
use maxgraph_common::util;
use maxgraph_common::util::get_local_ip;
use maxgraph_common::util::log4rs::init_log4rs;
use maxgraph_runtime::server::manager::*;
use maxgraph_runtime::server::RuntimeInfo;
use maxgraph_server::StoreContext;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::prelude::*;
use maxgraph_store::config::{StoreConfig, VINEYARD_GRAPH};
use pegasus_server::rpc::{start_rpc_server, RpcService};
use pegasus_server::service::Service;
use protobuf::Message;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    if let Some(_) = env::args().find(|arg| arg == "--show-build-info") {
        util::get_build_info();
        return;
    }
    init_log4rs();
    let mut store_config = {
        let args: Vec<String> = std::env::args().collect();
        if args.len() <= 6 && args[1] == "--config" {
            let mut store_config = StoreConfig::init_from_file(&args[2], &args[4]);
            if args.len() == 6 {
                store_config.graph_name = (&args[5]).to_string();
            }
            store_config
        } else {
            StoreConfig::init()
        }
    };
    let (alive_id, partitions) = get_init_info(&store_config);
    info!("alive_id: {:?}, partitions: {:?}", alive_id, partitions);
    store_config.update_alive_id(alive_id);
    info!("{:?}", store_config);

    let worker_num = store_config.timely_worker_per_process;
    let store_config = Arc::new(store_config);
    if store_config.graph_type.to_lowercase().eq(VINEYARD_GRAPH) {
        info!(
            "Start executor with vineyard graph object id {:?}",
            store_config.vineyard_graph_id
        );
        use maxgraph_runtime::store::ffi::FFIGraphStore;
        let ffi_store = FFIGraphStore::new(store_config.vineyard_graph_id, worker_num as i32);
        let partition_manager = ffi_store.get_partition_manager();
        run_main(store_config, Arc::new(ffi_store), Arc::new(partition_manager));
    } else {
        unimplemented!("only start vineyard graph from executor")
    }
}

fn run_main<V, VI, E, EI>(
    store_config: Arc<StoreConfig>,
    graph: Arc<GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<GraphPartitionManager>,
) where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    let process_partition_list = partition_manager.get_process_partition_list();
    info!("process_partition_list: {:?}", process_partition_list);
    let runtime_info = Arc::new(Mutex::new(RuntimeInfo::new(
        store_config.timely_worker_per_process,
        process_partition_list,
    )));
    let runtime_info_clone = runtime_info.clone();
    let (hb_resp_sender, hb_resp_receiver) = channel();
    let signal = Arc::new(AtomicBool::new(false));
    let gaia_server_manager =
        GaiaServerManager::new(hb_resp_receiver, runtime_info, signal.clone());

    let partition_worker_mapping = gaia_server_manager.get_partition_worker_mapping();
    let worker_partition_list_mapping = gaia_server_manager.get_worker_partition_list_mapping();
    let server_manager = Box::new(gaia_server_manager);
    let _manager_guards = ServerManager::start_server(
        server_manager,
        store_config.clone(),
        Box::new(recover_prepare),
    )
    .unwrap();

    let gaia_service = GaiaService::new(
        store_config.clone(),
        graph.clone(),
        partition_manager.clone(),
        partition_worker_mapping,
        worker_partition_list_mapping,
    );
    let (_, gaia_rpc_service_port) = gaia_service.start_rpc_service();
    let store_context = StoreContext::new(graph, partition_manager);
    start_hb_rpc_service(
        runtime_info_clone,
        store_config,
        gaia_rpc_service_port,
        hb_resp_sender,
        store_context,
    );
    thread::sleep(Duration::from_secs(u64::max_value()));
}

fn recover_prepare(prepared: &[u8]) -> Result<Vec<u8>, String> {
    ::protobuf::parse_from_bytes::<QueryFlow>(prepared)
        .map_err(|err| err.to_string())
        .and_then(move |desc| {
            info!("parse {} bytes to {:?} ", prepared.len(), desc);
            Ok(desc.write_to_bytes().expect("query flow to bytes"))
        })
}

fn start_hb_rpc_service<VV, VVI, EE, EEI>(
    runtime_info: Arc<Mutex<RuntimeInfo>>,
    store_config: Arc<StoreConfig>,
    gaia_service_port: u16,
    hb_resp_sender: Sender<Arc<ServerHBResp>>,
    store_context: StoreContext<VV, VVI, EE, EEI>,
) where
    VV: 'static + Vertex,
    VVI: 'static + Iterator<Item = VV> + Send,
    EE: 'static + Edge,
    EEI: 'static + Iterator<Item = EE> + Send,
{
    // build hb information
    let mut hb_providers = Vec::new();
    let mut hb_resp_senders = Vec::new();
    let hb_provider = move |ref mut server_hb_req: &mut ServerHBReq| {
        server_hb_req.set_runtimeReq(build_runtime_req(runtime_info.clone()));
        server_hb_req
            .mut_endpoint()
            .set_runtimCtrlAndAsyncPort(gaia_service_port as i32);
    };

    hb_providers.push(Box::new(hb_provider));
    hb_resp_senders.push(hb_resp_sender);

    let store_config_clone = store_config.clone();
    init_with_rpc_service(
        store_config_clone,
        hb_providers,
        hb_resp_senders,
        store_context,
    );
}

fn build_runtime_req(runtime_info: Arc<Mutex<RuntimeInfo>>) -> RuntimeHBReq {
    let hb_req = runtime_info.lock().expect("Lock runtime hb req failed");

    let mut runtime_req = RuntimeHBReq::new();
    runtime_req.set_serverStatus(hb_req.get_server_status());
    runtime_req.set_runtimePort(hb_req.get_server_port() as i32);
    runtime_req.set_worker_num_per_process(hb_req.get_worker_num_per_process());
    runtime_req.set_process_partition_list(hb_req.get_process_partition_list().to_vec());
    debug!("Build runtime request {:?} in heartbeat", &runtime_req);

    runtime_req
}

/// return: (aliveId, partiiton assignments)
fn get_init_info(config: &StoreConfig) -> (u64, Vec<PartitionId>) {
    use maxgraph_common::proto::data_grpc::*;
    use maxgraph_common::util::ip;
    use maxgraph_server::client::ZKClient;

    let zk_url = format!("{}/{}", config.zk_url, config.graph_name);
    let zk = ZKClient::new(&zk_url, config.zk_timeout_ms, config.get_zk_auth());
    let addr = zk.get_coordinator_addr();

    let channel =
        ChannelBuilder::new(Arc::new(EnvBuilder::new().build())).connect(addr.to_string().as_str());

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

pub struct GaiaService<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    store_config: Arc<StoreConfig>,
    graph: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>,
    // mapping of partition id -> worker id
    partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
    // mapping of worker id -> partition list
    worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>,
    rpc_runtime: Runtime,
}

impl<V, VI, E, EI> GaiaService<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    pub fn new(
        store_config: Arc<StoreConfig>,
        graph: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
        partition_manager: Arc<dyn GraphPartitionManager>,
        partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
        worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>,
    ) -> GaiaService<V, VI, E, EI> {
        GaiaService {
            store_config,
            graph,
            partition_manager,
            partition_worker_mapping,
            worker_partition_list_mapping,
            rpc_runtime: Runtime::new().unwrap(),
        }
    }

    pub fn start_rpc_service(&self) -> (String, u16) {
        let rpc_port = self.rpc_runtime.block_on(async {
            let query_vineyard = QueryVineyard::new(
                self.graph.clone(),
                self.partition_manager.clone(),
                self.partition_worker_mapping.clone(),
                self.worker_partition_list_mapping.clone(),
                self.store_config.worker_num as usize,
                self.store_config.worker_id as u64,
            );
            let job_compiler = query_vineyard.initialize_job_compiler();
            let service = Service::new(job_compiler);
            let addr = format!("{}:{}", "0.0.0.0", self.store_config.rpc_port);
            // TODO: add report in store_config
            let rpc_service = RpcService::new(service, true);
            let local_addr =  start_rpc_server(addr.parse().unwrap(), rpc_service, false).await.unwrap();
            local_addr.port()
        });
        let ip = get_local_ip();
        info!("start rpc server on {} {}", ip, rpc_port);
        (ip, rpc_port)
    }
}
