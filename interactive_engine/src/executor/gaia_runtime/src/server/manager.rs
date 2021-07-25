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

//! timely_server_manager responsible for controlling the timely_server, including starting, running and down
//! it also collects the status information of timely_server and reports to runtime_manager (coordinator)
extern crate protobuf;

use gaia_pegasus::Configuration;
use gs_gremlin::{InitializeJobCompiler, QueryVineyard};
use maxgraph_common::proto::hb::*;
use maxgraph_common::util::{get_local_ip, log};
use maxgraph_runtime::server::allocate::register_tcp_listener;
use maxgraph_runtime::server::manager::{ManagerGuards, ServerManager, ServerManagerCommon};
use maxgraph_runtime::server::network_manager::{NetworkManager, PegasusNetworkCenter};
use maxgraph_runtime::server::RuntimeAddress;
use maxgraph_runtime::server::RuntimeInfo;
use maxgraph_runtime::store::task_partition_manager::TaskPartitionManager;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::{Edge, GlobalGraphQuery, Vertex};
use maxgraph_store::config::StoreConfig;
use pegasus::network_connection;
use pegasus::Pegasus;
use pegasus_network::config::{NetworkConfig, PeerConfig};
use pegasus_server::rpc::start_rpc_server;
use pegasus_server::service::Service;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time;
use std::vec::Vec;
use tokio::runtime::Runtime;

fn parse_store_ip_list(address_list: &[RuntimeAddressProto]) -> (Vec<String>, Vec<RuntimeAddress>) {
    let mut ip_list = Vec::with_capacity(address_list.len());
    let mut store_address_list = Vec::with_capacity(address_list.len());
    for address in address_list {
        ip_list.push(format!(
            "{}:{}",
            address.get_ip(),
            address.get_runtime_port()
        ));
        let store_address =
            RuntimeAddress::new(address.get_ip().to_string(), address.get_store_port());
        store_address_list.push(store_address);
    }

    (ip_list, store_address_list)
}

pub struct GaiaServerManager<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    server_manager_common: ServerManagerCommon,
    gaia_pegasus_runtime: Arc<Option<Pegasus>>,
    task_partition_manager: Arc<RwLock<Option<TaskPartitionManager>>>,
    signal: Arc<AtomicBool>,
    store_config: Arc<StoreConfig>,
    graph: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
    partition_manager: Arc<dyn GraphPartitionManager>,
    rpc_runtime: Runtime,
}

impl<V, VI, E, EI> GaiaServerManager<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    pub fn new(
        receiver: Receiver<Arc<ServerHBResp>>,
        runtime_info: Arc<Mutex<RuntimeInfo>>,
        signal: Arc<AtomicBool>,
        store_config: Arc<StoreConfig>,
        graph: Arc<dyn GlobalGraphQuery<V = V, E = E, VI = VI, EI = EI>>,
        partition_manager: Arc<dyn GraphPartitionManager>,
    ) -> GaiaServerManager<V, VI, E, EI> {
        GaiaServerManager {
            server_manager_common: ServerManagerCommon::new(receiver, runtime_info),
            gaia_pegasus_runtime: Arc::new(None),
            task_partition_manager: Arc::new(RwLock::new(None)),
            signal,
            store_config,
            graph,
            partition_manager,
            rpc_runtime: Runtime::new().unwrap(),
        }
    }

    #[inline]
    pub fn get_server(&self) -> Arc<Option<Pegasus>> {
        self.gaia_pegasus_runtime.clone()
    }

    #[inline]
    pub fn get_task_partition_manager(&self) -> Arc<RwLock<Option<TaskPartitionManager>>> {
        self.task_partition_manager.clone()
    }

    fn initial_task_partition_manager(&self, task_partition_manager: TaskPartitionManager) {
        let mut manager = self.task_partition_manager.write().unwrap();
        manager.replace(task_partition_manager);
    }

    pub fn start_rpc_service(&self) -> (String, u16) {
        let rpc_port = self.rpc_runtime.block_on(async {
            let task_partition_manager = {
                let task_partition_manager = self.task_partition_manager.read().unwrap();
                while task_partition_manager.is_none() {
                    info!("task_partition_manager is none, waiting for initialization...");
                    thread::sleep(time::Duration::from_millis(
                        self.store_config.hb_interval_ms,
                    ));
                    continue;
                }
                task_partition_manager.clone().unwrap()
            };
            let partition_task_list = task_partition_manager.get_partition_task_list();
            info!(
                "partition_task_list in starting gaia {:?}",
                partition_task_list
            );
            let _task_partition_list_mapping =
                task_partition_manager.get_task_partition_list_mapping();
            let query_vineyard = QueryVineyard::new(
                self.graph.clone(),
                self.partition_manager.clone(),
                partition_task_list,
                self.store_config.worker_num as usize,
                self.store_config.worker_id as u64,
            );
            let job_compiler = query_vineyard.initialize_job_compiler();
            let service = Service::new(job_compiler);
            // TODO(bingqing): assign rpc_port in store_config
            //  let port = self.store_config.rpc_port;
            let port = 8088;
            let addr = format!("{}:{}", "0.0.0.0", port);
            let local_addr = start_rpc_server(addr.parse().unwrap(), service, true, false)
                .await
                .unwrap();
            local_addr.port()
        });
        let ip = get_local_ip();
        (ip, rpc_port)
    }
}

impl<V, VI, E, EI> ServerManager for GaiaServerManager<V, VI, E, EI>
where
    V: Vertex + 'static,
    VI: Iterator<Item = V> + Send + 'static,
    E: Edge + 'static,
    EI: Iterator<Item = E> + Send + 'static,
{
    type Data = Vec<u8>;
    fn start_server(
        self: Box<Self>,
        store_config: Arc<StoreConfig>,
        _recover: Box<dyn Send + Sync + 'static + Fn(&[u8]) -> Result<Self::Data, String>>,
    ) -> Result<ManagerGuards<()>, String> {
        info!("start_server for GaiaServerManager...");
        let manager_switch = self.server_manager_common.manager_switch.clone();
        let handle = thread::Builder::new().name("Gaia Server Manager".to_owned()).spawn(move || {
            let listener = register_tcp_listener();
            self.server_manager_common.update_port_and_status(listener.local_addr().expect("Build local address failed.").port(), RuntimeHBReq_RuntimeStatus::STARTING);

            let mut network_manager = NetworkManager::initialize(listener);
            let mut network_center = PegasusNetworkCenter::new();

            while self.server_manager_common.manager_switch.load(Ordering::Relaxed) {
                let hb_resp = self.server_manager_common.get_hb_response();
                if hb_resp.is_none() || network_manager.is_serving() {
                    if hb_resp.is_none() {
                        info!("hb_resp is none");
                    }
                    thread::sleep(time::Duration::from_millis(store_config.hb_interval_ms));
                    continue;
                }

                let timely_server_resp = hb_resp.as_ref().unwrap().get_runtimeResp();

                let group_id = timely_server_resp.get_groupId();
                let worker_id = timely_server_resp.get_workerId();
                let address_list = timely_server_resp.get_addresses();
                let task_partition_list = timely_server_resp.get_task_partition_list();

                if !address_list.is_empty() && !task_partition_list.is_empty() {
                    // start gaia_pegasus
                    let configuration = build_gaia_config(worker_id as usize, address_list);
                    if let Err(err) = gaia_pegasus::startup(configuration) {
                        info!("start pegasus failed {:?}", err);
                    } else {
                        info!("start pegasus successfully");
                    }

                    let (ip_list, _store_ip_list) = parse_store_ip_list(address_list);
                    info!("Receive task partition info {:?} from coordinator", task_partition_list);
                    let task_partition_manager = TaskPartitionManager::parse_proto(task_partition_list);

                    network_manager.update_number(worker_id as usize, ip_list.len());
                    network_center.initialize(ip_list);

                    info!("We have already initial_task_partition_manager {:?}", task_partition_manager);
                    self.initial_task_partition_manager(task_partition_manager);
                    self.signal.store(true, Ordering::Relaxed);

                    // start gaia_pegasus rpc service
                    let (ip, gaia_rpc_service_port) = self.start_rpc_service();
                    info!("gaia_rpc_service bind to: {:?} {:?}", ip, gaia_rpc_service_port);
                } else {
                    continue;
                }

                let (start_addresses, await_addresses) = network_manager.check_network_status(Box::new(network_center.clone()));
                info!("worker {} in group {} is starting, caused by connection between {:?} and {:?} is not working.",
                      worker_id, group_id, start_addresses, await_addresses);

                match network_manager.reconnect(start_addresses, await_addresses, store_config.hb_interval_ms) {
                    Ok(result) => {
                        info!("worker {} in group {} connect to {:?} success.", worker_id, group_id, result);
                        log::log_runtime(store_config.graph_name.as_str(), store_config.worker_id, group_id, worker_id, log::RuntimeEvent::ServerDown,
                                         self.server_manager_common.version, format!("worker {} in group {} connect to {:?} success.", worker_id, group_id, result).as_str());
                        for (index, address, tcp_stream) in result.into_iter() {
                            let connection = network_connection::Connection::new(index, address, tcp_stream);
                            self.gaia_pegasus_runtime.as_ref().as_ref().unwrap().reset_single_network(connection.clone());
                            network_manager.reset_network(connection);
                        }
                    }
                    Err(e) => {
                        error!("worker {} in group {} reset network failed, caused by {:?}.", worker_id, group_id, e);
                        log::log_runtime(store_config.graph_name.as_str(), store_config.worker_id, group_id, worker_id, log::RuntimeEvent::ServerDown,
                                         self.server_manager_common.version, format!("reset network error, caused by {:?}", e).as_str());
                    }
                }

                if network_manager.is_serving() {
                    self.server_manager_common.change_server_status(RuntimeHBReq_RuntimeStatus::RUNNING);
                    info!("worker {} in group {} is running.", worker_id, group_id);
                    log::log_runtime(store_config.graph_name.as_str(), store_config.worker_id, group_id, worker_id, log::RuntimeEvent::ServerDown,
                                     self.server_manager_common.version, format!("worker is running successfully.").as_str());
                }
            }
        });

        match handle {
            Ok(handle) => Ok(ManagerGuards::new(handle, manager_switch)),
            Err(e) => Err(format!("Build server manager thread fail: {:?}", e)),
        }
    }
}

fn build_gaia_config(worker_id: usize, address_list: &[RuntimeAddressProto]) -> Configuration {
    let peers = parse_store_ip_list_for_gaia(address_list);
    info!("gaia peers list: {:?}", peers);
    let ip = peers.get(worker_id as usize).unwrap().ip.clone();
    let port = peers.get(worker_id as usize).unwrap().port.clone();
    let network_config = NetworkConfig::with_default_config(worker_id as u64, ip, port, peers);
    Configuration {
        network: Some(network_config),
        max_pool_size: None,
    }
}

fn parse_store_ip_list_for_gaia(address_list: &[RuntimeAddressProto]) -> Vec<PeerConfig> {
    let mut peers_list = Vec::with_capacity(address_list.len());
    let mut server_idx = 0;
    for address in address_list {
        let peer_config = PeerConfig {
            server_id: server_idx,
            ip: address.get_ip().to_string(),
            // assign a random port for pegasus
            port: 0,
        };
        peers_list.push(peer_config);
        server_idx += 1;
    }
    peers_list
}
