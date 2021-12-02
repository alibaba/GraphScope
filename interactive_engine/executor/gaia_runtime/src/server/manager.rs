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
use maxgraph_common::proto::hb::*;
use maxgraph_common::util::log;
use maxgraph_runtime::server::allocate::register_tcp_listener;
use maxgraph_runtime::server::manager::{ManagerGuards, ServerManager, ServerManagerCommon};
use maxgraph_runtime::server::network_manager::{NetworkManager, PegasusNetworkCenter};
use maxgraph_runtime::server::RuntimeAddress;
use maxgraph_runtime::server::RuntimeInfo;
use maxgraph_runtime::store::task_partition_manager::TaskPartitionManager;
use maxgraph_store::config::StoreConfig;
use pegasus::{network_connection, ConfigArgs};
use pegasus::Pegasus;
use pegasus_network::config::{NetworkConfig, ServerAddr};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time;
use std::vec::Vec;
use gremlin_core::register_gremlin_types;

pub struct GaiaServerManager {
    server_manager_common: ServerManagerCommon,
    // we preserve pegasus_runtime for heartbeat
    pegasus_runtime: Arc<Option<Pegasus>>,
    // mapping of partition id -> worker id
    partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
    // mapping of worker id -> partition list
    worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>,
    signal: Arc<AtomicBool>,
}

impl GaiaServerManager {
    pub fn new(
        receiver: Receiver<Arc<ServerHBResp>>,
        runtime_info: Arc<Mutex<RuntimeInfo>>,
        signal: Arc<AtomicBool>,
    ) -> GaiaServerManager {
        GaiaServerManager {
            server_manager_common: ServerManagerCommon::new(receiver, runtime_info),
            pegasus_runtime: Arc::new(None),
            partition_worker_mapping: Arc::new(RwLock::new(None)),
            worker_partition_list_mapping: Arc::new(RwLock::new(None)),
            signal,
        }
    }

    #[inline]
    pub fn get_server(&self) -> Arc<Option<Pegasus>> {
        self.pegasus_runtime.clone()
    }

    #[inline]
    pub fn get_partition_worker_mapping(&self) -> Arc<RwLock<Option<HashMap<u32, u32>>>> {
        self.partition_worker_mapping.clone()
    }

    fn initial_partition_worker_mapping(&self, partition_task_list: HashMap<u32, u32>) {
        let mut partition_worker_mapping = self.partition_worker_mapping.write().unwrap();
        partition_worker_mapping.replace(partition_task_list);
    }

    #[inline]
    pub fn get_worker_partition_list_mapping(&self) -> Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>> {
        self.worker_partition_list_mapping.clone()
    }

    fn initial_worker_partition_list_mapping(&self, task_partition_lists: HashMap<u32, Vec<u32>>) {
        let mut worker_partition_list_mapping = self.worker_partition_list_mapping.write().unwrap();
        worker_partition_list_mapping.replace(task_partition_lists);
    }

    /// Initialize pegasus runtime
    ///
    /// Pegasus runtime is shared by query rpc thread and server manager thread, but it will only be initialized once by
    /// server manager thread and query rpc thread only read the value.
    /// Consequently here use `unsafe` but not `Mutex` or `RwLock` to initialize pegasus runtime.
    fn initial_pegasus_runtime(&self, process_id: usize, store_config: Arc<StoreConfig>) {
        let pegasus_runtime = ConfigArgs::distribute(process_id, store_config.pegasus_thread_pool_size as usize, store_config.worker_num as usize, "".to_string()).build();
        unsafe {
            let pegasus_pointer = Arc::into_raw(self.pegasus_runtime.clone()) as *mut Option<Pegasus>;
            (*pegasus_pointer).replace(pegasus_runtime);
        }
    }
}

impl ServerManager for GaiaServerManager {
    type Data = Vec<u8>;
    fn start_server(
        self: Box<Self>,
        store_config: Arc<StoreConfig>,
        _recover: Box<dyn Send + Sync + 'static + Fn(&[u8]) -> Result<Self::Data, String>>,
    ) -> Result<ManagerGuards<()>, String> {
        let manager_switch = self.server_manager_common.manager_switch.clone();
        let handle = thread::Builder::new().name("Gaia Server Manager".to_owned()).spawn(move || {
            let listener = register_tcp_listener();
            self.server_manager_common.update_port_and_status(listener.local_addr().expect("Build local address failed.").port(), RuntimeHBReq_RuntimeStatus::STARTING);

            let mut network_manager = NetworkManager::initialize(listener);
            let mut network_center = PegasusNetworkCenter::new();

            while self.server_manager_common.manager_switch.load(Ordering::Relaxed) {
                let hb_resp = self.server_manager_common.get_hb_response();
                if hb_resp.is_none() || network_manager.is_serving() {
                    thread::sleep(time::Duration::from_millis(store_config.hb_interval_ms));
                    continue;
                }

                let timely_server_resp = hb_resp.as_ref().unwrap().get_runtimeResp();

                let group_id = timely_server_resp.get_groupId();
                let worker_id = timely_server_resp.get_workerId();
                let address_list = timely_server_resp.get_addresses();
                let task_partition_list = timely_server_resp.get_task_partition_list();

                if self.pegasus_runtime.is_none() {
                    info!("Begin start pegasus with process id {} in group {}.", worker_id, group_id);
                    self.initial_pegasus_runtime(worker_id as usize, store_config.clone());
                }
                if !address_list.is_empty() && !task_partition_list.is_empty() {
                    // start gaia_pegasus
                    if let Err(err) = register_gremlin_types() {
                        error!("register_gremlin_types failed {:?}", err);
                    }
                    let configuration = build_gaia_config(worker_id as usize, address_list, store_config.clone());
                    info!("gaia configuration {:?}", configuration);
                    if let Err(err) = gaia_pegasus::startup(configuration) {
                        error!("start pegasus failed {:?}", err);
                    } else {
                        info!("start pegasus successfully");
                    }

                    let (ip_list, _store_ip_list) = parse_store_ip_list(address_list);
                    info!("Receive task partition info {:?} from coordinator", task_partition_list);
                    let task_partition_manager = TaskPartitionManager::parse_proto(task_partition_list);
                    self.initial_partition_worker_mapping(task_partition_manager.get_partition_task_list());
                    self.initial_worker_partition_list_mapping(task_partition_manager.get_task_partition_list_mapping());

                    network_manager.update_number(worker_id as usize, ip_list.len());
                    network_center.initialize(ip_list);

                    self.signal.store(true, Ordering::Relaxed);
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
                            self.pegasus_runtime.as_ref().as_ref().unwrap().reset_single_network(connection.clone());
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

fn build_gaia_config(worker_id: usize, address_list: &[RuntimeAddressProto], store_config: Arc<StoreConfig>) -> Configuration {
    let peers = parse_store_ip_list_for_gaia(address_list, store_config);
    info!("gaia peers list: {:?}", peers);
    // TODO: more configuration from store_config for pegasus
    let network_config = NetworkConfig::with(worker_id as u64, peers);
    Configuration {
        network: Some(network_config),
        max_pool_size: None,
    }
}

fn parse_store_ip_list_for_gaia(address_list: &[RuntimeAddressProto], store_config: Arc<StoreConfig>) -> Vec<ServerAddr> {
    let mut peers_list = Vec::with_capacity(address_list.len());
    for address in address_list {
        let peer_config = ServerAddr::new(address.get_ip().to_string(), store_config.gaia_engine_port as u16);
        peers_list.push(peer_config);
    }
    peers_list
}

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
