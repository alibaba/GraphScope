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


use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::vec::Vec;
use std::sync::mpsc::Receiver;
use super::allocate::*;
use super::ServerTimestamp;


use super::RuntimeInfo;
use super::prepare::read_prepared_from_zk;
use maxgraph_common::proto::hb::*;
use maxgraph_store::config::StoreConfig;
use maxgraph_common::util::log;
use std::net::TcpListener;
use std::fmt::Debug;
use std::thread;
use std::time;

use pegasus::ConfigArgs;
use pegasus::Pegasus;
use std::collections::HashMap;
use store::store_client::StoreClientManager;
use server::RuntimeAddress;

/// Join handle for rpc_timely server manager thread
pub struct ManagerGuards<T: Send + 'static> {
    guards: Option<thread::JoinHandle<T>>,
    manager_switch: Arc<AtomicBool>,
}

impl<T: Send + 'static> ManagerGuards<T> {
    pub fn new(handle: thread::JoinHandle<T>, manager_switch: Arc<AtomicBool>) -> ManagerGuards<T> {
        ManagerGuards {
            guards: Some(handle),
            manager_switch,
        }
    }

    pub fn join(&mut self) {
        self.manager_switch.store(false, Ordering::Relaxed);
        let handle = self.guards.take();
        handle.unwrap().join().expect("Join server manager thread fail");
    }
}

impl<T: Send + 'static> Drop for ManagerGuards<T> {
    fn drop(&mut self) {
        info!("Drop rpc_timely server manager thread");
        self.manager_switch.store(false, Ordering::Relaxed);
        let handle = self.guards.take();
        handle.unwrap().join().expect("Drop server manager thread fail");
    }
}


pub trait ServerManager {
    type Data;

    fn start_server(self: Box<Self>, store_config: Arc<StoreConfig>, recover: Box<Send + Sync + 'static + Fn(&[u8]) -> Result<Self::Data, String>>) -> Result<ManagerGuards<()>, String>;
}

pub struct ServerManagerCommon {
    pub version: i64,
    pub runtime_info: Arc<Mutex<RuntimeInfo>>,
    pub server_switch: Arc<AtomicBool>,
    pub manager_switch: Arc<AtomicBool>,
    pub hb_resp_receiver: Receiver<Arc<ServerHBResp>>,
}

impl ServerManagerCommon {
    pub fn new(receiver: Receiver<Arc<ServerHBResp>>, runtime_info: Arc<Mutex<RuntimeInfo>>) -> ServerManagerCommon {
        ServerManagerCommon {
            version: -1,
            runtime_info,
            server_switch: Arc::new(AtomicBool::new(false)),
            manager_switch: Arc::new(AtomicBool::new(true)),
            hb_resp_receiver: receiver,
        }
    }

    /// Change status of rpc_timely server from running to down
    /// when current status is running but receiving a different version.
    ///
    /// As a result, running rpc_timely server will be stopped.
    pub fn running_to_down(&self, store_config: &Arc<StoreConfig>, worker_id: u32, group_id: u32, version: i64) {
        self.server_switch.store(false, Ordering::Relaxed);
        self.update_port_and_status(0, RuntimeHBReq_RuntimeStatus::DOWN);
        info!("change worker {} in group {} state from running to down, caused by current version: {}, receive version {}", worker_id, group_id, self.version, version);

        log::log_runtime(store_config.graph_name.as_str(), store_config.worker_id, group_id, worker_id, log::RuntimeEvent::ServerDown,
                         self.version, format!("Clear port and change state from running to down; current version: {}, receive version: {}", self.version, version).as_str());
    }

    /// Change status of rpc_timely server from down to starting
    /// when receiving a different version and current state is down.
    ///
    /// As a result, rpc_timely server will register a port and change status to `STARTING`.
    /// The ip and port will be collected by coordinator.
    pub fn down_to_starting(&self, store_config: &Arc<StoreConfig>, worker_id: u32, group_id: u32, version: i64) -> TcpListener {
        let listener = register_tcp_listener();
        let port = listener.local_addr().expect("Bild local address failed.").port();

        self.update_port_and_status(port, RuntimeHBReq_RuntimeStatus::STARTING);
        info!("change wroker {} in group {} state from down to starting, current version: {}, receive version: {}", worker_id, group_id, self.version, version);
        log::log_runtime(store_config.graph_name.as_str(), store_config.worker_id, group_id, worker_id, log::RuntimeEvent::ServerStarting,
                         self.version, format!("Register port and changes state from down to starting; current version: {}, receive version: {}", self.version, version).as_str());

        listener
    }

    /// Change status of rpc_timely server from starting to running
    /// when receiving a full ip list and but connecting to each other rpc_timely servers failed
    pub fn starting_to_down(&self, e: String, store_config: &Arc<StoreConfig>, worker_id: u32, group_id: u32) {
        self.update_port_and_status(0, RuntimeHBReq_RuntimeStatus::DOWN);
        info!("worker {} in group {} initializing network failed, caused by: {:?}", worker_id, group_id, e);
        log::log_runtime(store_config.graph_name.as_str(), store_config.worker_id, group_id, worker_id, log::RuntimeEvent::ServerDown,
                         self.version, format!("Timely server initializing network failed, caused by {:?}, so change state from starting to down", e).as_str());
    }

    /// Receiving all hb response
    pub fn get_hb_response(&self) -> Option<Arc<ServerHBResp>> {
        let mut hb_resp: Arc<ServerHBResp> = Arc::new(ServerHBResp::new());

        // consume invalid hb_resp messages produced during connecting network
        while let Ok(resp) = self.hb_resp_receiver.try_recv() {
            hb_resp = resp;
        }
        if !hb_resp.has_runtimeResp() {
            None
        } else {
            Some(hb_resp)
        }
    }

    /// Update self port and status, which will be send to coordinator
    #[inline]
    pub fn update_port_and_status(&self, port: u16, status: RuntimeHBReq_RuntimeStatus) {
        self.runtime_info.lock().unwrap().change_port_and_status(port, status);
    }

    /// Update self status, which will be send to coordinator
    #[inline]
    pub fn change_server_status(&self, status: RuntimeHBReq_RuntimeStatus) {
        self.runtime_info.lock().unwrap().change_server_status(status);
    }
}

fn parse_store_ip_list(address_list: &[RuntimeAddressProto]) -> (Vec<String>, Vec<RuntimeAddress>) {
    let mut ip_list = Vec::with_capacity(address_list.len());
    let mut store_address_list = Vec::with_capacity(address_list.len());
    for address in address_list {
        ip_list.push(format!("{}:{}", address.get_ip(), address.get_runtime_port()));
        let store_address = RuntimeAddress::new(address.get_ip().to_string(), address.get_store_port());
        store_address_list.push(store_address);
    }

    (ip_list, store_address_list)
}


use server::network_manager::{PegasusNetworkCenter, NetworkManager};
use std::cell::RefCell;
use std::sync::mpsc::Sender;
use store::remote_store_service::RemoteStoreServiceManager;
use store::task_partition_manager::TaskPartitionManager;
use maxgraph_store::api::graph_partition::GraphPartitionManager;

pub struct PegasusServerManager {
    server_manager_common: ServerManagerCommon,
    pegasus_runtime: Arc<Option<Pegasus>>,
    remote_store_service_manager: Arc<RwLock<Option<RemoteStoreServiceManager>>>,
    task_partition_manager: Arc<RwLock<Option<TaskPartitionManager>>>,
    signal: Arc<AtomicBool>,
}

impl PegasusServerManager
{
    pub fn new(receiver: Receiver<Arc<ServerHBResp>>, runtime_info: Arc<Mutex<RuntimeInfo>>,
               remote_store_service_manager: Arc<RwLock<Option<RemoteStoreServiceManager>>>,
               signal: Arc<AtomicBool>) -> PegasusServerManager {
        PegasusServerManager {
            server_manager_common: ServerManagerCommon::new(receiver, runtime_info),
            pegasus_runtime: Arc::new(None),
            remote_store_service_manager,
            task_partition_manager: Arc::new(RwLock::new(None)),
            signal,
        }
    }

    #[inline]
    pub fn get_server(&self) -> Arc<Option<Pegasus>> {
        self.pegasus_runtime.clone()
    }

    #[inline]
    pub fn get_task_partition_manager(&self) -> Arc<RwLock<Option<TaskPartitionManager>>> {
        self.task_partition_manager.clone()
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

    fn initial_task_partition_manager(&self, task_partition_manager: TaskPartitionManager) {
        let mut manager = self.task_partition_manager.write().unwrap();
        manager.replace(task_partition_manager);
    }
}

impl ServerManager for PegasusServerManager
{
    type Data = Vec<u8>;
    fn start_server(self: Box<Self>, store_config: Arc<StoreConfig>, recover: Box<Send + Sync + 'static + Fn(&[u8]) -> Result<Self::Data, String>>) -> Result<ManagerGuards<()>, String>
    {
        let manager_switch = self.server_manager_common.manager_switch.clone();
        let handle = thread::Builder::new().name("Pegasus Server Manager".to_owned()).spawn(move || {
            let recover = Arc::new(recover);

            let listener = register_tcp_listener();
            self.server_manager_common.update_port_and_status(listener.local_addr().expect("Bild local address failed.").port(), RuntimeHBReq_RuntimeStatus::STARTING);

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
                    let (ip_list, store_ip_list) = parse_store_ip_list(address_list);
                    info!("Receive task partition info {:?} from coordinator", task_partition_list);
                    let task_partition_manager = TaskPartitionManager::parse_proto(task_partition_list);

                    network_manager.update_number(worker_id as usize, ip_list.len());
                    network_center.initialize(ip_list);

                    let remote_store_service_manager = RemoteStoreServiceManager::new(task_partition_manager.get_partition_process_list(), store_ip_list);
                    self.remote_store_service_manager.write().unwrap().replace(remote_store_service_manager);
                    self.initial_task_partition_manager(task_partition_manager);
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
                            let connection = crate::pegasus::network_connection::Connection::new(index, address, tcp_stream);
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
