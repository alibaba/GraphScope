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


#[macro_use]
extern crate log;
use pegasus;
use log4rs;
use pegasus_server;
use dotproperties;

use runtime_integration::{InitializeJobAssembly, QueryVineyard};
use pegasus_server::rpc::RPCServerConfig;
use pegasus::Configuration;
use pegasus_network::config::NetworkConfig;
use pegasus_network::config::ServerAddr;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use v6d_ffi::read_ffi::FFIGraphStore;
use maxgraph_store::api::graph_partition::GraphPartitionManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let log_config_file = &args[1];  // log4rs.yml
    let server_config_file = &args[2];  // executor.vineyard.properties

    // Init log4rs
    log4rs::init_file(log_config_file, Default::default())
        .unwrap_or_else(|_| panic!("init log4rs from {} failed", log_config_file));

    // Parse properties, init ServerConfig and RPCServerConfig
    let parsed = dotproperties::parse_from_file(server_config_file).unwrap();
    let mapped : HashMap<_,_> = parsed.into_iter().collect();
    let rpc_port: u16 = mapped.get("rpc.port").unwrap().parse().unwrap();
    let server_id: u64 = mapped.get("server.id").unwrap().parse().unwrap();
    let server_size: usize = mapped.get("server.size").unwrap().parse().unwrap();
    let hosts: Vec<&str> = mapped.get("pegasus.hosts").unwrap().split(",").collect();
    let worker_thread_num: i32 = mapped.get("pegasus.worker.num").unwrap().parse().unwrap();
    let vineyard_graph_id : i64 = mapped.get("graph.vineyard.object.id").unwrap().parse().unwrap();
    assert_eq!(server_size, hosts.len());

    let mut server_addrs = Vec::with_capacity(server_size);
    for host in hosts {
        let ip_port: Vec<&str> = host.split(":").collect();
        let server_addr = ServerAddr::new(ip_port[0].parse().unwrap(), ip_port[1].parse().unwrap());
        server_addrs.push(server_addr);
    }
    let network_config = NetworkConfig::with(server_id, server_addrs);
    let server_config = Configuration::with(network_config);
    let rpc_config = RPCServerConfig::new(Some(String::from("127.0.0.1")), Some(rpc_port));

    info!("server config {:?}", server_config);
    info!("rpc config {:?}", rpc_config);
    info!("Start executor with vineyard graph object id {:?}", vineyard_graph_id);

    let ffi_store = FFIGraphStore::new(vineyard_graph_id, worker_thread_num);

    // Init partitions information
    let partition_manager = ffi_store.get_partition_manager();
    let process_partition_list = partition_manager.get_process_partition_list();
    info!("process_partition_list: {:?}", process_partition_list);
    let mut partition_worker = HashMap::new();
    for partition_id in process_partition_list {
        partition_worker.insert(partition_id,server_id as u32);
    }
    let process_partition_list = partition_manager.get_process_partition_list();
    // 1 worker, 4 thread(partition)
    let worker_partition_list = HashMap::from([(server_id as u32, process_partition_list)]);
    let partition_worker_mapping : Arc<RwLock<Option<HashMap<u32, u32>>>> = Arc::new(RwLock::new(Some(partition_worker)));
    let worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>> = Arc::new(RwLock::new(Some(worker_partition_list)));


    let query_vineyard = QueryVineyard::new(
        Arc::new(ffi_store).clone(),
        Arc::new(partition_manager),
        partition_worker_mapping,
        worker_partition_list_mapping,
        server_size
    );
    let job_assembly = query_vineyard.initialize_job_assembly();
    // let runtime = Runtime::new().unwrap();
    pegasus_server::cluster::standalone::start(rpc_config, server_config, job_assembly).await?;
    Ok(())
}
