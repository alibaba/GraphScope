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

use log::info;
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use pegasus::api::{Fold, Sink};
use pegasus::{Configuration, JobConf, ServerConf};
use pegasus_network::config::NetworkConfig;
use pegasus_network::config::ServerAddr;
use pegasus_server::rpc::{start_rpc_server, RPCServerConfig, ServiceStartListener};
use runtime_integration::{InitializeJobAssembly, QueryVineyard};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use v6d_ffi::read_ffi::FFIGraphStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let log_config_file = &args[1]; // log4rs.yml
    let server_config_file = &args[2]; // executor.vineyard.properties

    // Init log4rs
    log4rs::init_file(log_config_file, Default::default())
        .unwrap_or_else(|_| panic!("init log4rs from {} failed", log_config_file));

    // Parse properties, init ServerConfig and RPCServerConfig
    let parsed = dotproperties::parse_from_file(server_config_file).unwrap();
    let mapped: HashMap<_, _> = parsed.into_iter().collect();
    let rpc_port: u16 = mapped.get("rpc.port").unwrap().parse().unwrap();
    let server_id: u64 = mapped
        .get("server.id")
        .unwrap()
        .parse()
        .unwrap();
    let server_size: usize = mapped
        .get("server.size")
        .unwrap()
        .parse()
        .unwrap();
    let hosts: Vec<&str> = mapped
        .get("pegasus.hosts")
        .unwrap()
        .split(",")
        .collect();
    let worker_thread_num: i32 = mapped
        .get("pegasus.worker.num")
        .unwrap()
        .parse()
        .unwrap();
    let vineyard_graph_id: i64 = mapped
        .get("graph.vineyard.object.id")
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(server_size, hosts.len());

    let mut server_addrs = Vec::with_capacity(server_size);
    for host in hosts {
        let ip_port: Vec<&str> = host.split(":").collect();
        let server_addr = ServerAddr::new(ip_port[0].parse().unwrap(), ip_port[1].parse().unwrap());
        server_addrs.push(server_addr);
    }
    let network_config = NetworkConfig::with(server_id, server_addrs);
    let server_config = Configuration::with(network_config);
    let rpc_config = RPCServerConfig::new(Some(String::from("0.0.0.0")), Some(rpc_port));

    info!("server config {:?}", server_config);
    info!("rpc config {:?}", rpc_config);
    info!("Start executor with vineyard graph object id {:?}", vineyard_graph_id);

    // TODO: remove the pre-partition logic on vineyard; Instead, vineyard can provide the true partition_nums;
    // we assume the truly partition numbers in vineyard is worker_thread_num for now.
    let ffi_store = FFIGraphStore::new(vineyard_graph_id, worker_thread_num);

    // Init partitions information
    let partition_manager = ffi_store.get_partition_manager();
    let process_partition_list = partition_manager.get_process_partition_list();
    info!("process_partition_list: {:?}", process_partition_list);

    pegasus::startup(server_config).unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));

    let query_vineyard = QueryVineyard::new(
        Arc::new(ffi_store).clone(),
        Arc::new(partition_manager),
        partition_server_index_map,
    );
    let job_assembly = query_vineyard.initialize_job_assembly();
    start_rpc_server(server_id, rpc_config, job_assembly, GaiaServiceListener).await?;
    Ok(())
}

fn get_global_partition_server_mapping(local_process_partition_list: Vec<u32>) -> HashMap<u32, u32> {
    // sync global mapping of server_index(process) -> partition_list via pegasus
    let mut conf = JobConf::new("get_partition_server_index_map");
    conf.reset_servers(ServerConf::All);
    let mut results = pegasus::run(conf, || {
        let local_process_partition_list = local_process_partition_list.clone();
        move |input, output| {
            let local_process_partition_list = local_process_partition_list.clone();
            let global_process_partition_list_mapping: HashMap<u32, Vec<u32>> = HashMap::new();
            input
                .input_from(local_process_partition_list)?
                .fold(global_process_partition_list_mapping, || {
                    |mut mapping, partition_id| {
                        mapping
                            .entry(pegasus::get_current_worker().server_index)
                            .or_insert_with(Vec::new)
                            .push(partition_id);
                        Ok(mapping)
                    }
                })?
                .map(|mut mapping| {
                    let mut vec = vec![];
                    for (server_index, workers) in mapping.drain() {
                        for worker in workers.into_iter() {
                            vec.push((server_index, worker));
                        }
                    }
                    Ok(vec)
                })?
                .into_stream()?
                .broadcast()
                .sink_into(output)
        }
    })
    .expect("submitted job failure");

    let process_partition_lists = results.next().unwrap().unwrap();
    let mut partition_server_index_map = HashMap::new();
    for (server_index, partition_id) in process_partition_lists {
        partition_server_index_map.insert(partition_id, server_index);
    }
    info!("partition_server_index_map {:?}", partition_server_index_map);

    partition_server_index_map
}

struct GaiaServiceListener;

impl ServiceStartListener for GaiaServiceListener {
    fn on_rpc_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("RPC server of server[{}] start on {}", server_id, addr);
        Ok(())
    }

    fn on_server_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("compute server[{}] start on {}", server_id, addr);
        Ok(())
    }
}
