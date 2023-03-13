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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use gaia_runtime::error::{StartServerError, StartServerResult};
use global_query::{FFIGraphStore, GraphPartitionManager};
use log::info;
use pegasus::api::{Fold, Sink};
use pegasus::{wait_servers_ready, Configuration, JobConf, ServerConf};
use pegasus_network::config::NetworkConfig;
use pegasus_network::config::ServerAddr;
use pegasus_server::rpc::{start_rpc_server, RPCServerConfig, ServiceStartListener};
use runtime_integration::{InitializeJobAssembly, QueryVineyard};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let log_config_file = &args[1]; // log4rs.yml
    let server_config_file = &args[2]; // executor.vineyard.properties

    // Init log4rs
    log4rs::init_file(log_config_file, Default::default())?;

    // Parse properties, init ServerConfig and RPCServerConfig
    let parsed = dotproperties::parse_from_file(server_config_file)
        .map_err(|_| StartServerError::parse_error("parse_from_file failed"))?;
    let config_map: HashMap<_, _> = parsed.into_iter().collect();
    let rpc_port: u16 = config_map
        .get("rpc.port")
        .ok_or(StartServerError::empty_config_error("rpc.port"))?
        .parse()?;
    let server_id: u64 = config_map
        .get("server.id")
        .ok_or(StartServerError::empty_config_error("server.id"))?
        .parse()?;
    let server_size: usize = config_map
        .get("server.size")
        .ok_or(StartServerError::empty_config_error("server.size"))?
        .parse()?;
    let hosts: Vec<&str> = config_map
        .get("network.servers")
        .ok_or(StartServerError::empty_config_error("network.servers"))?
        .split(",")
        .collect();
    let worker_thread_num: i32 = config_map
        .get("pegasus.worker.num")
        .ok_or(StartServerError::empty_config_error("pegasus.worker.num"))?
        .parse()?;
    let vineyard_graph_id: i64 = config_map
        .get("graph.vineyard.object.id")
        .ok_or(StartServerError::empty_config_error("graph.vineyard.object.id"))?
        .parse()?;

    assert_eq!(server_size, hosts.len());

    let mut server_addrs = Vec::with_capacity(server_size);
    for host in hosts {
        let ip_port: Vec<&str> = host.split(":").collect();
        let server_addr = ServerAddr::new(ip_port[0].parse()?, ip_port[1].parse()?);
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

    pegasus::startup(server_config)?;
    wait_servers_ready(&ServerConf::All);

    let (server_index, process_partition_lists) =
        sync_global_process_partition_lists(process_partition_list)?;
    let computed_process_partition_list = process_partition_lists
        .get(&server_index)
        .unwrap_or(&Vec::new())
        .clone();
    let partition_server_index_map = compute_partition_server_mapping(process_partition_lists)?;
    info!("server_index: {:?}, partition_server_index_map: {:?}", server_index, partition_server_index_map);

    let query_vineyard = QueryVineyard::new(
        Arc::new(ffi_store).clone(),
        Arc::new(partition_manager),
        partition_server_index_map,
        computed_process_partition_list,
    );
    let job_assembly = query_vineyard.initialize_job_assembly();
    start_rpc_server(server_id, rpc_config, job_assembly, GaiaServiceListener).await?;
    Ok(())
}

fn sync_global_process_partition_lists(
    local_server_partition_list: Vec<u32>,
) -> StartServerResult<(u32, HashMap<u32, Vec<u32>>)> {
    // sync global mapping of server_index(process) -> partition_list via pegasus
    let mut conf = JobConf::new("query_current_worker_id");
    conf.reset_servers(ServerConf::All);
    let mut results = pegasus::run(conf, || {
        move |input, output| {
            input
                .input_from(vec![pegasus::get_current_worker().server_index])?
                .sink_into(output)
        }
    })
    .map_err(|e| StartServerError::other_error(&format!("build job failed: {:?}", e)))?;

    let server_index_value: u32;
    if let Some(Ok(v)) = results.next() {
        server_index_value = v;
    } else {
        return Err(StartServerError::other_error("pull result failed for "));
    }

    let mut conf = JobConf::new("sync_global_process_partition_lists");
    conf.reset_servers(ServerConf::All);
    let mut results = pegasus::run(conf, || {
        let server_index = pegasus::get_current_worker().server_index;
        let local_server_partition_list = local_server_partition_list.clone();
        move |input, output| {
            input
                .input_from(vec![(server_index, local_server_partition_list)])?
                .broadcast()
                .sink_into(output)
        }
    })
    .map_err(|e| StartServerError::other_error(&format!("build job failed: {:?}", e)))?;

    let mut partition_lists: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut partition_list_on_processes: HashMap<Vec<u32>, Vec<u32>> = HashMap::new();
    while let Some(Ok((server_index, partitions))) = results.next() {
        partition_lists.insert(server_index, partitions.clone());
        partition_list_on_processes
            .entry(partitions)
            .or_insert_with(Vec::new)
            .push(server_index)
    }
    info!("partition_lists before dedup = {:?}", &partition_lists);
    for (partitions, servers) in partition_list_on_processes.iter() {
        if servers.len() > 1 {
            assert_eq!(partitions.len() % servers.len(), 0);
            let nchunk = partitions.len() / servers.len();
            for (index, server) in servers.iter().enumerate() {
                partition_lists.insert(*server, partitions[index * nchunk..(index + 1) * nchunk].to_vec());
            }
        }
    }
    info!("partition_lists = {:?}", &partition_lists);
    Ok((server_index_value, partition_lists))
}

fn compute_partition_server_mapping(
    process_partition_lists: HashMap<u32, Vec<u32>>,
) -> StartServerResult<HashMap<u32, u32>> {
    let mut partition_server_index_map = HashMap::new();
    for (server_index, partitions) in process_partition_lists.iter() {
        for partition in partitions.iter() {
            partition_server_index_map.insert(*partition, *server_index);
        }
    }
    info!("partition_server_index_map = {:?}", &partition_server_index_map);
    Ok(partition_server_index_map)
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
