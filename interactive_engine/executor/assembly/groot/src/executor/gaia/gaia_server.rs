//
//! Copyright 2021 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.
//!

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use gaia_pegasus::Configuration as GaiaConfig;
use global_query::GlobalGraph;
use maxgraph_store::api::PartitionId;
use maxgraph_store::db::api::{GraphConfig, GraphResult};
use maxgraph_store::db::graph::store::GraphStore;
use pegasus_network::config::{NetworkConfig, ServerAddr};
use pegasus_network::SimpleServerDetector;
use pegasus_server::rpc::{start_all, RPCServerConfig, ServiceStartListener};
use runtime_integration::{InitializeJobAssembly, QueryGrootGraph};
use tokio::runtime::Runtime;

pub struct GaiaServer {
    config: Arc<GraphConfig>,
    graph: Arc<GlobalGraph>,
    detector: Arc<SimpleServerDetector>,
    rpc_runtime: Runtime,
}

impl GaiaServer {
    pub fn new(config: Arc<GraphConfig>) -> Self {
        let partition_count = config
            .get_storage_option("partition.count")
            .expect("required config partition.count is missing")
            .parse()
            .expect("parse partition.count failed");
        GaiaServer {
            config,
            graph: Arc::new(GlobalGraph::empty(partition_count)),
            detector: Arc::new(SimpleServerDetector::new()),
            rpc_runtime: Runtime::new().unwrap(),
        }
    }

    pub fn add_partition(&mut self, partition_id: PartitionId, graph_partition: Arc<GraphStore>) {
        Arc::get_mut(&mut self.graph)
            .unwrap()
            .add_partition(partition_id, graph_partition);
    }

    pub fn update_partition_routing(&mut self, partition_id: PartitionId, worker_id: u32) {
        Arc::get_mut(&mut self.graph)
            .unwrap()
            .update_partition_routing(partition_id, worker_id);
    }

    pub fn start(&'static self) -> GraphResult<(u16, u16)> {
        let gaia_config = make_gaia_config(self.config.clone());
        let gaia_rpc_config = make_gaia_rpc_config(self.config.clone());
        info!("Server config {:?}\nRPC config {:?}", gaia_config, gaia_rpc_config);
        let (server_port, rpc_port) = self.rpc_runtime.block_on(async {
            let query_maxgraph = QueryGrootGraph::new(self.graph.clone(), self.graph.clone());
            let job_compiler = query_maxgraph.initialize_job_assembly();
            let service_listener = GaiaServiceListener::default();
            let service_listener_clone = service_listener.clone();
            self.rpc_runtime.spawn(async move {
                start_all(
                    gaia_rpc_config,
                    gaia_config,
                    job_compiler,
                    self.detector.clone(),
                    service_listener_clone,
                )
                .await
                .unwrap()
            });
            loop {
                if service_listener.get_server_port().is_some() && service_listener.get_rpc_port().is_some()
                {
                    break;
                } else {
                    thread::sleep(Duration::from_millis(10));
                }
            }
            (service_listener.get_server_port().unwrap(), service_listener.get_rpc_port().unwrap())
        });
        Ok((server_port, rpc_port))
    }

    pub fn update_peer_view(&self, peer_view: Vec<(u64, ServerAddr)>) {
        self.detector
            .update_peer_view(peer_view.into_iter());
    }

    pub fn stop(&self) {
        gaia_pegasus::shutdown_all();
    }
}

fn make_gaia_config(graph_config: Arc<GraphConfig>) -> GaiaConfig {
    let server_id = graph_config
        .get_storage_option("node.idx")
        .expect("required config node.idx is missing")
        .parse()
        .expect("parse node.idx failed");
    let ip = "0.0.0.0".to_string();
    let port = match graph_config.get_storage_option("gaia.engine.port") {
        None => 0,
        Some(server_port_string) => server_port_string
            .parse()
            .expect("parse gaia.engine.port failed"),
    };
    let worker_num = match graph_config.get_storage_option("worker.num") {
        None => 1,
        Some(worker_num_string) => worker_num_string
            .parse()
            .expect("parse worker.num failed"),
    };
    let nonblocking = graph_config
        .get_storage_option("gaia.nonblocking")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.nonblocking failed")
        });
    let read_timeout_ms = graph_config
        .get_storage_option("gaia.read.timeout.ms")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.read.timeout.ms failed")
        });
    let write_timeout_ms = graph_config
        .get_storage_option("gaia.write.timeout.ms")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.write.timeout.ms failed")
        });
    let read_slab_size = graph_config
        .get_storage_option("gaia.read.slab.size")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.read.slab.size failed")
        });
    let no_delay = graph_config
        .get_storage_option("gaia.no.delay")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.no.delay failed")
        });
    let send_buffer = graph_config
        .get_storage_option("gaia.send.buffer")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.send.buffer failed")
        });
    let heartbeat_sec = graph_config
        .get_storage_option("gaia.heartbeat.sec")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.heartbeat.sec failed")
        });
    let max_pool_size = graph_config
        .get_storage_option("gaia.max.pool.size")
        .map(|config_str| {
            config_str
                .parse()
                .expect("parse gaia.max.pool.size failed")
        });
    let mut network_config = NetworkConfig::new(server_id, ServerAddr::new(ip, port), worker_num);
    network_config
        .nonblocking(nonblocking)
        .read_timeout_ms(read_timeout_ms)
        .write_timeout_ms(write_timeout_ms)
        .read_slab_size(read_slab_size)
        .no_delay(no_delay)
        .send_buffer(send_buffer)
        .heartbeat_sec(heartbeat_sec);
    GaiaConfig { network: Some(network_config), max_pool_size }
}

fn make_gaia_rpc_config(graph_config: Arc<GraphConfig>) -> RPCServerConfig {
    let rpc_port = match graph_config.get_storage_option("gaia.rpc.port") {
        None => 0,
        Some(server_port_string) => server_port_string
            .parse()
            .expect("parse node.idx failed"),
    };
    RPCServerConfig::new(Some("0.0.0.0".to_string()), Some(rpc_port))
}

#[derive(Default)]
struct GaiaServiceListener {
    rpc_addr: Arc<Mutex<Option<SocketAddr>>>,
    server_addr: Arc<Mutex<Option<SocketAddr>>>,
}

impl Clone for GaiaServiceListener {
    fn clone(&self) -> Self {
        GaiaServiceListener { rpc_addr: self.rpc_addr.clone(), server_addr: self.server_addr.clone() }
    }
}

impl GaiaServiceListener {
    fn get_rpc_port(&self) -> Option<u16> {
        self.rpc_addr
            .lock()
            .unwrap()
            .map(|addr| addr.port())
    }
    fn get_server_port(&self) -> Option<u16> {
        self.server_addr
            .lock()
            .unwrap()
            .map(|addr| addr.port())
    }
}

impl ServiceStartListener for GaiaServiceListener {
    fn on_rpc_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("RPC server of server[{}] start on {}", server_id, addr);
        let mut rpc_addr = self
            .rpc_addr
            .lock()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        *rpc_addr = Some(addr);
        Ok(())
    }

    fn on_server_start(&mut self, server_id: u64, addr: SocketAddr) -> std::io::Result<()> {
        info!("compute server[{}] start on {}", server_id, addr);
        let mut server_addr = self
            .server_addr
            .lock()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
        *server_addr = Some(addr);
        Ok(())
    }
}
