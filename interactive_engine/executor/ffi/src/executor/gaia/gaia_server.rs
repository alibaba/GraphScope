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

use maxgraph_store::db::api::{GraphConfig, GraphResult, GraphError};
use std::sync::Arc;
use maxgraph_store::db::graph::store::GraphStore;
use std::net::SocketAddr;
use maxgraph_runtime::store::groot::global_graph::GlobalGraph;
use gaia_pegasus::Configuration as GaiaConfig;
use maxgraph_store::db::api::GraphErrorCode::EngineError;
use tokio::runtime::Runtime;
use pegasus_server::service::Service;
use pegasus_server::rpc::{start_rpc_server, RpcService};
use pegasus_network::SimpleServerDetector;
use pegasus_network::config::{NetworkConfig, ServerAddr};
use gs_gremlin::{InitializeJobCompiler, QueryMaxGraph};
use maxgraph_store::api::PartitionId;
use gremlin_core::register_gremlin_types;

pub struct GaiaServer {
    config: Arc<GraphConfig>,
    graph: Arc<GlobalGraph>,
    detector: Arc<SimpleServerDetector>,
    rpc_runtime: Runtime,
}

impl GaiaServer {
    pub fn new(config: Arc<GraphConfig>) -> Self {
        let partition_count = config.get_storage_option("partition.count")
            .expect("required config partition.count is missing").parse().expect("parse partition.count failed");
        GaiaServer {
            config,
            graph: Arc::new(GlobalGraph::empty(partition_count)),
            detector: Arc::new(SimpleServerDetector::new()),
            rpc_runtime: Runtime::new().unwrap(),
        }
    }

    pub fn add_partition(&mut self, partition_id: PartitionId, graph_partition: Arc<GraphStore>) {
        Arc::get_mut(&mut self.graph).unwrap().add_partition(partition_id, graph_partition);
    }

    pub fn update_partition_routing(&mut self, partition_id: PartitionId, worker_id: u32) {
        Arc::get_mut(&mut self.graph).unwrap().update_partition_routing(partition_id, worker_id);
    }

    pub fn start(&self) -> GraphResult<(u16, u16)> {
        register_gremlin_types().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        let report = match self.config.get_storage_option("gaia.report") {
            None => false,
            Some(report_string) => report_string.parse()
                .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?,
        };
        let worker_num = match self.config.get_storage_option("worker.num") {
            None => 1,
            Some(worker_num_string) => worker_num_string.parse()
                .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?,
        };
        let rpc_port = match self.config.get_storage_option("gaia.rpc.port") {
            None => { 0 },
            Some(server_port_string) => {
                server_port_string.parse().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?
            },
        };
        let addr = format!("{}:{}", "0.0.0.0", rpc_port).parse()
            .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        let gaia_config = make_gaia_config(self.config.clone());
        let server_id = gaia_config.server_id();
        let socket_addr = gaia_pegasus::startup_with(gaia_config, self.detector.clone())
            .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?
            .ok_or(GraphError::new(EngineError, "gaia engine return None addr".to_string()))?;

        let rpc_port = self.rpc_runtime.block_on(async{
            let query_maxgraph = QueryMaxGraph::new(self.graph.clone(), self.graph.clone(), worker_num, server_id);
            let job_compiler = query_maxgraph.initialize_job_compiler();
            let service = Service::new(job_compiler);
            let rpc_service = RpcService::new(service, report);
            let local_addr = start_rpc_server(addr, rpc_service, false).await.unwrap();
            local_addr.port()
        });
        Ok((socket_addr.port(), rpc_port))
    }

    pub fn update_peer_view(&self, peer_view: Vec<(u64, SocketAddr)>) {
        self.detector.update_peer_view(peer_view.into_iter());
    }

    pub fn stop(&self) {
        gaia_pegasus::shutdown_all();
    }
}

fn make_gaia_config(graph_config: Arc<GraphConfig>) -> GaiaConfig {
    let server_id = graph_config.get_storage_option("node.idx").expect("required config node.idx is missing")
        .parse().expect("parse node.idx failed");
    let ip = "0.0.0.0".to_string();
    let port = match graph_config.get_storage_option("gaia.engine.port") {
        None => { 0 },
        Some(server_port_string) => {
            server_port_string.parse().expect("parse gaia.engine.port failed")
        },
    };
    let worker_num = match graph_config.get_storage_option("worker.num") {
        None => 1,
        Some(worker_num_string) => worker_num_string.parse().expect("parse worker.num failed"),
    };
    let nonblocking = graph_config.get_storage_option("gaia.nonblocking")
        .map(|config_str| config_str.parse().expect("parse gaia.nonblocking failed"));
    let read_timeout_ms = graph_config.get_storage_option("gaia.read.timeout.ms")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let write_timeout_ms = graph_config.get_storage_option("gaia.write.timeout.ms")
        .map(|config_str| config_str.parse().expect("parse gaia.write.timeout.ms failed"));
    let read_slab_size = graph_config.get_storage_option("gaia.read.slab.size")
        .map(|config_str| config_str.parse().expect("parse gaia.read.slab.size failed"));
    let no_delay = graph_config.get_storage_option("gaia.no.delay")
        .map(|config_str| config_str.parse().expect("parse gaia.no.delay failed"));
    let send_buffer = graph_config.get_storage_option("gaia.send.buffer")
        .map(|config_str| config_str.parse().expect("parse gaia.send.buffer failed"));
    let heartbeat_sec = graph_config.get_storage_option("gaia.heartbeat.sec")
        .map(|config_str| config_str.parse().expect("parse gaia.heartbeat.sec failed"));
    let max_pool_size = graph_config.get_storage_option("gaia.max.pool.size")
        .map(|config_str| config_str.parse().expect("parse gaia.max.pool.size failed"));
    let mut network_config = NetworkConfig::new(server_id, ServerAddr::new(ip, port), worker_num);
    network_config.nonblocking(nonblocking)
        .read_timeout_ms(read_timeout_ms)
        .write_timeout_ms(write_timeout_ms)
        .read_slab_size(read_slab_size)
        .no_delay(no_delay)
        .send_buffer(send_buffer)
        .heartbeat_sec(heartbeat_sec);
    GaiaConfig {
        network: Some(network_config),
        max_pool_size,
    }
}

