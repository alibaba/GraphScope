use maxgraph_store::db::api::{GraphConfig, GraphResult, GraphError};
use std::sync::Arc;
use maxgraph_store::db::graph::store::GraphStore;
use std::net::SocketAddr;
use maxgraph_runtime::store::v2::global_graph::GlobalGraph;
use gaia_pegasus::Configuration;
use gremlin_core::{register_gremlin_types, Partition};
use maxgraph_store::db::api::GraphErrorCode::EngineError;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use gremlin_core::compiler::GremlinJobCompiler;
use pegasus_server::service::Service;
use pegasus_server::rpc::start_rpc_server;
use pegasus_network::manager::SimpleServerDetector;
use pegasus_network::config::NetworkConfig;

pub struct GaiaServer {
    config: Arc<GraphConfig>,
    graph: Arc<GlobalGraph>,
    detector: Arc<SimpleServerDetector>,
}

impl GaiaServer {
    pub fn new(config: Arc<GraphConfig>) -> GraphResult<Self> {
        let partition_count = config.get_storage_option("partition.count").unwrap().parse().unwrap();
        Ok(GaiaServer {
            config,
            graph: Arc::new(GlobalGraph::empty(partition_count)),
            detector: Arc::new(SimpleServerDetector::new()),
        })
    }

    pub fn add_partition(&mut self, partition_id: i32, graph_partition: Arc<GraphStore>) {
        Arc::get_mut(&mut self.graph).unwrap().add_partition(partition_id as u32, graph_partition);
    }

    pub fn start(&self) -> GraphResult<(u16, u16)> {
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
        let rpc_port = match self.config.get_storage_option("gaia.server.port") {
            None => { 0 },
            Some(server_port_string) => {
                server_port_string.parse().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?
            },
        };
        let addr = format!("{}:{}", "0.0.0.0", rpc_port).parse()
            .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        register_gremlin_types().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        let gaia_config = make_gaia_config(self.config.clone());
        let server_id = gaia_config.server_id();
        let socket_addr = gaia_pegasus::startup_with(gaia_config, self.detector.clone())
            .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?
            .ok_or(GraphError::new(EngineError, "gaia engine return None addr".to_string()))?;

        let (tx, rx) = oneshot::channel();
        let rt = Runtime::new().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        rt.block_on(async {
            // TODO: later integrate with new store
            let partition = Partition { num_servers: 0, };
            let factory = GremlinJobCompiler::new(partition, worker_num, server_id);
            let service = Service::new(factory);
            let local_addr = start_rpc_server(addr, service, report).await.unwrap();
            tx.send(local_addr.port()).unwrap();
        });
        let rpc_port = futures::executor::block_on(rx).unwrap();
        Ok((socket_addr.port(), rpc_port))
    }

    pub fn update_peer_view(&self, peer_view: Vec<(u64, SocketAddr)>) {
        self.detector.update_peer_view(peer_view.into_iter());
    }

    pub fn stop(&self) {
        gaia_pegasus::shutdown_all();
    }
}

fn make_gaia_config(graph_config: Arc<GraphConfig>) -> Configuration {
    let server_id = graph_config.get_storage_option("node.idx").expect("required config node.idx is missing")
        .parse().expect("parse node.idx failed");
    let ip = "0.0.0.0".to_string();
    let port = graph_config.get_storage_option("gaia.engine.port").expect("required config gaia.engine.port is missing")
        .parse().expect("parse gaia.engine.port failed");
    let nonblocking = graph_config.get_storage_option("gaia.nonblocking")
        .map(|config_str| config_str.parse().expect("parse gaia.nonblocking failed"));
    let read_timeout_ms = graph_config.get_storage_option("gaia.read.timeout.ms")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let write_timeout_ms = graph_config.get_storage_option("gaia.write.timeout.ms")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let read_slab_size = graph_config.get_storage_option("gaia.read.slab.size")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let no_delay = graph_config.get_storage_option("gaia.no.delay")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let send_buffer = graph_config.get_storage_option("gaia.send.buffer")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let heartbeat_sec = graph_config.get_storage_option("gaia.heartbeat.sec")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let max_pool_size = graph_config.get_storage_option("gaia.max.pool.size")
        .map(|config_str| config_str.parse().expect("parse gaia.read.timeout.ms failed"));
    let network_config = NetworkConfig {
        server_id,
        ip,
        port,
        nonblocking,
        read_timeout_ms,
        write_timeout_ms,
        read_slab_size,
        no_delay,
        send_buffer,
        heartbeat_sec,
        peers: None,
    };
    Configuration {
        network: Some(network_config),
        max_pool_size,
    }
}
