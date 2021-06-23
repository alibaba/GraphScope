use maxgraph_store::db::api::{GraphConfig, GraphResult, GraphError, GraphErrorCode};
use std::sync::Arc;
use maxgraph_store::db::graph::store::GraphStore;
use std::net::SocketAddr;
use maxgraph_runtime::store::v2::global_graph::GlobalGraph;
use gaia_pegasus::Configuration;
use executor::gaia::jna_server_detector::JnaServerDetector;
use gremlin_core::{register_gremlin_types, Partition};
use maxgraph_store::db::api::GraphErrorCode::EngineError;
use tokio::runtime::Runtime;
use crate::executor::gaia::jna_server_detector::JnaServerDetector;
use tokio::sync::oneshot;
use gremlin_core::compiler::GremlinJobCompiler;
use pegasus_server::service::Service;
use pegasus_server::rpc::start_rpc_server;

pub struct GaiaServer {
    config: Arc<GraphConfig>,
    graph: Arc<GlobalGraph>,
    detector: Arc<JnaServerDetector>,
}

impl GaiaServer {
    pub fn new(config: Arc<GraphConfig>) -> GraphResult<Self> {
        let partition_count = config.get_storage_option("partition.count").unwrap().parse().unwrap();
        Ok(GaiaServer {
            config,
            graph: Arc::new(GlobalGraph::empty(partition_count)),
            detector: Arc::new(JnaServerDetector::new()),
        })
    }

    pub fn add_partition(&mut self, partition_id: i32, graph_partition: Arc<GraphStore>) {
        Arc::get_mut(&mut self.graph).unwrap().add_partition(partition_id as u32, graph_partition);
    }

    pub fn start(&self) -> GraphResult<(u16, u16)> {
        let report = self.config.get_storage_option("gaia.report")?.parse().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        let worker_num = self.config.get_storage_option("worker.num")?.parse().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        let rpc_port = match self.config.get_storage_option("gaia.server.port") {
            None => { 0 },
            Some(server_port_string) => {
                server_port_string.parse().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?
            },
        };
        let addr = format!("{}:{}", "0.0.0.0", rpc_port).parse().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        register_gremlin_types().map_err(|e| GraphError::new(EngineError, "register gremlin types failed".to_string()))?;
        let gaia_config = make_gaia_config(self.config.clone());
        let socket_addr = gaia_pegasus::startup_with(gaia_config, self.detector.clone())
            .map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))??;

        let (tx, rx) = oneshot::channel();
        let mut rt = Runtime::new().map_err(|e| GraphError::new(EngineError, format!("{:?}", e)))?;
        rt.block_on(async {
            let partition = Partition { num_servers: 0, };
            let factory = GremlinJobCompiler::new(partition, worker_num, gaia_config.server_id());
            let service = Service::new(factory);
            let local_addr = start_rpc_server(addr, service, report).await?;
            tx.send(local_addr.port()).unwrap();
        });
        let server_port = futures::executor::block_on(rx).unwrap();
        Ok((socket_addr.port(), server_port))
    }

    pub fn update_peer_view(&self, peer_view: Vec<(u64, SocketAddr)>) {
        self.detector.update_peer_view(peer_view);
    }

}

fn make_gaia_config(graph_config: Arc<GraphConfig>) -> Configuration {
    unimplemented!()
}
