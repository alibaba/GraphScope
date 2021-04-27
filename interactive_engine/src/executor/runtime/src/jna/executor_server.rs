use store::v2::global_graph::GlobalGraph;
use std::sync::{Arc, Mutex, RwLock};
use maxgraph_store::config::StoreConfig;
use server::query_manager::QueryManager;
use std::net::TcpListener;
use maxgraph_store::db::api::{GraphResult, GraphError, GraphConfig};
use maxgraph_store::db::api::GraphErrorCode::InvalidOperation;
use std::collections::HashMap;
use maxgraph_store::api::PartitionId;
use jna::pegasus_server_manager::PegasusServerManager;
use maxgraph_common::util::get_local_ip;
use store::task_partition_manager::TaskPartitionManager;

use rpc::rpc_pegasus::ctrl_service::MaxGraphCtrlServiceImpl as PegasusCtrlService;
use rpc::rpc_pegasus::async_maxgraph_service::AsyncMaxGraphServiceImpl as PegasusAsyncService;
use rpc::rpc_pegasus::maxgraph_service::MaxGraphServiceImpl as PegasusService;
use store::remote_store_service::RemoteStoreServiceManager;
use std::sync::atomic::AtomicBool;
use grpcio::{Environment, ServerBuilder, ChannelBuilder, Server};
use maxgraph_server::service::GremlinRpcService;
use maxgraph_server::StoreContext;
use maxgraph_common::proto::gremlin_query_grpc;
use server::manager::{ServerManager, ManagerGuards};
use maxgraph_store::db::graph::store::GraphStore;
use std::borrow::BorrowMut;

pub struct ExecutorServer {
    graph_config: Arc<GraphConfig>,
    store_config: Arc<StoreConfig>,
    graph: Arc<GlobalGraph>,
    listener: Option<TcpListener>,
    node_id: usize,
    partition_worker_ids: HashMap<PartitionId, i32>,
    engine_server_manager: Option<Box<PegasusServerManager>>,
    manager_guards: Option<ManagerGuards<()>>,
}

impl ExecutorServer {
    pub fn new(graph_config: Arc<GraphConfig>) -> GraphResult<Self> {
        let node_id = graph_config.get_storage_option("node.idx").unwrap().parse::<usize>().unwrap();
        let store_config = Arc::new(StoreConfig::init_from_config(graph_config.get_storage_options()));
        info!("store config created");
        Ok(ExecutorServer {
            graph_config,
            store_config,
            graph: Arc::new(GlobalGraph::empty()),
            listener: None,
            node_id,
            partition_worker_ids: HashMap::new(),
            engine_server_manager: None,
            manager_guards: None,
        })
    }

    pub fn add_graph_partition(&mut self, partition_id: PartitionId, graph_store: Arc<GraphStore>) {
        Arc::get_mut(&mut self.graph).unwrap().add_partition(partition_id, graph_store);
    }

    pub fn add_partition_worker_mapping(&mut self,
                                        partition_id: PartitionId,
                                        worker_id: i32) {
        self.partition_worker_ids.insert(partition_id, worker_id);
    }

    fn build_task_partition_manager(&self) -> TaskPartitionManager {
        let mut task_partition_list = HashMap::new();
        let mut partition_task_list = HashMap::new();
        for (partition_id, worker_id) in self.partition_worker_ids.iter() {
            partition_task_list.insert(*partition_id as u32, *worker_id as u32);
            task_partition_list.entry(*worker_id as u32).or_insert(vec![]).push(*partition_id as u32);
        }
        TaskPartitionManager::new(task_partition_list, partition_task_list, HashMap::new())
    }

    pub fn start(&mut self) -> GraphResult<String> {
        if self.listener.is_some() {
            return Err(GraphError::new(InvalidOperation, "listener should be none".to_owned()));
        }
        let server_manager = PegasusServerManager::new(self.node_id,
                                                       self.store_config.pegasus_thread_pool_size as usize,
                                                       self.store_config.worker_num as usize);
        let tcp_listener = server_manager.register_listener();
        let tcp_address = tcp_listener.local_addr().expect("bind local address failed");
        self.listener = Some(tcp_listener);
        self.engine_server_manager = Some(Box::new(server_manager));
        Ok(format!("{}", tcp_address.port()))
    }

    pub fn start_rpc(&self) -> (u16, u16) {
        let task_partition_manager = Arc::new(RwLock::new(Some(self.build_task_partition_manager())));
        let signal = Arc::new(AtomicBool::new(false));
        let remote_store_service_manager = Arc::new(RwLock::new(Some(RemoteStoreServiceManager::empty())));
        let query_manager = QueryManager::new();
        let pegasus_runtime = self.engine_server_manager.as_ref().unwrap().get_server();
        let ctrl_service = PegasusCtrlService::new_service(query_manager.clone(), pegasus_runtime.clone());
        let async_maxgraph_service = PegasusAsyncService::new_service(
            self.store_config.clone(),
            pegasus_runtime.clone(),
            query_manager.clone(),
            remote_store_service_manager,
            None,
            signal,
            self.graph.clone(),
            self.graph.clone(),
            task_partition_manager);
        let maxgraph_service = PegasusService::new_service(self.store_config.clone(), query_manager.clone());
        let ctrl_and_async_server = Self::start_ctrl_and_async_service(0, ctrl_service, async_maxgraph_service).expect("Start ctrl and async service error.");
        info!("async maxgraph service and control service bind to: {:?}", ctrl_and_async_server.bind_addrs());
        let ctrl_and_async_service_port = ctrl_and_async_server.bind_addrs()[0].1;

        let gremlin_service = gremlin_query_grpc::create_gremlin_service(
            GremlinRpcService::new(
                Arc::new(StoreContext::new(self.graph.clone(),
                                           self.graph.clone()))));
        let env = Arc::new(Environment::new(self.store_config.rpc_thread_count as usize));
        let mut server_builder = ServerBuilder::new(env.clone())
            .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
            .register_service(gremlin_service)
            .bind("0.0.0.0", self.store_config.rpc_port as u16);
        let mut server = server_builder.build().expect("Error when build rpc server");
        server.start();
        let (_, port) = server.bind_addrs()[0];
        return (ctrl_and_async_service_port, port);
    }

    fn start_ctrl_and_async_service(port: u16, ctrl_service: grpcio::Service, async_maxgraph_service: grpcio::Service)
        -> Result<Server, String> {
        let env = Arc::new(Environment::new(1));
        let server_builder = ServerBuilder::new(env.clone())
            .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
            .register_service(async_maxgraph_service)
            .register_service(ctrl_service)
            .bind("0.0.0.0", port);
        let mut server = server_builder.build().map_err(|err| format!("Error when build rpc server: {:?}", err))?;
        server.start();
        Ok(server)
    }

    pub fn engine_connect(&mut self, address_list: Vec<String>) {
        if let Some(ref server_manager) = self.engine_server_manager {
            let server_result = server_manager.start_server(self.listener.take().unwrap(), address_list.clone(),
                                        self.store_config.hb_interval_ms);
            if server_result.is_ok() {
                self.manager_guards = server_result.ok();
                info!("engine server connect to {:?} success", address_list);
            } else {
                error!("{}", format!("connect engine failed {:?}", server_result.err().unwrap()));
            }
        } else {
            error!("engine server should be started first");
        }
    }
}
