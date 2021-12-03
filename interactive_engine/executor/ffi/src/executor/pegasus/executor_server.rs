use std::sync::{Arc, RwLock};
use maxgraph_store::config::StoreConfig;
use std::net::TcpListener;
use maxgraph_store::db::api::{GraphResult, GraphError, GraphConfig};
use maxgraph_store::db::api::GraphErrorCode::InvalidOperation;
use std::collections::HashMap;
use maxgraph_store::api::PartitionId;

use maxgraph_runtime::rpc::rpc_pegasus::ctrl_service::MaxGraphCtrlServiceImpl as PegasusCtrlService;
use maxgraph_runtime::rpc::rpc_pegasus::async_maxgraph_service::AsyncMaxGraphServiceImpl as PegasusAsyncService;
use std::sync::atomic::AtomicBool;
use grpcio::{Environment, ServerBuilder, ChannelBuilder};
use maxgraph_server::service::GremlinRpcService;
use maxgraph_server::StoreContext;
use maxgraph_common::proto::gremlin_query_grpc;
use maxgraph_store::db::graph::store::GraphStore;
use std::sync::mpsc::channel;
use std::time::Duration;
use maxgraph_runtime::store::groot::global_graph::GlobalGraph;
use maxgraph_runtime::server::manager::ManagerGuards;
use maxgraph_runtime::store::task_partition_manager::TaskPartitionManager;
use maxgraph_runtime::server::query_manager::QueryManager;
use maxgraph_runtime::store::remote_store_service::RemoteStoreServiceManager;
use crate::executor::pegasus::pegasus_server_manager::PegasusServerManager;

pub struct ExecutorServer {
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
        let partition = store_config.partition_num;
        info!("store config created {:?}",store_config);
        Ok(ExecutorServer {
            store_config,
            graph: Arc::new(GlobalGraph::empty(partition)),
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
        let default_addr = format!("0.0.0.0:{}", self.store_config.engine_port);
        let tcp_listener = TcpListener::bind(&default_addr).expect("bind address failed");
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
        let ctrl_and_async_service_port = Self::start_ctrl_and_async_service(self.store_config.query_port as u16, ctrl_service, async_maxgraph_service);
        info!("async maxgraph service and control service bind to port: {:?}", ctrl_and_async_service_port);

        let gremlin_service = gremlin_query_grpc::create_gremlin_service(
            GremlinRpcService::new(
                Arc::new(StoreContext::new(self.graph.clone(),
                                           self.graph.clone()))));
        let (tx, rx) = channel();
        let rpc_thread_count = self.store_config.rpc_thread_count;
        let rpc_port = self.store_config.graph_port as u16;
        std::thread::spawn(move || {
            let env = Arc::new(Environment::new(rpc_thread_count as usize));
            let server_builder = ServerBuilder::new(env.clone())
                .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
                .register_service(gremlin_service)
                .bind("0.0.0.0", rpc_port);
            let mut server = server_builder.build().expect("Error when build rpc server");
            server.start();
            let (_, port) = server.bind_addrs()[0];
            tx.send(port).unwrap();
            std::thread::sleep(Duration::from_millis(u64::max_value()));
        });
        let port = rx.recv().unwrap();
        return (ctrl_and_async_service_port, port);
    }

    fn start_ctrl_and_async_service(port: u16, ctrl_service: grpcio::Service, async_maxgraph_service: grpcio::Service)
                                    -> u16 {
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            let env = Arc::new(Environment::new(1));
            let server_builder = ServerBuilder::new(env.clone())
                .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
                .register_service(async_maxgraph_service)
                .register_service(ctrl_service)
                .bind("0.0.0.0", port);
            let mut server = server_builder.build().expect( "Error when build rpc server");
            server.start();
            let (_, port) = server.bind_addrs()[0];
            tx.send(port).unwrap();
            std::thread::sleep(Duration::from_millis(u64::max_value()));
        });
        rx.recv().unwrap()
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
