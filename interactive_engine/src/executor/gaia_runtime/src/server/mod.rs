pub mod manager;

use std::sync::Arc;
use std::sync::mpsc::channel;
use maxgraph_common::proto::debug_grpc;
use std::thread;
use std::time::Duration;
use maxgraph_common::util::get_local_ip;
use maxgraph_store::api::prelude::*;
use maxgraph_common::proto::gremlin_query_grpc;
use maxgraph_store::config::StoreConfig;
use maxgraph_store::api::prelude::*;
use std::sync::mpsc::Sender;
use maxgraph_common::proto::hb::*;
use maxgraph_server::{StoreContext, Store, service};
use maxgraph_server::service::{GremlinRpcService, DebugService};
use tokio::runtime::Runtime;
use pegasus_server::rpc::start_rpc_server;
use std::collections::HashMap;
use pegasus_server::service::Service;
use maxgraph_common::proto::hb::*;
use maxgraph_server::heartbeat::Heartbeat;
use grpcio::{Environment, ServerBuilder, ChannelBuilder};


pub fn init_with_rpc_service<VV, VVI, EE, EEI,  FS>(config: Arc<StoreConfig>,
                                                      hb_providers: Vec<Box<FS>>,
                                                      hb_resp_senders: Vec<Sender<Arc<ServerHBResp>>>,
                                                      store_context: StoreContext<VV, VVI, EE, EEI>)
    where VV: 'static + Vertex,
          VVI: 'static  +  Iterator<Item=VV> + Send,
          EE: 'static + Edge,
          EEI: 'static  +  Iterator<Item=EE> + Send,
          FS: 'static + Fn(&mut ServerHBReq) + Send {
    let store = Arc::new(Store::new(config.clone()));
    let gremlin_service = GremlinRpcService::new(Arc::new(store_context));
    let (host, port) = start_all(store.clone(),gremlin_service);
    info!("Start rpc service successfully {} {}", host, port);
    let mut hb = Heartbeat::new(host, port as u32, store.clone(), hb_providers, hb_resp_senders);
    hb.start();
}



pub(crate) fn start_all<VV, VVI, EE, EEI>(store: Arc<Store>,
                                             gremlin_server: GremlinRpcService<VV, VVI, EE, EEI>) -> (String, u16)
    where
          VV: 'static + Vertex,
          VVI: 'static +  Send + Iterator<Item=VV>,
          EE: 'static + Edge,
          EEI: 'static + Send +  Iterator<Item=EE> {

    let (tx, rx) = channel();
    thread::spawn(move || {
        let config = store.get_config();
        let debug_service = debug_grpc::create_debug_service_api(DebugService::new(store.clone()));
        // let store_api_service = store_api_grpc::create_store_service(StoreApiServer::new(store.get_graph()));
        let gremlin_service = gremlin_query_grpc::create_gremlin_service(gremlin_server);
        // let graph_store_service = store_node_grpc::create_graph_store_service(GraphStoreServer::new(store.get_graph()));
        let env = Arc::new(Environment::new(config.rpc_thread_count as usize));
        let mut server_builder = ServerBuilder::new(env.clone())
            .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
            .register_service(debug_service)
            .register_service(gremlin_service)
            .bind("0.0.0.0", config.rpc_port as u16);
        let mut server = server_builder.build().expect("Error when build rpc server");

        server.start();
        let (_, port) = server.bind_addrs()[0];
        tx.send(port).unwrap();
        thread::sleep(Duration::from_millis(u64::max_value()));
    });
    let ip = get_local_ip();
    let port = rx.recv().unwrap();
    info!("start service success, bind address: {}:{}", ip, port);
    (ip, port)
}


