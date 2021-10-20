pub mod manager;

use grpcio::{ChannelBuilder, Environment, ServerBuilder};
use maxgraph_common::proto::gremlin_query_grpc;
use maxgraph_common::proto::hb::*;
use maxgraph_common::util::get_local_ip;
use maxgraph_server::heartbeat::Heartbeat;
use maxgraph_server::service::GremlinRpcService;
use maxgraph_server::{Store, StoreContext};
use maxgraph_store::api::prelude::*;
use maxgraph_store::config::StoreConfig;

use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub fn init_with_rpc_service<VV, VVI, EE, EEI, FS>(
    config: Arc<StoreConfig>,
    hb_providers: Vec<Box<FS>>,
    hb_resp_senders: Vec<Sender<Arc<ServerHBResp>>>,
    store_context: StoreContext<VV, VVI, EE, EEI>,
) where
    VV: 'static + Vertex,
    VVI: 'static + Iterator<Item = VV> + Send,
    EE: 'static + Edge,
    EEI: 'static + Iterator<Item = EE> + Send,
    FS: 'static + Fn(&mut ServerHBReq) + Send,
{
    let store = Arc::new(Store::new(config.clone()));
    let gremlin_service = GremlinRpcService::new(Arc::new(store_context));
    let (host, port) = start_all(store.clone(), gremlin_service);
    info!("Start rpc service successfully {} {}", host, port);
    let mut hb = Heartbeat::new(
        host,
        port as u32,
        store.clone(),
        hb_providers,
        hb_resp_senders,
    );
    hb.start();
}

pub(crate) fn start_all<VV, VVI, EE, EEI>(
    store: Arc<Store>,
    gremlin_server: GremlinRpcService<VV, VVI, EE, EEI>,
) -> (String, u16)
where
    VV: 'static + Vertex,
    VVI: 'static + Send + Iterator<Item = VV>,
    EE: 'static + Edge,
    EEI: 'static + Send + Iterator<Item = EE>,
{
    let (tx, rx) = channel();
    thread::spawn(move || {
        let config = store.get_config();
        let gremlin_service = gremlin_query_grpc::create_gremlin_service(gremlin_server);
        let env = Arc::new(Environment::new(config.rpc_thread_count as usize));
        let server_builder = ServerBuilder::new(env.clone())
            .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
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
