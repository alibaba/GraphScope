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

pub mod gremlin;

pub use self::gremlin::*;
use std::sync::Arc;
use std::sync::mpsc::channel;
use grpcio::*;
use std::thread;
use std::time::Duration;
use maxgraph_common::util::get_local_ip;
use super::Store;
use maxgraph_store::api::prelude::*;
use maxgraph_common::proto::gremlin_query_grpc;


pub(crate) fn start_all<F, VV, VVI, EE, EEI>(store: Arc<Store>,
                                               rpc_service_fn: F,
                                               gremlin_server: GremlinRpcService<VV, VVI, EE, EEI>) -> (String, u16)
    where F: 'static + Send + FnOnce() -> Option<::grpcio::Service>,
          VV: 'static + Vertex,
          VVI: 'static + Iterator<Item=VV>,
          EE: 'static + Edge,
          EEI: 'static + Iterator<Item=EE> {
    let (tx, rx) = channel();
    thread::spawn(move || {
        let config = store.get_config();
        let gremlin_service = gremlin_query_grpc::create_gremlin_service(gremlin_server);
        let env = Arc::new(Environment::new(config.rpc_thread_count as usize));
        let mut server_builder = ServerBuilder::new(env.clone())
            .channel_args(ChannelBuilder::new(env).reuse_port(false).build_args())
            .register_service(gremlin_service)
            .bind("0.0.0.0", config.rpc_port as u16);
        if let Some(s) = rpc_service_fn() {
            server_builder = server_builder.register_service(s);
        }
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


