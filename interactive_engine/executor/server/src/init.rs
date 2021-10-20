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

#![allow(unused_variables)]

use std::sync::Arc;
use maxgraph_store::config::StoreConfig;
use super::Store;
use maxgraph_store::api::prelude::*;
use super::processor::*;
use std::sync::mpsc::Sender;
use crate::{service, StoreContext};
use maxgraph_common::proto::hb::*;
use crate::processor::heartbeat::*;
use crate::service::GremlinRpcService;


pub fn init_with_rpc_service<VV, VVI, EE, EEI, F, FS>(config: Arc<StoreConfig>,
                                                        rpc_service_generator: F,
                                                        hb_providers: Vec<Box<FS>>,
                                                        hb_resp_senders: Vec<Sender<Arc<ServerHBResp>>>,
                                                        store_context: StoreContext<VV, VVI, EE, EEI>)
    where VV: 'static + Vertex,
          VVI: 'static + Iterator<Item=VV>,
          EE: 'static + Edge,
          EEI: 'static + Iterator<Item=EE>,
          F: 'static + Send + FnOnce(Arc<Store>) -> Option<::grpcio::Service>,
          FS: 'static + Fn(&mut ServerHBReq) + Send {
    let store = Arc::new(Store::new(config.clone()));
    let gremlin_service = GremlinRpcService::new(Arc::new(store_context));
    let store_clone = store.clone();
    let (host, port) = service::start_all(store.clone(),
                                          move || rpc_service_generator(store_clone),
                                          gremlin_service);

    monitor::start(store.clone());
    let mut hb = Heartbeat::new(host, port as u32, store.clone(), hb_providers, hb_resp_senders);
    hb.start();
}


