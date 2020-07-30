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

#![allow(dead_code)]
use std::thread;
use std::thread::JoinHandle;
use std::sync::Arc;
use maxgraph_common::proto::hb::*;
use maxgraph_common::proto::common::{EndpointProto};
use std::time::Duration;
use maxgraph_store::config::StoreConfig;
use crate::store::Store;
use std::sync::mpsc::Sender as SSender;
use crate::client::HeartbeatClient;
use super::common::*;
use crate::common::volatile::*;

pub struct Heartbeat<F> {
    host: String,
    port: u32,
    state: Arc<Volatile<WorkerState>>,
    store: Arc<Store>,
    client: Option<HeartbeatClient>,
    hb_providers: Option<Vec<Box<F>>>,
    hb_resp_senders: Option<Vec<SSender<Arc<ServerHBResp>>>>,
    worker: Option<JoinHandle<()>>,
    alive_id: i64,
}

impl<F> Heartbeat<F> where
    F: 'static + Fn(&mut ServerHBReq) + Send {
    pub fn new(host: String,
               port: u32,
               store: Arc<Store>,
               hb_providers: Vec<Box<F>>,
               hb_resp_senders: Vec<SSender<Arc<ServerHBResp>>>) -> Self {
        let config = store.get_config();
        let client = HeartbeatClient::new(config.clone());
        Heartbeat {
            host,
            port,
            state: Arc::new(Volatile::new(WorkerState::Normal)),
            store,
            client: Some(client),
            hb_providers: Some(hb_providers),
            hb_resp_senders: Some(hb_resp_senders),
            worker: None,
            alive_id: 0,
        }
    }

    pub fn start(&mut self) {
        let mut endpoint = EndpointProto::new();
        endpoint.set_host(self.host.clone());
        endpoint.set_port(self.port as i32);
        self.first_heartbeat(endpoint.clone());
        let mut client = self.client.take().unwrap();
        let store = self.store.clone();
        let state = self.state.clone();
        let hb_providers = self.hb_providers.take().unwrap();
        let hb_resp_senders = self.hb_resp_senders.take().unwrap();
        let alive_id = self.alive_id;
        self.worker = Some(thread::Builder::new()
            .name("heartbeat".to_owned())
            .spawn(move || {
                let config = store.get_config();
                loop {
                    match state.read() {
                        WorkerState::Normal | WorkerState::Recovered => {
                            Self::do_heartbeat(alive_id,
                                               store.as_ref(),
                                               config.as_ref(),
                                               &endpoint, &mut client,
                                               &hb_providers,
                                               &hb_resp_senders);
                        }
                        WorkerState::PrepareRecovering => {
                            state.write(WorkerState::Recovering);
                        }
                        WorkerState::Recovering => {
                            Self::send_empty_heartbeat(alive_id, config.as_ref(), store.as_ref(), &endpoint, &mut client, &hb_providers, &hb_resp_senders);
                        }
                    }

                    thread::sleep(Duration::from_millis(config.hb_interval_ms));
                }
            }).unwrap());
    }


    fn first_heartbeat(&mut self, endpoint: EndpointProto) {
        info!("send first heartbeat to get assignment");
        let mut request = ServerHBReq::new();
        let config = self.store.get_config();
        request.set_id(config.worker_id);
        request.set_endpoint(endpoint);
        request.set_aliveId(config.alive_id);
        let response = self.client.as_mut().unwrap().send(&request);
        if !response.isLegal {
            ::std::process::exit(0);
        }
        self.alive_id = response.get_aliveId() as i64;
    }

    fn do_heartbeat(alive_id: i64,
                    store: &Store,
                    config: &StoreConfig,
                    endpoint: &EndpointProto,
                    client: &mut HeartbeatClient,
                    hb_providers: &Vec<Box<F>>,
                    hb_resp_senders: &Vec<SSender<Arc<ServerHBResp>>>) {
        let mut request = ServerHBReq::new();
        request.set_id(config.worker_id);
        request.set_endpoint(endpoint.clone());
        request.set_aliveId(alive_id as u64);
        request.set_status(store.get_status());

        // build hb_req information needed by others except store
        for hb_provider in hb_providers {
            hb_provider(&mut request);
        }

        _info!("send hb: {:?}", request);
        let response = client.send(&request);
        if !response.isLegal {
            ::std::process::exit(0);
        }
        _info!("get response: {:?}", response);
        if store.get_status() == StoreStatus::RECOVERED {
            if response.get_targetStatus() == StoreStatus::STARTED {
                store.set_status(StoreStatus::STARTED);
            }
        }

        let response_clone = Arc::new(response.clone());
        for hb_resp_sender in hb_resp_senders.iter() {
            hb_resp_sender.send(response_clone.clone()).expect("send hb response failed");
        }
    }

    fn send_empty_heartbeat(alive_id: i64,
                            config: &StoreConfig,
                            store: &Store,
                            endpoint: &EndpointProto,
                            client: &mut HeartbeatClient,
                            hb_providers: &Vec<Box<F>>,
                            hb_resp_senders: &Vec<SSender<Arc<ServerHBResp>>>) {
        let mut request = ServerHBReq::new();
        request.set_id(config.worker_id);
        request.set_endpoint(endpoint.clone());
        request.set_aliveId(alive_id as u64);
        request.set_status(store.get_status());

        // build hb_req information needed by others except store
        for hb_provider in hb_providers {
            hb_provider(&mut request);
        }

        _info!("send hb: {:?}", request);
        let response = client.send(&request);
        _info!("get response: {:?}", response);
        if !response.isLegal {
            ::std::process::exit(0);
        }

        if store.get_status() == StoreStatus::INITIALING {
            if response.get_targetStatus() == StoreStatus::STARTED {
                store.set_status(StoreStatus::STARTED);
            }
        }

        let response_clone = Arc::new(response.clone());
        for hb_resp_sender in hb_resp_senders.iter() {
            hb_resp_sender.send(response_clone.clone()).expect("send hb response failed");
        }
    }
}

unsafe impl<F> Sync for Heartbeat<F> {}
