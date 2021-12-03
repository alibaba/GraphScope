//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use crate::config::ConnectionParams;
use crate::{NetError, Server};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};

pub trait ServerDetect: Send {
    fn fetch(&self) -> Vec<Server>;
}

#[allow(dead_code)]
enum IOMode {
    Block,
    Nonblock(usize),
}

pub(crate) struct ServerManager {
    server_id: u64,
    peer_detect: Box<dyn ServerDetect>,
    conn_params: ConnectionParams,
}

impl ServerManager {
    pub fn new<D: ServerDetect + 'static>(server_id: u64, conf: ConnectionParams, detect: D) -> Self {
        ServerManager { server_id, peer_detect: Box::new(detect), conn_params: conf }
    }

    pub fn bind<A: ToSocketAddrs>(&self, addr: A) -> Result<SocketAddr, NetError> {
        let addr = crate::transport::block::listen_on(self.server_id, self.conn_params, addr)?;
        Ok(addr)
    }

    pub fn refresh(&mut self) {
        for s in self.peer_detect.fetch() {
            if s.id < self.server_id && !crate::state::is_connected(self.server_id, s.id) {
                if let Err(e) =
                    crate::transport::block::connect(self.server_id, s.id, self.conn_params, s.addr)
                {
                    error!("fail to connect server[id={},addr={:?}], caused by {}", s.id, s.addr, e);
                }
            }
        }
    }
}

impl ServerDetect for Vec<Server> {
    fn fetch(&self) -> Vec<Server> {
        self.clone()
    }
}

pub struct SimpleServerDetector {
    peers_mutex: Mutex<Vec<Server>>,
}

impl SimpleServerDetector {
    pub fn new() -> Self {
        SimpleServerDetector { peers_mutex: Mutex::new(vec![]) }
    }

    pub fn update_peer_view<Iter: Iterator<Item = (u64, SocketAddr)>>(&self, peer_view: Iter) {
        let new_peers = peer_view.map(|(id, addr)| Server { id, addr }).collect::<Vec<Server>>();
        let mut peers = self.peers_mutex.lock().expect("unexpected error locking when update peer view");
        *peers = new_peers;
    }
}

impl ServerDetect for SimpleServerDetector {
    fn fetch(&self) -> Vec<Server> {
        let peers = self.peers_mutex.lock().expect("unexpected error locking when fetch servers");
        peers.clone()
    }
}

impl ServerDetect for Arc<SimpleServerDetector> {
    fn fetch(&self) -> Vec<Server> {
        self.as_ref().fetch()
    }
}