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

use std::time::Duration;
use std::thread;

use zookeeper::*;

pub use zookeeper::{Acl, CreateMode};

pub struct ZkClient {
    zk: ZooKeeper,
    url: String,
    timeout_ms: u64,
    auth: Option<(String, String)>,
}

impl ZkClient {
    pub fn new(url: &str, timeout_ms: u64, auth: Option<(String, String)>) -> Self {
        let zk = ZooKeeper::connect(
            url,
            Duration::from_millis(timeout_ms),
            LoggingWatcher)
            .unwrap();
        if let Some((username, password)) = auth.as_ref() {
            let auth = format!("{}:{}", username, password);
            zk.add_auth("digest", auth.as_bytes().to_vec()).expect(&format!("zk add auth {} failed", auth));
        }
        ZkClient {
            zk,
            url: url.to_owned(),
            timeout_ms,
            auth,
        }
    }

    pub fn reconnect(&self) {
        unsafe {
            let zk = &mut *(&self.zk as *const ZooKeeper as *mut ZooKeeper);
            *zk = ZooKeeper::connect(
                self.url.as_str(),
                Duration::from_millis(self.timeout_ms),
                LoggingWatcher)
                .unwrap();
            if let Some((username, password)) = self.auth.as_ref() {
                let auth = format!("{}:{}", username, password);
                zk.add_auth("digest", auth.as_bytes().to_vec()).expect(&format!("zk add auth {} failed", auth));
            }
        }
    }

    pub fn create(&self, path: &str, data: Vec<u8>, acl: Vec<Acl>, mode: CreateMode) {
        loop {
            match self.zk.create(path, data.clone(), acl.clone(), mode) {
                Ok(_) => break,
                Err(ZkError::ConnectionLoss) => self.deal_with_zk_connection_loss(),
                Err(e) => self.deal_with_zk_error(e),
            }
        }
    }

    pub fn set_data(&self, path: &str, data: Vec<u8>, version: Option<i32>) {
        loop {
            match self.zk.set_data(path, data.clone(), version.clone()) {
                Ok(_) => break,
                Err(ZkError::ConnectionLoss) => self.deal_with_zk_connection_loss(),
                Err(e) => self.deal_with_zk_error(e),
            }
        }
    }

    pub fn get_data(&self, path: &str, watch: bool) -> Option<Vec<u8>> {
        loop {
            match self.zk.get_data(path, watch) {
                Ok((ret, _)) => return Some(ret),
                Err(ZkError::NoNode) => return None,
                Err(ZkError::ConnectionLoss) => self.deal_with_zk_connection_loss(),
                Err(e) => self.deal_with_zk_error(e),
            }
        }
    }

    pub fn exists(&self, path: &str, watch: bool) -> bool {
        loop {
            match self.zk.exists(path, watch) {
                Ok(Some(_)) => return true,
                Ok(None) => return false,
                Err(ZkError::ConnectionLoss) => self.deal_with_zk_connection_loss(),
                Err(e) => self.deal_with_zk_error(e),
            }
        }
    }

    fn deal_with_zk_error(&self, e: ZkError) {
        error!("request to zk error: {:?}, sleep for 5 seconds and reconnect", e);
        thread::sleep(Duration::from_millis(5000));
        self.reconnect();
    }

    fn deal_with_zk_connection_loss(&self) {
        warn!("zk connection loss, start to reconnect");
        self.reconnect();
    }
}

struct LoggingWatcher;

impl Watcher for LoggingWatcher {
    fn handle(&self, e: WatchedEvent) {
        info!("{:?}", e.keeper_state);
    }
}
