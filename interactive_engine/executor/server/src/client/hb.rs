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

use std::sync::Arc;
use maxgraph_common::proto::data_grpc::ServerDataApiClient;
use std::time::Duration;
use super::zk::*;
use maxgraph_store::config::StoreConfig;
use maxgraph_common::proto::hb::*;
use std::thread;
use grpcio::*;

const MAX_RETRY_INTERVAL_MS: u64 = 16000;

#[allow(dead_code)]
pub struct HeartbeatClient {
    config: Arc<StoreConfig>,
    client: ServerDataApiClient,
    zk: ZKClient,
}

impl HeartbeatClient {
    pub fn new(config: Arc<StoreConfig>) -> Self {
        let zk_url = format!("{}/{}", config.zk_url, config.graph_name);
        let auth = if config.zk_auth_enable {
            Some((config.zk_auth_user.clone(), config.zk_auth_password.clone()))
        } else {
            None
        };
        let zk = ZKClient::new(zk_url.as_str(), config.zk_timeout_ms, auth);
        let client = connect_to_coordinator(&zk);
        HeartbeatClient {
            config,
            client,
            zk,
        }
    }

    fn reconnect(&mut self) {
        self.client = connect_to_coordinator(&self.zk);
    }

    pub fn send(&mut self, req: &ServerHBReq) -> ServerHBResp {
        _trace!("send heartbeat {:?}", req);
        let mut retry_interval_ms = 1000;
        loop {
            let res = self.client.heartbeat(req);
            match res {
                Ok(ret) => { return ret; }
                Err(e) => {
                    error!("heartbeat error: {:?}, try to reconnect to coordinator after {} ms", e, retry_interval_ms);
                    thread::sleep(Duration::from_millis(retry_interval_ms));
                    if retry_interval_ms < MAX_RETRY_INTERVAL_MS {
                        retry_interval_ms *= 2;
                    }
                    self.reconnect();
                }
            }
        }
    }

}


fn connect_to_coordinator(zk: &ZKClient) -> ServerDataApiClient {
    let addr = zk.get_coordinator_addr();
    let channel = ChannelBuilder::new(Arc::new(EnvBuilder::new().build()))
        .connect(addr.to_string().as_str());
    ServerDataApiClient::new(channel)
}
