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
use serde_json;

use maxgraph_common::util::zk;

const MAX_RETRY_INTERVAL_MS: u64 = 16000;

pub struct ZKClient {
    zk: zk::ZkClient,
}

impl ZKClient {
    pub fn new(url: &str, timeout_ms: u64, auth: Option<(String, String)>) -> Self {
        ZKClient {
            zk: zk::ZkClient::new(url, timeout_ms, auth),
        }
    }

    pub fn get_coordinator_addr(&self) -> String {
        let mut retry_interval_ms = 1000;
        loop {
            if let Some(data) = self.zk.get_data("/coordinator", false) {
                if let Ok(coordinator_url) = String::from_utf8(data) {
                    let addr: CoordinatorAddr = serde_json::from_str(coordinator_url.as_str()).unwrap();
                    info!("get coordinator address: {}", addr.to_string());
                    return addr.to_string();
                } else {
                    error!("invaild string");
                }
            } else {
                error!("get zk data failed, retry after {} ms", retry_interval_ms);
            }
            thread::sleep(Duration::from_millis(retry_interval_ms));
            if retry_interval_ms < MAX_RETRY_INTERVAL_MS {
                retry_interval_ms *= 2;
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct CoordinatorAddr {
    ip: String,
    port: u32,
}

impl ToString for CoordinatorAddr {
    fn to_string(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}
