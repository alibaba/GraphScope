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

use maxgraph_common::proto::store_api_grpc::StoreServiceClient;

use grpcio::{ChannelBuilder, EnvBuilder};
use std::sync::Arc;
use futures::Future;

pub struct StoreClientManager {
    host: String,
    port: i32,
    client: StoreServiceClient,
}

impl Clone for StoreClientManager {
    fn clone(&self) -> Self {
        StoreClientManager {
            host: self.host.clone(),
            port: self.port,
            client: self.client.clone(),
        }
    }
}

fn connect_to_store(host: &String, port: &i32) -> StoreServiceClient {
    let addr = format!("{}:{}", host, port);
    let channel = ChannelBuilder::new(Arc::new(EnvBuilder::new().build()))
        .connect(addr.as_str());
    StoreServiceClient::new(channel)
}

impl StoreClientManager {
    pub fn new(host: String, port: i32) -> Self {
        let client = connect_to_store(&host, &port);
        StoreClientManager {
            host,
            port,
            client,
        }
    }

    pub fn update(&mut self, host: String, port: i32) {
        self.host = host;
        self.port = port;
    }

    pub fn get_client(&self) -> &StoreServiceClient {
        return &self.client;
    }

    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item=(), Error=()> + Send + 'static {
        self.client.spawn(f);
    }
}
