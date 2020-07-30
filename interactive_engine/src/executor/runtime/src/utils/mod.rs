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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use maxgraph_common::proto::lambda_service_grpc::LambdaServiceClient;
use std::sync::Arc;
use grpcio::{EnvBuilder, ChannelBuilder};

pub static PROP_ID: i32 = -1;
pub static PROP_ID_LABEL: i32 = -2;
pub static PROP_KEY: i32 = -3;
pub static PROP_VALUE: i32 = -4;

pub static MAX_BATCH_SIZE: i64 = 100000;

#[inline]
pub fn hash(key: &str) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize
}

pub fn get_lambda_service_client()-> Option<Arc<LambdaServiceClient>> {
    let log_dir = match ::std::env::var("LOG_DIRS") {
        Ok(log_dir) => log_dir,
        Err(e) => return None,
    };

    let port_file_pos = format!("{}/lambda-server.port", log_dir);
    info!("lambda-server.port exist?: {}", Path::new(&port_file_pos).exists());

    let line = ::std::fs::read_to_string(port_file_pos)
        .expect("Unable to read lambda server port");
    let lambda_server_port = line.trim().parse::<i32>().unwrap();

    info!("executor lambda_server_port: {}", lambda_server_port);

    let env = Arc::new(EnvBuilder::new().build());
    let lambda_server = format!("localhost:{}", lambda_server_port);
    let ch = ChannelBuilder::new(env).connect( &lambda_server);

    Some(Arc::new(LambdaServiceClient::new(ch)))
}

// pub fn get_mock_lambda_service_client() -> LambdaServiceClient {
//     let lambda_server_port = 0;
//     let env = Arc::new(EnvBuilder::new().build());
//     let lambda_server = format!("localhost:{}", lambda_server_port);
//     let ch = ChannelBuilder::new(env).connect( &lambda_server);
//
//     LambdaServiceClient::new(ch)
// }

