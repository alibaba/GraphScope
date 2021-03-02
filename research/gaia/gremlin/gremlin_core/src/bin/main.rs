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

use gremlin_core::{create_demo_graph, GremlinJobFactory, Partitioner, ProtoReflect};
use pegasus::Configuration;
use pegasus_server::rpc::start_debug_rpc_server;
use pegasus_server::service::Service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pegasus_common::logs::init_log();
    create_demo_graph();
    pegasus::startup(Configuration::singleton()).unwrap();
    println!("try to start rpc server;");
    let partition = Partition;
    let factory = GremlinJobFactory::new(partition);
    let mut service = Service::new(factory);
    service.register_preprocess(ProtoReflect);
    start_debug_rpc_server("0.0.0.0:1234".parse().unwrap(), service).await?;
    Ok(())
}

pub struct Partition;

impl Partitioner for Partition {
    fn get_partition(&self, _id: &u128) -> u64 {
        // TODO: get partition by id
        0
    }
}
