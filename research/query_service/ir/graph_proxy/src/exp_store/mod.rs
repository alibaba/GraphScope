//
//! Copyright 2021 Alibaba Group Holding Limited.
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

mod graph_partition;
mod graph_query;

pub use graph_partition::SimplePartition;
pub use graph_query::create_demo_graph;
use runtime::IRJobAssembly;

use crate::InitializeJobCompiler;

pub struct QueryExpGraph {
    num_servers: usize,
}

impl QueryExpGraph {
    pub fn new(num_servers: usize) -> Self {
        QueryExpGraph { num_servers }
    }
}

impl InitializeJobCompiler for QueryExpGraph {
    fn initialize_job_compiler(&self) -> IRJobAssembly {
        create_demo_graph();
        let partitioner = SimplePartition { num_servers: self.num_servers.clone() };
        IRJobAssembly::new(partitioner)
    }
}
