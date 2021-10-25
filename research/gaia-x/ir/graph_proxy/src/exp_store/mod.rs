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

use crate::exp_store::graph_partition::SinglePartition;
use crate::InitializeJobCompiler;
pub use graph_query::{create_demo_graph, encode_store_e_id, ID_MASK};
use runtime::IRJobCompiler;

pub struct QueryExpGraph {
    num_servers: usize,
}

impl QueryExpGraph {
    pub fn new(num_servers: usize) -> Self {
        QueryExpGraph { num_servers }
    }
}

impl InitializeJobCompiler for QueryExpGraph {
    fn initialize_job_compiler(&self) -> IRJobCompiler {
        create_demo_graph();
        let partitioner = SinglePartition {
            num_servers: self.num_servers.clone(),
        };
        IRJobCompiler::new(partitioner)
    }
}
