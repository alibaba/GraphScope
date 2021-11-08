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
use graph_query::create_demo_graph;
use runtime::graph::{ID, ID_BITS};
use runtime::IRJobCompiler;

pub const ID_SHIFT_BITS: usize = ID_BITS >> 1;

/// Given the encoding of an edge, the `ID_MASK` is used to get the lower half part of an edge, which is
/// the src_id. As an edge is indiced by its src_id, one can use edge_id & ID_MASK to route to the
/// machine of the edge.
pub const ID_MASK: ID = ((1 as ID) << (ID_SHIFT_BITS as ID)) - (1 as ID);

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
