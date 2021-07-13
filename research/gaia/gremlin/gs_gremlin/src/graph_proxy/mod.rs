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

mod global_storage;
mod util;
use gremlin_core::{Partitioner, ID, ID_MASK};
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::{PropId, VertexId};
use std::sync::Arc;

/// A partition utility that one server contains multiple graph partitions
pub struct MultiPartition {
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
}

impl MultiPartition {
    pub fn new(graph_partition_manager: Arc<dyn GraphPartitionManager>) -> Self {
        MultiPartition {
            graph_partition_manager,
        }
    }
}

impl Partitioner for MultiPartition {
    fn get_partition(&self, id: &ID, worker_num_per_server: usize) -> u64 {
        // The partitioning logics is as follows:
        // 1. `partition_id = self.graph_partition_manager.get_partition_id(*id as VertexId)` routes a given id
        // to the partition that holds its data.
        // 2. `server_index = partition_id % self.num_servers as u64` routes the partition id to the
        // server R that holds the partition
        // 3. `worker_index = partition_id % worker_num_per_server` picks up one worker to do the computation.
        // 4. `server_index * worker_num_per_server + worker_index` computes the worker index in server R
        // to do the computation.
        let vid = (*id & (ID_MASK)) as VertexId;
        let worker_num_per_server = worker_num_per_server as u64;
        let partition_id = self.graph_partition_manager.get_partition_id(vid) as u64;
        let server_index = self
            .graph_partition_manager
            .get_server_id(partition_id as PropId)
            .unwrap_or_else(|| {
                log::debug!("get server id failed in graph_partition_manager");
                0
            }) as u64;
        let worker_index = partition_id % worker_num_per_server;
        server_index * worker_num_per_server + worker_index as u64
    }
}
