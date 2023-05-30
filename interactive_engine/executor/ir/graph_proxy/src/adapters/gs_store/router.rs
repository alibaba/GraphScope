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

use std::collections::HashMap;
use std::sync::Arc;

use global_query::store_api::{PartitionId, VertexId};
use global_query::GraphPartitionManager;

use crate::apis::{Router, ID};
use crate::{GraphProxyError, GraphProxyResult};

/// A partition utility that one server contains multiple graph partitions for Groot Store
pub struct GrootMultiPartition {
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
}

#[allow(dead_code)]
impl GrootMultiPartition {
    pub fn new(graph_partition_manager: Arc<dyn GraphPartitionManager>) -> Self {
        GrootMultiPartition { graph_partition_manager }
    }
}

impl Router for GrootMultiPartition {
    fn route(&self, id: &ID, worker_num_per_server: usize) -> GraphProxyResult<u64> {
        // The route logics is as follows:
        // 1. `partition_id = self.graph_partition_manager.get_partition_id(*id as VertexId)` routes a given id
        // to the partition that holds its data.
        // 2. `server_index = partition_id % self.num_servers as u64` routes the partition id to the
        // server R that holds the partition
        // 3. `worker_index = partition_id % worker_num_per_server` picks up one worker to do the computation.
        // 4. `server_index * worker_num_per_server + worker_index` computes the worker index in server R
        // to do the computation.
        let vid = *id as VertexId;
        let worker_num_per_server = worker_num_per_server as u64;
        let partition_id = self
            .graph_partition_manager
            .get_partition_id(vid) as u64;
        let server_index = self
            .graph_partition_manager
            .get_server_id(partition_id as PartitionId)
            .ok_or(GraphProxyError::query_store_error(&format!(
                "get server id failed on Groot with vid of {:?}, partition_id of {:?}",
                vid, partition_id
            )))? as u64;
        let worker_index = partition_id % worker_num_per_server;
        Ok(server_index * worker_num_per_server + worker_index as u64)
    }
}

/// A partition utility that one server contains multiple graph partitions for Vineyard
/// Starting GIE with vineyard will pre-allocate partitions for each server to process,
/// thus we use graph_partitioner together with partition_server_index_mapping for data routing.
pub struct VineyardMultiPartition {
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
    // mapping of partition id -> server_index
    partition_server_index_mapping: HashMap<u32, u32>,
}

impl VineyardMultiPartition {
    pub fn new(
        graph_partition_manager: Arc<dyn GraphPartitionManager>,
        partition_server_index_mapping: HashMap<u32, u32>,
    ) -> VineyardMultiPartition {
        VineyardMultiPartition { graph_partition_manager, partition_server_index_mapping }
    }
}

impl Router for VineyardMultiPartition {
    fn route(&self, id: &ID, worker_num_per_server: usize) -> GraphProxyResult<u64> {
        // The partitioning logics is as follows:
        // 1. `partition_id = self.graph_partition_manager.get_partition_id(*id as VertexId)` routes a given id
        // to the partition that holds its data.
        // 2. `server_index = partition_id % self.num_servers as u64` routes the partition id to the
        // server R that holds the partition
        // 3. `worker_index = partition_id % worker_num_per_server` picks up one worker to do the computation.
        // 4. `server_index * worker_num_per_server + worker_index` computes the worker index in server R
        // to do the computation.
        let vid = *id as VertexId;
        let worker_num_per_server = worker_num_per_server as u64;
        let partition_id = self
            .graph_partition_manager
            .get_partition_id(vid) as PartitionId;
        let server_index = *self
            .partition_server_index_mapping
            .get(&partition_id)
            .ok_or(GraphProxyError::query_store_error(&format!(
                "get server id failed on Vineyard with vid of {:?}, partition_id of {:?}",
                vid, partition_id
            )))? as u64;
        let worker_index = partition_id as u64 % worker_num_per_server;
        Ok(server_index * worker_num_per_server + worker_index)
    }
}
