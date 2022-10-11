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

use crate::apis::{Partitioner, ID};
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

impl Partitioner for GrootMultiPartition {
    fn get_partition(&self, id: &ID, worker_num_per_server: usize) -> GraphProxyResult<u64> {
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

    fn get_worker_partitions(
        &self, job_workers: usize, worker_id: u32,
    ) -> GraphProxyResult<Option<Vec<u64>>> {
        // Get worker partition list logic is as follows:
        // 1. `process_partition_list = self.graph_partition_manager.get_process_partition_list()`
        // get all partitions on current server
        // 2. 'pid % job_workers' picks one worker to do the computation.
        // and 'pid % job_workers == worker_id % job_workers' checks if current worker is the picked worker
        let mut worker_partition_list = vec![];
        let process_partition_list = self
            .graph_partition_manager
            .get_process_partition_list();
        for pid in process_partition_list {
            if pid % (job_workers as u32) == worker_id % (job_workers as u32) {
                worker_partition_list.push(pid as u64)
            }
        }
        info!(
            "job_workers {:?}, worker id: {:?},  worker_partition_list {:?}",
            job_workers, worker_id, worker_partition_list
        );
        Ok(Some(worker_partition_list))
    }
}

/// A partition utility that one server contains multiple graph partitions for Vineyard
/// Starting gaia with vineyard will pre-allocate partitions for each worker to process,
/// thus we use graph_partitioner together with partition_worker_mapping for data routing.
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

impl Partitioner for VineyardMultiPartition {
    fn get_partition(&self, id: &ID, worker_num_per_server: usize) -> GraphProxyResult<u64> {
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

    fn get_worker_partitions(
        &self, job_workers: usize, worker_id: u32,
    ) -> GraphProxyResult<Option<Vec<u64>>> {
        // Get worker partition list logic is as follows:
        // 1. `process_partition_list = self.graph_partition_manager.get_process_partition_list()`
        // get all partitions on current server
        // 2. 'pid % job_workers' picks one worker to do the computation.
        // and 'pid % job_workers == worker_id % job_workers' checks if current worker is the picked worker
        let mut worker_partition_list = vec![];
        let process_partition_list = self
            .graph_partition_manager
            .get_process_partition_list();
        for pid in process_partition_list {
            if pid % (job_workers as u32) == worker_id % (job_workers as u32) {
                worker_partition_list.push(pid as u64)
            }
        }
        info!(
            "job_workers {:?}, worker id: {:?},  worker_partition_list {:?}",
            job_workers, worker_id, worker_partition_list
        );
        Ok(Some(worker_partition_list))
    }
}
