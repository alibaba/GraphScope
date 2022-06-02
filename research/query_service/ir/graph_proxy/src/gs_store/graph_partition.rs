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
use std::sync::{Arc, RwLock};

use maxgraph_store::api::graph_partition::GraphPartitionManager;
use maxgraph_store::api::{PartitionId, VertexId};
use pegasus::api::function::FnResult;
use runtime::error::FnExecError;
use runtime::graph::partitioner::Partitioner;
use runtime::graph::ID;

/// A partition utility that one server contains multiple graph partitions for MaxGraph (V2) Store
pub struct MaxGraphMultiPartition {
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
}

#[allow(dead_code)]
impl MaxGraphMultiPartition {
    pub fn new(graph_partition_manager: Arc<dyn GraphPartitionManager>) -> Self {
        MaxGraphMultiPartition { graph_partition_manager }
    }
}

impl Partitioner for MaxGraphMultiPartition {
    fn get_partition(&self, id: &ID, worker_num_per_server: usize) -> FnResult<u64> {
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
            .ok_or(FnExecError::query_store_error("get server id failed in graph_partition_manager"))?
            as u64;
        let worker_index = partition_id % worker_num_per_server;
        Ok(server_index * worker_num_per_server + worker_index as u64)
    }

    fn get_worker_partitions(&self, job_workers: usize, worker_id: u32) -> FnResult<Option<Vec<u64>>> {
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
    // mapping of partition id -> worker id
    partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
    // mapping of worker id -> partition list
    worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>,
    num_servers: usize,
}

impl VineyardMultiPartition {
    pub fn new(
        graph_partition_manager: Arc<dyn GraphPartitionManager>,
        partition_worker_mapping: Arc<RwLock<Option<HashMap<u32, u32>>>>,
        worker_partition_list_mapping: Arc<RwLock<Option<HashMap<u32, Vec<u32>>>>>, num_servers: usize,
    ) -> VineyardMultiPartition {
        VineyardMultiPartition {
            graph_partition_manager,
            partition_worker_mapping,
            worker_partition_list_mapping,
            num_servers,
        }
    }
}

impl Partitioner for VineyardMultiPartition {
    fn get_partition(&self, id: &ID, worker_num_per_server: usize) -> FnResult<u64> {
        // The partitioning logics is as follows:
        // 1. `partition_id = self.graph_partition_manager.get_partition_id(*id as VertexId)` routes a given id
        // to the partition that holds its data.
        // 2. get worker_id by the prebuild partition_worker_map, which specifies partition_id -> worker_id

        // Firstly, we check if the job parallelism is identical to the pre-allocated parallelism,
        let parallelism = self
            .worker_partition_list_mapping
            .read()
            .unwrap()
            .as_ref()
            .map_or(0, |map| map.len());
        if self.num_servers * worker_num_per_server != parallelism {
            Err(FnExecError::query_store_error(
                "Job parallelism is not identical to the pre-allocated parallelism",
            ))?
        } else {
            let vid = *id as VertexId;
            let partition_id = self
                .graph_partition_manager
                .get_partition_id(vid) as PartitionId;
            if let Ok(partition_worker_mapping) = self.partition_worker_mapping.read() {
                if let Some(partition_worker_mapping) = partition_worker_mapping.as_ref() {
                    if let Some(worker_id) = partition_worker_mapping.get(&partition_id) {
                        Ok(*worker_id as u64)
                    } else {
			Err(FnExecError::query_store_error(&format!(
                            "get worker id failed in VineyardMultiPartition with partition id  {:?} {:?}",
                            id, partition_id
                        )))?
                    }
                } else {
                    Err(FnExecError::query_store_error(
                        "partition_worker_mapping is not initialized in VineyardMultiPartition",
                    ))?
                }
            } else {
                Err(FnExecError::query_store_error(
                    "read partition_worker_mapping in VineyardMultiPartition failed",
                ))?
            }
        }
    }

    fn get_worker_partitions(&self, job_workers: usize, worker_id: u32) -> FnResult<Option<Vec<u64>>> {
        // If only one worker each server, it will process all partitions
        if job_workers == 1 {
            Ok(Some(
                self.graph_partition_manager
                    .get_process_partition_list()
                    .into_iter()
                    .map(|pid| pid as u64)
                    .collect(),
            ))
        }
        // Vineyard will pre-allocate the worker_partition_list mapping
        else if let Ok(worker_partition_list_mapping) = self.worker_partition_list_mapping.read() {
            if let Some(worker_partition_list_mapping) = worker_partition_list_mapping.as_ref() {
                if let Some(partition_list) = worker_partition_list_mapping.get(&worker_id) {
                    Ok(Some(
                        partition_list
                            .iter()
                            .map(|pid| *pid as u64)
                            .collect(),
                    ))
                } else {
                    Err(FnExecError::query_store_error(
                        "get worker partitions failed in VineyardMultiPartition",
                    ))?
                }
            } else {
                Err(FnExecError::query_store_error(
                    "worker_partition_list is not initialized in VineyardMultiPartition",
                ))?
            }
        } else {
            Err(FnExecError::query_store_error(
                "read worker_partition_list failed in VineyardMultiPartition",
            ))?
        }
    }
}
