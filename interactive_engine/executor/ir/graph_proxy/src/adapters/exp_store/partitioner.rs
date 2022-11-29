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

use crate::apis::{Partitioner, ID};
use crate::GraphProxyResult;

/// A simple partition utility that one server contains a single graph partition
pub struct SimplePartition {
    pub num_servers: usize,
}

impl Partitioner for SimplePartition {
    fn get_partition(&self, id: &ID, workers: usize) -> GraphProxyResult<u64> {
        let id_usize = *id as usize;
        let magic_num = id_usize / self.num_servers;
        // The partitioning logics is as follows:
        // 1. `R = id - magic_num * num_servers = id % num_servers` routes a given id
        // to the machine R that holds its data.
        // 2. `R * workers` shifts the worker's id in the machine R.
        // 3. `magic_num % workers` then picks up one of the workers in the machine R
        // to do the computation.
        Ok(((id_usize - magic_num * self.num_servers) * workers + magic_num % workers) as u64)
    }

    fn get_worker_partitions(
        &self, _job_workers: usize, _worker_id: u32,
    ) -> GraphProxyResult<Option<Vec<u64>>> {
        // In graph that one server contains a single graph partition,
        // there's no need to assign the specific partition id for query (as all workers will scan part of current partition).
        // In source scan, workers will scan the vertices in a parallel way
        Ok(None)
    }
}
