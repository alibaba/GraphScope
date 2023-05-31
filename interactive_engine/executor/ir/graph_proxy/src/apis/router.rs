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

use crate::apis::ID;
use crate::GraphProxyResult;

pub type PartitionId = u32;
pub type ServerId = u32;
pub type WorkerId = u64;

/// A `PartitionInfo` is used to query the partition information when the data has been partitioned.
pub trait PartitionInfo: Send + 'static {
    /// Given the data, return the id of the partition that holds the data.
    fn get_partition_id(&self, data: &ID) -> GraphProxyResult<PartitionId>;
}

/// A `ClusterInfo` is used to query the cluster information when the system is running on a cluster.
pub trait ClusterInfo {
    /// Given the partition id, return the id of the server that is able to access the partition.
    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId>;
}

/// A `Router` is used to route the data to the destination worker so that it can be properly processed,
/// especially when the underlying data has been partitioned across the cluster.
/// Given the partition information (by `PartitionInfo`) as well as how our cluster is managed (by `ClusterInfo`) and co-located with the graph data,
/// we can implement the corresponding `route` function to guide the system to transfer the data to a proper destination worker.
///
/// For example, suppose our computer server contains 10 servers, each further forking 10 workers for processing queries.
/// In addition, the graph is partitioned into these 10 servers by the following strategy:
/// vertex of give ID is placed in the server with id i (0 to 9) i given ID % 10 == i, the vertex's adjacent edges are also placed with the vertex.
/// Then the router can decide which worker should process the vertex of ID 25534 as follows:
/// - It first do `25534 % 10 == 4`, which means it must be routed to the 4-th server.
/// - Any worker in the 4-th server can process the vertex. Thus it randomly picks a worker, saying 5-th worker, which has ID 4 * 10 + 5 = 45.
/// - Then 45-th worker will be returned for routing this vertex.
pub trait Router: PartitionInfo + ClusterInfo + Send + Sync + 'static {
    /// a route function that given the data, return the worker id that is going to do the query.
    fn route(&self, data: &ID, job_workers: usize) -> GraphProxyResult<WorkerId> {
        // a default implementation that pick a random worker to do the query.
        let partition_id = self.get_partition_id(data)?;
        let server_id = self.get_server_id(partition_id)?;
        let random_worker_index = (*data as usize % job_workers) as u32;
        Ok((server_id * job_workers as u32 + random_worker_index) as u64)
    }
}
