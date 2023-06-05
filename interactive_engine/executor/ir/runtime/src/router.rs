//
//! Copyright 2023 Alibaba Group Holding Limited.
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

use std::sync::Arc;

use graph_proxy::apis::cluster_info::ClusterInfo;
use graph_proxy::apis::partitioner::PartitionInfo;
use graph_proxy::apis::ID;
use graph_proxy::GraphProxyResult;

pub type WorkerId = u64;

/// A `Router` is used to route the data to the destination worker so that it can be properly processed,
/// especially when the underlying data has been partitioned across the cluster.
/// Given the partition information (by `PartitionInfo`) as well as how our cluster is managed (by `ClusterInfo`) and co-located with the graph data,
/// we can implement the corresponding `route` function to guide the system to transfer the data to a proper destination worker.

pub trait Router: Send + Sync + 'static {
    /// a route function that given the data, return the worker id that is going to do the query.
    fn route(&self, data: &ID) -> GraphProxyResult<WorkerId>;
}

/// A `DefaultRouter` is a default implementation of `Router` that can be used in most distributed cases.
/// For example, suppose our computer server contains 10 servers, each further forking 10 workers for processing queries.
/// In addition, the graph is partitioned into these 10 servers by the following strategy:
/// vertex of give ID is placed in the server with id i (0 to 9) i given ID % 10 == i, the vertex's adjacent edges are also placed with the vertex.
/// Then the router can decide which worker should process the vertex of ID 25534 as follows:
/// - It first do `25534 % 10 == 4`, which means it must be routed to the 4-th server.
/// - Any worker in the 4-th server can process the vertex. Thus it randomly picks a worker, saying 5-th worker, which has ID 4 * 10 + 5 = 45.
/// - Then 45-th worker will be returned for routing this vertex.
pub struct DefaultRouter<P: PartitionInfo, C: ClusterInfo> {
    partition_info: Arc<P>,
    cluster_info: Arc<C>,
}

impl<P: PartitionInfo, C: ClusterInfo> DefaultRouter<P, C> {
    pub fn new(partition_info: Arc<P>, cluster_info: Arc<C>) -> Self {
        DefaultRouter { partition_info, cluster_info }
    }
}

impl<P: PartitionInfo, C: ClusterInfo> Router for DefaultRouter<P, C> {
    fn route(&self, data: &ID) -> GraphProxyResult<WorkerId> {
        let partition_id = self.partition_info.get_partition_id(data)?;
        let server_id = self
            .partition_info
            .get_server_id(partition_id)?;
        let local_worker_num = self.cluster_info.get_local_worker_num()?;
        let random_worker_index = *data as u32 % local_worker_num;
        Ok((server_id * local_worker_num + random_worker_index) as WorkerId)
    }
}
