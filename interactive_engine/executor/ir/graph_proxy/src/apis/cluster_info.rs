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

use crate::{GraphProxyError, GraphProxyResult};

/// A `ClusterInfo` is used to query the cluster information when the system is running on a cluster.
pub trait ClusterInfo: Send + Sync + 'static {
    /// Return the number of servers in the cluster.
    fn get_server_num(&self) -> GraphProxyResult<u32>;
    /// Return the index of current server in the cluster.
    fn get_server_index(&self) -> GraphProxyResult<u32>;
    /// Return the number of workers in current server.
    fn get_local_worker_num(&self) -> GraphProxyResult<u32>;
    /// Return the index of current worker in the cluster.
    fn get_worker_index(&self) -> GraphProxyResult<u32>;
}

#[derive(Default)]
pub struct PegasusClusterInfo {}

impl ClusterInfo for PegasusClusterInfo {
    fn get_server_num(&self) -> GraphProxyResult<u32> {
        pegasus::get_current_worker_checked()
            .as_ref()
            .map(|info| info.servers)
            .ok_or(GraphProxyError::cluster_info_missing("server number"))
    }

    fn get_server_index(&self) -> GraphProxyResult<u32> {
        pegasus::get_current_worker_checked()
            .as_ref()
            .map(|info| info.server_index)
            .ok_or(GraphProxyError::cluster_info_missing("server index"))
    }

    fn get_local_worker_num(&self) -> GraphProxyResult<u32> {
        pegasus::get_current_worker_checked()
            .as_ref()
            .map(|info| info.local_peers)
            .ok_or(GraphProxyError::cluster_info_missing("local worker number"))
    }

    fn get_worker_index(&self) -> GraphProxyResult<u32> {
        pegasus::get_current_worker_checked()
            .as_ref()
            .map(|info| info.index)
            .ok_or(GraphProxyError::cluster_info_missing("worker index"))
    }
}
