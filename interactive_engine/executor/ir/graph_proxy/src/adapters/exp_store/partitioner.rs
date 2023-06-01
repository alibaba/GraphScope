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

use crate::apis::partitioner::{ClusterInfo, PartitionId, PartitionInfo, ServerId};
use crate::apis::ID;
use crate::GraphProxyResult;

/// A simple partition utility that one server contains a single graph partition
pub struct SimplePartition {
    pub num_servers: usize,
}

impl PartitionInfo for SimplePartition {
    fn get_partition_id(&self, data: &ID) -> GraphProxyResult<PartitionId> {
        Ok((*data as usize % self.num_servers) as u32)
    }
}

impl ClusterInfo for SimplePartition {
    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId> {
        Ok(partition_id)
    }
}
