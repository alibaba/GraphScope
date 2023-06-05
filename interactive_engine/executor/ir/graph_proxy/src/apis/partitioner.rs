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

use super::GraphElement;
use crate::apis::Edge;
use crate::apis::Vertex;
use crate::apis::ID;
use crate::GraphProxyResult;

pub type PartitionId = u32;
pub type ServerId = u32;

pub trait PartitionedData {
    fn get_id(&self) -> ID;
}
impl PartitionedData for ID {
    fn get_id(&self) -> ID {
        *self
    }
}
impl PartitionedData for Vertex {
    fn get_id(&self) -> ID {
        self.id()
    }
}
impl PartitionedData for Edge {
    fn get_id(&self) -> ID {
        self.id()
    }
}

/// A `PartitionInfo` is used to query the partition information when the data has been partitioned.
pub trait PartitionInfo: Send + Sync + 'static {
    /// Given the data, return the id of the partition that holds the data.
    fn get_partition_id<D: PartitionedData>(&self, data: &D) -> GraphProxyResult<PartitionId>;
    /// Given the partition id, return the id of the server that is able to access the partition.
    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId>;
}
