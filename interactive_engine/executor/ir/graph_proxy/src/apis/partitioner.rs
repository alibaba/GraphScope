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

use crate::apis::Edge;
use crate::apis::GraphElement;
use crate::apis::GraphPath;
use crate::apis::Vertex;
use crate::apis::ID;
use crate::GraphProxyResult;

/// The server id is used to identify the server that is able to access the partition.
pub type ServerId = u32;
/// The partition id is used to identify the partition that holds the data.
pub type PartitionId = u32;
/// The partition key id is used to identify key of the data that is used for partitioning.
pub type PartitionKeyId = u64;

pub trait PartitionedData {
    /// To obtain the key's id of the data that is used for partitioning.
    fn get_partition_key_id(&self) -> PartitionKeyId;
}

impl PartitionedData for ID {
    fn get_partition_key_id(&self) -> PartitionKeyId {
        *self as PartitionKeyId
    }
}
impl PartitionedData for Vertex {
    fn get_partition_key_id(&self) -> PartitionKeyId {
        self.id() as PartitionKeyId
    }
}
impl PartitionedData for Edge {
    fn get_partition_key_id(&self) -> PartitionKeyId {
        self.src_id as PartitionKeyId
    }
}

impl PartitionedData for GraphPath {
    fn get_partition_key_id(&self) -> PartitionKeyId {
        match self.get_path_end() {
            super::VertexOrEdge::V(v) => v.get_partition_key_id(),
            super::VertexOrEdge::E(e) => e.get_partition_key_id(),
        }
    }
}

impl PartitionedData for PartitionKeyId {
    fn get_partition_key_id(&self) -> PartitionKeyId {
        *self
    }
}

/// A `PartitionInfo` is used to query the partition information when the data has been partitioned.
pub trait PartitionInfo: Send + Sync + 'static {
    /// Given the data, return the id of the partition that holds the data.
    fn get_partition_id<D: PartitionedData>(&self, data: &D) -> GraphProxyResult<PartitionId>;
    /// Given the partition id, return the id of the server that is able to access the partition.
    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId>;
}
