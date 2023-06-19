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

use crate::apis::partitioner::{PartitionInfo, PartitionedData, ServerId};
use crate::{GraphProxyError, GraphProxyResult};

/// A partition utility that one server contains multiple graph partitions for Groot Store
pub struct GrootMultiPartition {
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
}

impl GrootMultiPartition {
    pub fn new(graph_partition_manager: Arc<dyn GraphPartitionManager>) -> Self {
        GrootMultiPartition { graph_partition_manager }
    }
}

impl PartitionInfo for GrootMultiPartition {
    fn get_partition_id<D: PartitionedData>(&self, data: &D) -> GraphProxyResult<PartitionId> {
        Ok(self
            .graph_partition_manager
            .get_partition_id(data.get_partition_key_id() as VertexId) as PartitionId)
    }
    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId> {
        self.graph_partition_manager
            .get_server_id(partition_id)
            .ok_or(GraphProxyError::query_store_error(&format!(
                "get server id failed on Groot with partition_id of {:?}",
                partition_id
            )))
    }
}

/// A partition utility that one server contains multiple graph partitions for Vineyard
/// Starting GIE with vineyard will pre-allocate partitions for each server to process,
/// thus we use graph_partitioner together with partition_server_index_mapping for data routing.
pub struct VineyardMultiPartition {
    graph_partition_manager: Arc<dyn GraphPartitionManager>,
    // mapping of partition id -> server_index
    partition_server_index_mapping: HashMap<PartitionId, ServerId>,
}

impl VineyardMultiPartition {
    pub fn new(
        graph_partition_manager: Arc<dyn GraphPartitionManager>,
        partition_server_index_mapping: HashMap<PartitionId, ServerId>,
    ) -> VineyardMultiPartition {
        VineyardMultiPartition { graph_partition_manager, partition_server_index_mapping }
    }
}

impl PartitionInfo for VineyardMultiPartition {
    fn get_partition_id<D: PartitionedData>(&self, data: &D) -> GraphProxyResult<PartitionId> {
        Ok(self
            .graph_partition_manager
            .get_partition_id(data.get_partition_key_id() as VertexId) as PartitionId)
    }
    fn get_server_id(&self, partition_id: PartitionId) -> GraphProxyResult<ServerId> {
        self.partition_server_index_mapping
            .get(&partition_id)
            .cloned()
            .ok_or(GraphProxyError::query_store_error(&format!(
                "get server id failed on Vineyard with partition_id of {:?}",
                partition_id
            )))
    }
}
