//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use maxgraph_store::api::prelude::Property;
use maxgraph_store::api::{LabelId, PartitionId, VertexId};

// Partition manager for graph query
pub trait GraphPartitionManager: Send + Sync {
    fn get_partition_id(&self, vid: VertexId) -> i32;
    fn get_server_id(&self, pid: PartitionId) -> Option<u32>;
    fn get_process_partition_list(&self) -> Vec<PartitionId>;
    fn get_vertex_id_by_primary_key(
        &self, label_id: LabelId, key: &String,
    ) -> Option<(PartitionId, VertexId)>;
    fn get_vertex_id_by_primary_keys(&self, label_id: LabelId, pks: &[Property]) -> Option<VertexId>;
}
