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

use crate::api::{VertexId, PartitionId, Vertex, Edge, MVGraph, LabelId};
use std::sync::Arc;

// Partition manager for graph query
pub trait GraphPartitionManager: Send + Sync {
    fn get_partition_id(&self, vid: VertexId) -> i32;
    fn get_server_id(&self, pid: PartitionId) -> Option<u32>;
    fn get_process_partition_list(&self) -> Vec<PartitionId>;
    fn get_vertex_id_by_primary_key(&self, label_id: LabelId, key: &String) -> Option<(PartitionId, VertexId)>;
}

pub struct ConstantPartitionManager {
    partition_id_list: Vec<PartitionId>,
    partition_id: PartitionId,
}

impl ConstantPartitionManager {
    pub fn new() -> Self {
        ConstantPartitionManager {
            partition_id_list: vec![],
            partition_id: 0,
        }
    }

    pub fn from(partition_id: PartitionId,
                partition_id_list: Vec<PartitionId>) -> Self {
        ConstantPartitionManager {
            partition_id,
            partition_id_list,
        }
    }
}

impl GraphPartitionManager for ConstantPartitionManager {
    fn get_partition_id(&self, _vid: i64) -> i32 {
        self.partition_id as i32
    }

    fn get_server_id(&self, _pid: u32) -> Option<u32> {
        //TODO(bingqing)
        unimplemented!()
    }

    fn get_process_partition_list(&self) -> Vec<PartitionId> {
        self.partition_id_list.to_vec()
    }

    fn get_vertex_id_by_primary_key(&self, _label_id: u32, _key: &String) -> Option<(u32, i64)> {
        None
    }
}

pub struct FixedStorePartitionManager<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
    partition_id_list: Vec<u32>,
}

impl<V, VI, E, EI> FixedStorePartitionManager<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    pub fn new(graph: Arc<dyn MVGraph<V=V, VI=VI, E=E, EI=EI>>,
               partition_id_list: Vec<u32>, ) -> Self {
        FixedStorePartitionManager {
            graph,
            partition_id_list,
        }
    }
}

impl<V, VI, E, EI> GraphPartitionManager for FixedStorePartitionManager<V, VI, E, EI>
    where V: Vertex,
          VI: Iterator<Item=V>,
          E: Edge,
          EI: Iterator<Item=E> {
    fn get_partition_id(&self, vid: VertexId) -> i32 {
        self.graph.as_ref().get_partition_id(vid) as i32
    }

    fn get_server_id(&self, _pid: u32) -> Option<u32> {
        // TODO(bingqing)
        unimplemented!()
    }

    fn get_process_partition_list(&self) -> Vec<PartitionId> {
        // Cant get partitions from graph directly for graph may return empty when the partitions is not active in the graph(by heartbeat)
        self.partition_id_list.clone()
    }

    fn get_vertex_id_by_primary_key(&self, _label_id: u32, _key: &String) -> Option<(u32, i64)> {
        None
    }
}
