//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use maxgraph_store::api::*;

use dataflow::message::PropertyEntity;

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

pub struct PartitionCache {
    properties: Arc<RwLock<HashMap<(VertexId, LabelId), Vec<PropertyEntity>>>>,
}

impl PartitionCache {
    pub fn new() -> Self {
        PartitionCache { properties: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub fn write(&self, vertex_id :VertexId, label_id: LabelId, property: PropertyEntity) -> bool {
        let key = (vertex_id, label_id);
        let mut m = self.properties.write().expect("write properties from partitionCatche failed.");
        let vec = m.entry(key).or_insert_with(|| Vec::new());
        vec.push(property);
        true
    }

    pub fn get(&self, vertex_id :VertexId, label_id: LabelId, prop_id: i32) -> Option<PropertyEntity> {
        let key = (vertex_id, label_id);
        let m = self.properties.read().expect("read properties from partitionCache failed.");
        let vec = m.get(&key);
        vec.map_or(None, |v| {
            v.iter().find(|x| x.get_propid() == prop_id).map_or(None, |x| Some(x.clone()))
        })
    }
}

pub struct CacheStore {
    partitions: Vec<Option<PartitionCache>>,
}

impl CacheStore {
    #[allow(dead_code)]
    pub fn new(partition_ids: &Vec<u32>) -> Self {
        let mut partitions = vec![];
        let max_id = partition_ids.iter().max().unwrap_or(&0);
        for i in 0..*max_id {
            if partition_ids.contains(&i) {
                partitions.push(Some(PartitionCache::new()));
            } else {
                partitions.push(None);
            }
        }
        CacheStore { partitions }
    }

    #[allow(dead_code)]
    pub fn get_partition(&self, partition_id: PartitionId) -> Option<&PartitionCache> {
        if let Some(ref p) = self.partitions[partition_id as usize] {
            Some(p)
        } else {
            None
        }
    }
}
