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

use std::collections::HashMap;
use maxgraph_common::proto::hb::RuntimeTaskPartitionProto;

pub struct PartitionTaskRoute {
    partition_task_list: HashMap<u32, u32>,      // partition id as index and task index as value in vec
}

impl PartitionTaskRoute {
    pub fn new(partition_task_list: HashMap<u32, u32>) -> Self {
        PartitionTaskRoute {
            partition_task_list
        }
    }

    pub fn get_task_index(&self, partition_id: u32) -> u32 {
        if let Some(task_index) = self.partition_task_list.get(&partition_id) {
            *task_index
        } else {
            0
        }
    }
}

#[derive(Clone, Debug)]
pub struct TaskPartitionManager {
    task_partition_list: HashMap<u32, Vec<u32>>,    // task global index -> partition list
    partition_task_list: HashMap<u32, u32>,         // partition id -> task global index
    partition_process_list: HashMap<u32, u32>,      // partition id -> process index
}

impl TaskPartitionManager {
    pub fn new(task_partition_list: HashMap<u32, Vec<u32>>,
               partition_task_list: HashMap<u32, u32>,
               partition_process_list: HashMap<u32, u32>) -> Self {
        TaskPartitionManager {
            task_partition_list,
            partition_task_list,
            partition_process_list,
        }
    }

    // for test
    pub fn empty() -> Self {
        TaskPartitionManager {
            task_partition_list: HashMap::new(),
            partition_task_list: HashMap::new(),
            partition_process_list: HashMap::new(),
        }
    }

    pub fn parse_proto(task_partition_proto: &[RuntimeTaskPartitionProto]) -> Self {
        let mut task_partition_list = HashMap::new();
        let mut partition_process_list = HashMap::new();
        let mut partition_task_list = HashMap::new();
        for task_partition in task_partition_proto.iter() {
            let task_index = task_partition.get_task_index();
            let process_index = task_partition.get_process_index();
            let partition_list = task_partition.get_partition_list().to_vec();
            for partition_id in partition_list.iter() {
                partition_task_list.insert(*partition_id, task_index);
                partition_process_list.insert(*partition_id, process_index);
            }
            task_partition_list.insert(task_index, partition_list);
        }

        TaskPartitionManager {
            task_partition_list,
            partition_task_list,
            partition_process_list,
        }
    }

    pub fn get_task_partition_list(&self, task_index: &u32) -> Vec<u32> {
        if let Some(partition_list) = self.task_partition_list.get(task_index) {
            partition_list.to_vec()
        } else {
            vec![]
        }
    }

    pub fn get_partition_task_list(&self) -> HashMap<u32, u32> {
        self.partition_task_list.clone()
    }

    pub fn get_partition_process_list(&self) -> HashMap<u32, u32> {
        self.partition_process_list.clone()
    }

    pub fn get_task_partition_list_mapping(&self) ->HashMap<u32, Vec<u32>> {
        self.task_partition_list.clone()
    }
}
