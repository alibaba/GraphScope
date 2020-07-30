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

pub fn assign_single_partition(vid: i64, partition_id: u32, partition_vertex_list: &mut Vec<(u32, Vec<i64>)>) {
    for (curr_partition_id, vid_list) in partition_vertex_list.iter_mut() {
        if *curr_partition_id == partition_id {
            if !vid_list.contains(&vid) {
                vid_list.push(vid);
            }
            return;
        }
    }

    partition_vertex_list.push((partition_id, vec![vid]));
}

pub fn assign_empty_partition(partition_id_list: &Vec<u32>, partition_vertex_list: &mut Vec<(u32, Vec<i64>)>) {
    partition_vertex_list.clear();
    for partition_id in partition_id_list.iter() {
        partition_vertex_list.push((*partition_id, vec![]));
    }
}

pub fn assign_all_partition(vid: i64, partition_vertex_list: &mut Vec<(u32, Vec<i64>)>) {
    for (_, vidlist) in partition_vertex_list.iter_mut() {
        if !vidlist.contains(&vid) {
            vidlist.push(vid);
        }
    }
}

pub fn assign_vertex_label_partition(label_id: Option<u32>,
                                     vid: i64,
                                     partition_id: u32,
                                     partition_vertex_list: &mut Vec<(u32, Vec<(Option<u32>, Vec<i64>)>)>) {
    for (curr_partition_id, vid_list) in partition_vertex_list.iter_mut() {
        if *curr_partition_id == partition_id {
            for (label_option, vidlist) in vid_list.iter_mut() {
                if (label_id.is_none() && label_option.is_none()) ||
                    (label_id.is_some() &&
                        label_option.is_some() &&
                        label_id.unwrap() == label_option.unwrap()) {
                    if !vidlist.contains(&vid) {
                        vidlist.push(vid);
                    }
                    return;
                }
            }
        }
    }

    partition_vertex_list.push((partition_id, vec![(label_id, vec![vid])]));
}
