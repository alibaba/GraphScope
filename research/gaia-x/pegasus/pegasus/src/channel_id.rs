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

use std::fmt::Debug;

use crate::graph::Port;

///计算逻辑图中的channel的id;
#[derive(Copy, Clone, Default, Hash, Eq, PartialEq, Debug)]
pub struct ChannelId {
    /// The sequence number of task the communication_old belongs to;
    pub job_seq: u64,
    /// The index of a communication_old channel in the dataflow execution plan;
    pub index: u32,
}

impl ChannelId {
    pub fn new(job_seq: u64, index: u32) -> Self {
        ChannelId { job_seq, index }
    }
}

#[derive(Copy, Clone, Default, Hash, Eq, PartialEq)]
pub struct ChannelInfo {
    pub id: ChannelId,
    pub scope_level: u32,
    pub source_peers: usize,
    pub target_peers: usize,
    pub source_port: Port,
    pub target_port: Port,
}

impl ChannelInfo {
    pub fn new(
        id: ChannelId, scope_level: u32, source_peers: usize, target_peers: usize, source_port: Port,
        target_port: Port,
    ) -> Self {
        ChannelInfo { id, scope_level, source_peers, target_peers, source_port, target_port }
    }

    pub fn index(&self) -> u32 {
        self.id.index
    }
}
