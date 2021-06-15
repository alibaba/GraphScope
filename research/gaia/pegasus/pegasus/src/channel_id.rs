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

use crate::WorkerId;
use std::fmt::Debug;

///计算逻辑图中的channel的id;
#[derive(Copy, Clone, Default, Hash, Eq, PartialEq)]
pub struct ChannelId {
    /// The sequence number of task the communication_old belongs to;
    pub job_seq: u64,
    /// The index of a communication_old channel in the dataflow execution plan;
    pub index: u32,
}

impl From<[usize; 2]> for ChannelId {
    fn from(array: [usize; 2]) -> Self {
        ChannelId { job_seq: array[0] as u64, index: array[1] as u32 }
    }
}

#[derive(Copy, Clone, Default, Hash, Eq, PartialEq)]
pub struct SubChannelId {
    /// parent communication_old id;
    pub parent: ChannelId,
    /// parallel worker index this sub-communication_old belongs to;
    pub worker: u32,
}

impl From<[usize; 3]> for SubChannelId {
    fn from(array: [usize; 3]) -> Self {
        let parent = ChannelId { job_seq: array[0] as u64, index: array[2] as u32 };
        SubChannelId { parent, worker: array[1] as u32 }
    }
}

impl From<(WorkerId, usize)> for SubChannelId {
    fn from(tuple: (WorkerId, usize)) -> Self {
        let parent = ChannelId { job_seq: tuple.0.job_id, index: tuple.1 as u32 };
        SubChannelId { parent, worker: tuple.0.index }
    }
}

impl From<(WorkerId, u32)> for SubChannelId {
    fn from(tuple: (WorkerId, u32)) -> Self {
        let parent = ChannelId { job_seq: tuple.0.job_id, index: tuple.1 };
        SubChannelId { parent, worker: tuple.0.index }
    }
}

impl From<ChannelId> for SubChannelId {
    fn from(parent: ChannelId) -> Self {
        SubChannelId { parent, worker: 0 }
    }
}

impl From<(ChannelId, usize)> for SubChannelId {
    fn from(t: (ChannelId, usize)) -> Self {
        SubChannelId { parent: t.0, worker: t.1 as u32 }
    }
}

impl From<(ChannelId, u32)> for SubChannelId {
    fn from(t: (ChannelId, u32)) -> Self {
        SubChannelId { parent: t.0, worker: t.1 }
    }
}

impl Debug for SubChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}_{}_{}]", self.parent.job_seq, self.worker, self.parent.index)
    }
}

impl SubChannelId {
    #[inline(always)]
    pub fn job_id(&self) -> u64 {
        self.parent.job_seq
    }

    #[inline(always)]
    pub fn index(&self) -> u32 {
        self.parent.index
    }

    #[inline(always)]
    pub fn worker(&self) -> u32 {
        self.worker
    }
}
