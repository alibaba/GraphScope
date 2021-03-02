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

use crate::desc::{Resource, SharedResource};
use crate::generated::protobuf as pb;
use pegasus::api::{Range, RANGES};
use pegasus::codec::Decode;
use pegasus::BuildJobError;
use pegasus_common::downcast::*;
use prost::Message;

// same as pb::OpKind, only used for easy import by outer users;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum OpKind {
    Source = 0,
    Exchange = 1,
    Aggregate = 2,
    Broadcast = 3,
    Map = 4,
    Flatmap = 5,
    Filter = 6,
    Repeat = 7,
    Subtask = 8,
    Sink = 9,
    Limit = 10,
    Sort = 11,
    Group = 12,
    Count = 13,
    Union = 14,
    Dedup = 15,
}

#[derive(Clone)]
pub enum ChannelDesc {
    Pipeline,
    Exchange(SharedResource),
    Broadcast(Option<SharedResource>),
    Aggregate(u32),
}

#[derive(Clone)]
pub struct OperatorDesc {
    pub op_kind: OpKind,
    pub ch_kind: ChannelDesc,
    pub resource: SharedResource,
}

impl OperatorDesc {
    #[inline]
    pub fn get_resource<T: 'static>(&self) -> Option<&T> {
        self.resource.as_any_ref().downcast_ref::<T>()
    }
}

#[derive(Clone)]
pub struct RepeatDesc {
    pub times: u32,
    pub until: Option<SharedResource>,
    pub body: Vec<OperatorDesc>,
}

impl From<pb::RepeatCond> for RepeatDesc {
    fn from(raw: pb::RepeatCond) -> Self {
        RepeatDesc { times: raw.times, until: SharedResource::from_bytes(raw.until), body: vec![] }
    }
}

impl_as_any!(RepeatDesc);

#[derive(Clone, Default)]
pub struct SubtaskDesc {
    pub subtask: Vec<OperatorDesc>,
    pub joiner: Option<SharedResource>,
}

#[allow(dead_code)]
impl SubtaskDesc {
    pub fn with_join<R: Resource>(joiner: R) -> Self {
        SubtaskDesc { subtask: vec![], joiner: Some(SharedResource::new(joiner)) }
    }
}

impl From<pb::Joiner> for SubtaskDesc {
    fn from(raw: pb::Joiner) -> Self {
        SubtaskDesc { subtask: vec![], joiner: SharedResource::from_bytes(raw.joiner) }
    }
}

impl_as_any!(SubtaskDesc);

#[derive(Clone, Default)]
pub struct UnionDesc {
    pub tasks: Vec<Vec<OperatorDesc>>,
}

impl_as_any!(UnionDesc);

#[derive(Clone)]
pub struct SortByDesc {
    pub range: Range,
    pub limit: Option<usize>,
    pub cmp: SharedResource,
}

impl From<pb::SortBy> for SortByDesc {
    fn from(raw: pb::SortBy) -> Self {
        let range = RANGES[raw.global as usize];
        let cmp = SharedResource::from_bytes(raw.cmp).expect("compare function lost");
        if raw.limit > 0 {
            SortByDesc { range, limit: Some(raw.limit as usize), cmp }
        } else {
            SortByDesc { range, limit: None, cmp }
        }
    }
}

impl_as_any!(SortByDesc);

#[derive(Clone)]
pub struct LimitDesc {
    pub range: Range,
    pub size: u32,
}

impl From<pb::Limit> for LimitDesc {
    fn from(raw: pb::Limit) -> Self {
        let range = RANGES[raw.global as usize];
        LimitDesc { range, size: raw.size }
    }
}

impl_as_any!(LimitDesc);

pub struct DedupDesc {
    pub range: Range,
    pub set: Option<SharedResource>,
}

impl_as_any!(DedupDesc);

#[derive(Clone)]
pub enum AccumKind {
    Count,
    Sum,
    Max(SharedResource),
    Min(SharedResource),
    ToList,
    ToSet,
    Custom(SharedResource),
}

impl_as_any!(AccumKind);

#[derive(Clone)]
pub struct GroupByDesc {
    pub range: Range,
    pub key_func: SharedResource,
    pub accum: AccumKind,
}

impl From<pb::GroupBy> for GroupByDesc {
    fn from(raw: pb::GroupBy) -> Self {
        let range = RANGES[raw.global as usize];
        let accum_kind: pb::AccumKind = unsafe { std::mem::transmute(raw.accum) };
        let key_func = SharedResource::from_bytes(raw.get_key).expect("get key resource lost;");
        let accum = match accum_kind {
            pb::AccumKind::Cnt => AccumKind::Count,
            pb::AccumKind::Sum => AccumKind::Sum,
            pb::AccumKind::Max => {
                let res =
                    SharedResource::from_bytes(raw.resource).expect("accumulate resource lost;");
                AccumKind::Max(res)
            }
            pb::AccumKind::Min => {
                let res =
                    SharedResource::from_bytes(raw.resource).expect("accumulate resource lost;");
                AccumKind::Min(res)
            }
            pb::AccumKind::ToList => AccumKind::ToList,
            pb::AccumKind::ToSet => AccumKind::ToSet,
            pb::AccumKind::Custom => {
                let res =
                    SharedResource::from_bytes(raw.resource).expect("accumulate resource lost;");
                AccumKind::Custom(res)
            }
        };
        GroupByDesc { range, key_func, accum }
    }
}

impl_as_any!(GroupByDesc);

impl From<pb::ChannelDef> for ChannelDesc {
    fn from(raw: pb::ChannelDef) -> Self {
        match raw.ch_kind {
            Some(pb::channel_def::ChKind::ToLocal(_)) | None => ChannelDesc::Pipeline,
            Some(pb::channel_def::ChKind::ToAnother(res)) => {
                ChannelDesc::Exchange(res.resource.into())
            }
            Some(pb::channel_def::ChKind::ToOthers(res)) => {
                if res.resource.is_empty() {
                    ChannelDesc::Broadcast(None)
                } else {
                    ChannelDesc::Broadcast(Some(res.resource.into()))
                }
            }
            Some(pb::channel_def::ChKind::ToOne(target)) => ChannelDesc::Aggregate(target.target),
        }
    }
}

impl OperatorDesc {
    pub fn new<R: Resource>(op_kind: OpKind, ch_kind: ChannelDesc, resource: R) -> Self {
        OperatorDesc { op_kind, ch_kind, resource: SharedResource::new(resource) }
    }

    pub fn parse(op: pb::OperatorDef) -> Result<Self, BuildJobError> {
        let ch_kind = op.ch.map(|ch| ch.into()).unwrap_or(ChannelDesc::Pipeline);
        let op_kind = unsafe { std::mem::transmute(op.op_kind) };
        match op_kind {
            OpKind::Repeat => {
                if let Some(mut nested) = op.nested_task.into_iter().nth(0).take() {
                    match nested.param.take() {
                        Some(pb::nested_task::Param::RepeatCond(r)) => {
                            let mut repeat: RepeatDesc = r.into();
                            for nested_op in nested.plan {
                                repeat.body.push(OperatorDesc::parse(nested_op)?);
                            }
                            Ok(OperatorDesc::new(op_kind, ch_kind, repeat))
                        }
                        _ => Err("Repeat condition lost")?,
                    }
                } else {
                    Err("Repeat body lost;")?
                }
            }
            OpKind::Subtask => {
                if let Some(mut nested) = op.nested_task.into_iter().nth(0).take() {
                    let mut subtask = match nested.param.take() {
                        Some(pb::nested_task::Param::Joiner(r)) => r.into(),
                        _ => SubtaskDesc::default(),
                    };
                    for nested_op in nested.plan {
                        subtask.subtask.push(OperatorDesc::parse(nested_op)?);
                    }
                    Ok(OperatorDesc::new(op_kind, ch_kind, subtask))
                } else {
                    Err("Subtask body lost")?
                }
            }
            OpKind::Union => {
                let mut tasks = UnionDesc::default();
                if op.nested_task.len() < 2 {
                    Err("Union subtasks lost")?
                }
                for task in op.nested_task {
                    let mut subtask = vec![];
                    for op in task.plan {
                        subtask.push(OperatorDesc::parse(op)?);
                    }
                    tasks.tasks.push(subtask);
                }
                Ok(OperatorDesc::new(op_kind, ch_kind, tasks))
            }
            OpKind::Limit => match pb::Limit::decode(&op.resource[0..]) {
                Ok(limit) => Ok(OperatorDesc::new(op_kind, ch_kind, LimitDesc::from(limit))),
                Err(e) => Err(format!("decode limit failure: {}", e))?,
            },
            OpKind::Count => {
                let mut reader = &op.resource[0..];
                match <u8>::read_from(&mut reader) {
                    Ok(global) => Ok(OperatorDesc::new(op_kind, ch_kind, global)),
                    Err(e) => Err(format!("decode count failure: {}", e))?,
                }
            }
            OpKind::Dedup => {
                let mut reader = &op.resource[0..];
                match <u8>::read_from(&mut reader) {
                    Ok(global) => {
                        let dedup_desc = DedupDesc { range: RANGES[global as usize], set: None };
                        Ok(OperatorDesc::new(op_kind, ch_kind, dedup_desc))
                    }
                    Err(e) => Err(format!("decode Dedup failure: {}", e))?,
                }
            }
            OpKind::Sort => match pb::SortBy::decode(&op.resource[0..]) {
                Ok(sort_by) => Ok(OperatorDesc::new(op_kind, ch_kind, SortByDesc::from(sort_by))),
                Err(e) => Err(format!("decode sort_by failure: {}", e))?,
            },
            OpKind::Group => match pb::GroupBy::decode(&op.resource[0..]) {
                Ok(group_by) => {
                    Ok(OperatorDesc::new(op_kind, ch_kind, GroupByDesc::from(group_by)))
                }
                Err(e) => Err(format!("decode group_by failure: {}", e))?,
            },
            _ => Ok(OperatorDesc::new(op_kind, ch_kind, op.resource)),
        }
    }
}
