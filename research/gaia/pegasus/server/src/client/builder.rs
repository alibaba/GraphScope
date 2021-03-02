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

use crate::desc::OpKind;
use crate::generated::protobuf as pb;
use pegasus::api::Range;
use pegasus::{BuildJobError, JobConf};
use prost::Message;
use std::ops::{Deref, DerefMut};

pub struct Plan {
    plan: Vec<pb::OperatorDef>,
}

impl Default for Plan {
    fn default() -> Self {
        Plan { plan: vec![] }
    }
}

impl Deref for Plan {
    type Target = Vec<pb::OperatorDef>;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

impl DerefMut for Plan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.plan
    }
}

pub type BinaryResource = Vec<u8>;

impl pb::OperatorDef {
    #[inline]
    pub fn kind(&self) -> OpKind {
        unsafe { std::mem::transmute(self.op_kind) }
    }
}

#[inline]
fn new_operator(kind: OpKind, ch: pb::ChannelDef) -> pb::OperatorDef {
    pb::OperatorDef { op_kind: kind as i32, ch: Some(ch), resource: vec![], nested_task: vec![] }
}

#[inline(always)]
fn pipeline() -> pb::ChannelDef {
    pb::ChannelDef { ch_kind: Some(pb::channel_def::ChKind::ToLocal(pb::Pipeline {})) }
}

impl Plan {
    pub fn exchange(&mut self, route: BinaryResource) -> &mut Self {
        let ch_kind = Some(pb::channel_def::ChKind::ToAnother(pb::Exchange { resource: route }));
        let ch = pb::ChannelDef { ch_kind };
        let op = new_operator(OpKind::Exchange, ch);
        self.plan.push(op);
        self
    }

    pub fn broadcast(&mut self) -> &mut Self {
        let ch_kind = Some(pb::channel_def::ChKind::ToOthers(pb::Broadcast { resource: vec![] }));
        let ch = pb::ChannelDef { ch_kind };
        let op = new_operator(OpKind::Broadcast, ch);
        self.plan.push(op);
        self
    }

    pub fn broadcast_by(&mut self, route: BinaryResource) -> &mut Self {
        let ch_kind = Some(pb::channel_def::ChKind::ToOthers(pb::Broadcast { resource: route }));
        let ch = pb::ChannelDef { ch_kind };
        let op = new_operator(OpKind::Broadcast, ch);
        self.plan.push(op);
        self
    }

    pub fn aggregate(&mut self, target: u32) -> &mut Self {
        let ch_kind = Some(pb::channel_def::ChKind::ToOne(pb::Aggregate { target }));
        let ch = pb::ChannelDef { ch_kind };
        let op = new_operator(OpKind::Aggregate, ch);
        self.plan.push(op);
        self
    }

    pub fn map(&mut self, func: BinaryResource) -> &mut Self {
        let mut op = new_operator(OpKind::Map, pipeline());
        op.resource = func;
        self.try_chain(&mut op);
        self.plan.push(op);
        self
    }

    pub fn flat_map(&mut self, func: BinaryResource) -> &mut Self {
        let mut op = new_operator(OpKind::Flatmap, pipeline());
        op.resource = func;
        self.try_chain(&mut op);
        self.plan.push(op);
        self
    }

    pub fn filter(&mut self, func: BinaryResource) -> &mut Self {
        let mut op = new_operator(OpKind::Flatmap, pipeline());
        op.resource = func;
        self.try_chain(&mut op);
        self.plan.push(op);
        self
    }

    pub fn limit(&mut self, n: Range, size: u32) -> &mut Self {
        let limit = match n {
            Range::Local => pb::Limit { global: false, size },
            Range::Global => pb::Limit { global: true, size },
        };

        let size = limit.encoded_len();
        let mut buf = Vec::with_capacity(size);
        limit.encode(&mut buf).expect("pb::Limit encode failure;");
        let mut op = new_operator(OpKind::Limit, pipeline());
        op.resource = buf;
        self.plan.push(op);
        self
    }

    pub fn repeat<F>(&mut self, times: u32, mut func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut plan = Plan::default();
        func(&mut plan);
        let mut op = new_operator(OpKind::Repeat, pipeline());
        let param =
            Some(pb::nested_task::Param::RepeatCond(pb::RepeatCond { times, until: vec![] }));
        let nested = pb::NestedTask { plan: plan.take(), param };
        op.nested_task = vec![nested];
        self.plan.push(op);
        self
    }

    pub fn repeat_until<F>(&mut self, times: u32, until: BinaryResource, mut func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut plan = Plan::default();
        func(&mut plan);
        let mut op = new_operator(OpKind::Repeat, pipeline());
        let param = Some(pb::nested_task::Param::RepeatCond(pb::RepeatCond { times, until }));
        let nested = pb::NestedTask { plan: plan.take(), param };
        op.nested_task = vec![nested];
        self.plan.push(op);
        self
    }

    pub fn fork<F>(&mut self, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut plan = Plan::default();
        subtask(&mut plan);
        let mut op = new_operator(OpKind::Subtask, pipeline());
        let param = Some(pb::nested_task::Param::Joiner(pb::Joiner { joiner: vec![] }));
        let nested = pb::NestedTask { plan: plan.take(), param };
        op.nested_task = vec![nested];
        self.plan.push(op);
        self
    }

    pub fn fork_join<F>(&mut self, joiner: BinaryResource, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut plan = Plan::default();
        subtask(&mut plan);
        let mut op = new_operator(OpKind::Subtask, pipeline());
        let param = Some(pb::nested_task::Param::Joiner(pb::Joiner { joiner }));
        let nested = pb::NestedTask { plan: plan.take(), param };
        op.nested_task = vec![nested];
        self.plan.push(op);
        self
    }

    pub fn take(self) -> Vec<pb::OperatorDef> {
        self.plan
    }

    #[inline]
    fn try_chain(&mut self, op: &mut pb::OperatorDef) {
        if let Some(pre) = self.plan.pop() {
            let kind = pre.kind();
            if pre.nested_task.is_empty()
                && (OpKind::Exchange == kind
                    || OpKind::Broadcast == kind
                    || OpKind::Aggregate == kind)
            {
                assert!(pre.resource.is_empty());
                let pb::OperatorDef { op_kind: _, ch, resource: _, nested_task: _ } = pre;
                op.ch = ch;
            } else {
                self.plan.push(pre);
            }
        }
    }
}

pub struct JobBuilder {
    conf: JobConf,
    source: BinaryResource,
    plan: Plan,
}

impl JobBuilder {
    pub fn new(conf: JobConf) -> Self {
        JobBuilder { conf, source: vec![], plan: Default::default() }
    }

    pub fn add_source(&mut self, src: BinaryResource) -> &mut Self {
        self.source = src;
        self
    }

    pub fn exchange(&mut self, route: BinaryResource) -> &mut Self {
        self.plan.exchange(route);
        self
    }

    pub fn broadcast(&mut self) -> &mut Self {
        self.plan.broadcast();
        self
    }

    pub fn broadcast_by(&mut self, route: BinaryResource) -> &mut Self {
        self.plan.broadcast_by(route);
        self
    }

    pub fn aggregate(&mut self, target: u32) -> &mut Self {
        self.plan.aggregate(target);
        self
    }

    pub fn map(&mut self, func: BinaryResource) -> &mut Self {
        self.plan.map(func);
        self
    }

    pub fn flat_map(&mut self, func: BinaryResource) -> &mut Self {
        self.plan.flat_map(func);
        self
    }

    pub fn filter(&mut self, func: BinaryResource) -> &mut Self {
        self.plan.filter(func);
        self
    }

    pub fn limit(&mut self, n: Range, size: u32) -> &mut Self {
        self.plan.limit(n, size);
        self
    }

    pub fn repeat<F>(&mut self, times: u32, func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.repeat(times, func);
        self
    }

    pub fn repeat_until<F>(&mut self, times: u32, until: BinaryResource, func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.repeat_until(times, until, func);
        self
    }

    pub fn fork<F>(&mut self, subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.fork(subtask);
        self
    }

    pub fn fork_join<F>(&mut self, subtask: F, joiner: BinaryResource) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.fork_join(joiner, subtask);
        self
    }

    pub fn sink(&mut self, output: BinaryResource) {
        let mut op = new_operator(OpKind::Sink, pipeline());
        op.resource = output;
        self.plan.push(op);
    }

    pub fn build(self) -> Result<pb::JobRequest, BuildJobError> {
        let conf = pb::JobConfig {
            job_id: self.conf.job_id,
            job_name: self.conf.job_name.clone(),
            workers: self.conf.workers,
            servers: self.conf.servers().to_vec(),
        };

        Ok(pb::JobRequest { conf: Some(conf), source: self.source, plan: self.plan.take() })
    }
}

impl Deref for JobBuilder {
    type Target = Plan;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

impl DerefMut for JobBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.plan
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_job_build() {
        let mut builder = JobBuilder::new(JobConf::new(1, "test_build", 2));
        builder
            .add_source(vec![0u8; 32])
            .map(vec![1u8; 32])
            .exchange(vec![2u8; 32])
            .map(vec![3u8; 32])
            .limit(Range::Global, 1)
            .repeat(3, |start| {
                start.exchange(vec![4u8; 32]).map(vec![5u8; 32]);
            })
            .sink(vec![6u8; 32]);
        let job_req = builder.build().unwrap();
        assert_eq!(&job_req.source, &vec![0u8; 32]);
        assert_eq!(
            &job_req.conf,
            &Some(pb::JobConfig {
                job_id: 1,
                job_name: "test_build".to_owned(),
                workers: 2,
                servers: vec![]
            })
        );
        // exchange.map is merge as one map;
        assert_eq!(job_req.plan.len(), 5);
    }
}
