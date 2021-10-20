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

use pegasus::{BuildJobError, JobConf};
use pegasus_server::pb;
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

impl Plan {
    pub fn exchange(&mut self, route: BinaryResource) -> &mut Self {
        let exchange = pb::Exchange { resource: route };
        let comm = pb::Communicate { ch_kind: Some(pb::communicate::ChKind::ToAnother(exchange)) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Comm(comm)) };
        self.plan.push(op);
        self
    }

    pub fn broadcast(&mut self) -> &mut Self {
        let broadcast = pb::Broadcast {};
        let comm = pb::Communicate { ch_kind: Some(pb::communicate::ChKind::ToOthers(broadcast)) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Comm(comm)) };
        self.plan.push(op);
        self
    }

    pub fn aggregate(&mut self, target: u32) -> &mut Self {
        let aggregate = pb::Aggregate { target };
        let comm = pb::Communicate { ch_kind: Some(pb::communicate::ChKind::ToOne(aggregate)) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Comm(comm)) };
        self.plan.push(op);
        self
    }

    pub fn map(&mut self, func: BinaryResource) -> &mut Self {
        let map = pb::Map { resource: func };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Map(map)) };
        self.plan.push(op);
        self
    }

    pub fn flat_map(&mut self, func: BinaryResource) -> &mut Self {
        let flat_map = pb::FlatMap { resource: func };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::FlatMap(flat_map)) };
        self.plan.push(op);
        self
    }

    pub fn filter(&mut self, func: BinaryResource) -> &mut Self {
        let filter = pb::Filter { resource: func };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Filter(filter)) };
        self.plan.push(op);
        self
    }

    pub fn limit(&mut self, size: u32) -> &mut Self {
        let limit = pb::Limit { limit: size };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Limit(limit)) };
        self.plan.push(op);
        self
    }

    pub fn count(&mut self) -> &mut Self {
        let fold = pb::Fold { accum: pb::AccumKind::Cnt as i32, resource: vec![] };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Fold(fold)) };
        self.plan.push(op);
        self
    }

    pub fn fold(&mut self, accum_kind: pb::AccumKind) -> &mut Self {
        let fold = pb::Fold { accum: accum_kind as i32, resource: vec![] };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Fold(fold)) };
        self.plan.push(op);
        self
    }

    pub fn fold_custom(&mut self, accum_kind: pb::AccumKind, func: BinaryResource) -> &mut Self {
        let fold = pb::Fold { accum: accum_kind as i32, resource: func };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Fold(fold)) };
        self.plan.push(op);
        self
    }

    pub fn dedup(&mut self) -> &mut Self {
        let dedup = pb::Dedup {};
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Dedup(dedup)) };
        self.plan.push(op);
        self
    }

    pub fn iterate<F>(&mut self, times: u32, mut func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        func(&mut sub_plan);
        let iteration = pb::Iteration {
            max_iters: times,
            until: None,
            body: Some(pb::TaskPlan { plan: sub_plan.take() }),
        };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Iterate(iteration)) };
        self.plan.push(op);
        self
    }

    pub fn iterate_until<F>(&mut self, times: u32, until: BinaryResource, mut func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        func(&mut sub_plan);
        let filter = pb::Filter { resource: until };
        let iteration = pb::Iteration {
            max_iters: times,
            until: Some(filter),
            body: Some(pb::TaskPlan { plan: sub_plan.take() }),
        };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Iterate(iteration)) };
        self.plan.push(op);
        self
    }

    pub fn apply<F>(&mut self, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let subtask = pb::Apply { join: None, task: Some(pb::TaskPlan { plan: sub_plan.take() }) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Apply(subtask)) };
        self.plan.push(op);
        self
    }

    pub fn apply_join<F>(&mut self, joiner: BinaryResource, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let left_join = pb::LeftJoin { resource: joiner };
        let subtask =
            pb::Apply { join: Some(left_join), task: Some(pb::TaskPlan { plan: sub_plan.take() }) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Apply(subtask)) };
        self.plan.push(op);
        self
    }

    pub fn segment_apply<F>(&mut self, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let subtask = pb::SegmentApply { join: None, task: Some(pb::TaskPlan { plan: sub_plan.take() }) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::SegApply(subtask)) };
        self.plan.push(op);
        self
    }

    pub fn segment_apply_join<F>(&mut self, joiner: BinaryResource, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let left_join = pb::LeftJoin { resource: joiner };
        let subtask =
            pb::SegmentApply { join: Some(left_join), task: Some(pb::TaskPlan { plan: sub_plan.take() }) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::SegApply(subtask)) };
        self.plan.push(op);
        self
    }

    pub fn merge<F>(&mut self, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let merge = pb::Merge { task: Some(pb::TaskPlan { plan: sub_plan.take() }) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Merge(merge)) };
        self.plan.push(op);
        self
    }

    pub fn join<F>(&mut self, join_kind: pb::join::JoinKind, mut subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut join_plan = Plan::default();
        subtask(&mut join_plan);
        let join = pb::Join { kind: join_kind as i32, task: Some(pb::TaskPlan { plan: join_plan.take() }) };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Join(join)) };
        self.plan.push(op);
        self
    }

    pub fn sort_by(&mut self, cmp: BinaryResource) -> &mut Self {
        let sort = pb::SortBy { limit: -1, compare: cmp };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Sort(sort)) };
        self.plan.push(op);
        self
    }

    pub fn sort_limit_by(&mut self, n: i64, cmp: BinaryResource) -> &mut Self {
        let sort = pb::SortBy { limit: n, compare: cmp };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Sort(sort)) };
        self.plan.push(op);
        self
    }

    pub fn group_by(&mut self, accum_kind: pb::AccumKind, key_selector: BinaryResource) -> &mut Self {
        let group = pb::GroupBy { accum: accum_kind as i32, resource: key_selector };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::Group(group)) };
        self.plan.push(op);
        self
    }

    pub fn key_by(&mut self, key_selector: BinaryResource) -> &mut Self {
        let key_by = pb::KeyBy { key_selector };
        let op = pb::OperatorDef { op_kind: Some(pb::operator_def::OpKind::KeyBy(key_by)) };
        self.plan.push(op);
        self
    }

    pub fn take(self) -> Vec<pb::OperatorDef> {
        self.plan
    }
}

pub struct JobBuilder {
    conf: JobConf,
    source: BinaryResource,
    plan: Plan,
    sink: BinaryResource,
}

impl JobBuilder {
    pub fn new(conf: JobConf) -> Self {
        JobBuilder { conf, source: vec![], plan: Default::default(), sink: vec![] }
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

    pub fn limit(&mut self, size: u32) -> &mut Self {
        self.plan.limit(size);
        self
    }

    pub fn dedup(&mut self) -> &mut Self {
        self.plan.dedup();
        self
    }

    pub fn iterate<F>(&mut self, times: u32, func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.iterate(times, func);
        self
    }

    pub fn iterate_until<F>(&mut self, times: u32, until: BinaryResource, func: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.iterate_until(times, until, func);
        self
    }

    pub fn apply<F>(&mut self, subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.apply(subtask);
        self
    }

    pub fn apply_join<F>(&mut self, subtask: F, joiner: BinaryResource) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.apply_join(joiner, subtask);
        self
    }

    pub fn segment_apply<F>(&mut self, subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.segment_apply(subtask);
        self
    }

    pub fn segment_apply_join<F>(&mut self, subtask: F, joiner: BinaryResource) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.segment_apply_join(joiner, subtask);
        self
    }

    pub fn merge<F>(&mut self, subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.merge(subtask);
        self
    }

    pub fn join<F>(&mut self, join_kind: pb::join::JoinKind, subtask: F) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        self.plan.join(join_kind, subtask);
        self
    }

    pub fn sort_by(&mut self, cmp: BinaryResource) -> &mut Self {
        self.plan.sort_by(cmp);
        self
    }

    pub fn sort_limit_by(&mut self, n: i64, cmp: BinaryResource) -> &mut Self {
        self.plan.sort_limit_by(n, cmp);
        self
    }

    pub fn count(&mut self) -> &mut Self {
        self.plan.count();
        self
    }

    pub fn fold(&mut self, accum_kind: pb::AccumKind) -> &mut Self {
        self.plan.fold(accum_kind);
        self
    }

    pub fn fold_custom(&mut self, accum_kind: pb::AccumKind, func: BinaryResource) -> &mut Self {
        self.plan.fold_custom(accum_kind, func);
        self
    }

    pub fn key_by(&mut self, key_selector: BinaryResource) -> &mut Self {
        self.plan.key_by(key_selector);
        self
    }

    pub fn sink(&mut self, output: BinaryResource) {
        self.sink = output;
    }

    pub fn build(self) -> Result<pb::JobRequest, BuildJobError> {
        let conf = pb::JobConfig {
            job_id: self.conf.job_id,
            job_name: self.conf.job_name.clone(),
            workers: self.conf.workers,
            time_limit: self.conf.time_limit,
            batch_size: self.conf.batch_size,
            output_capacity: self.conf.batch_capacity,
            memory_limit: self.conf.memory_limit,
            plan_print: self.conf.plan_print,
            servers: self.conf.servers().get_servers(),
        };
        let source = pb::Source { resource: self.source };
        let plan = pb::TaskPlan { plan: self.plan.take() };
        let sink = pb::Sink { resource: self.sink };

        Ok(pb::JobRequest { conf: Some(conf), source: Some(source), plan: Some(plan), sink: Some(sink) })
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
        let mut builder = JobBuilder::new(JobConf::new("test_build"));
        builder
            .add_source(vec![0u8; 32])
            .map(vec![1u8; 32])
            .exchange(vec![2u8; 32])
            .map(vec![3u8; 32])
            .limit(1)
            .iterate(3, |start| {
                start.exchange(vec![4u8; 32]).map(vec![5u8; 32]);
            })
            .sink(vec![6u8; 32]);
        let job_req = builder.build().unwrap();
        assert_eq!(&job_req.source.unwrap().resource, &vec![0u8; 32]);
        assert_eq!(&job_req.sink.unwrap().resource, &vec![6u8; 32]);
        assert_eq!(&job_req.plan.unwrap().plan.len(), &5);
    }
}
