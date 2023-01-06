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

use std::fmt;
use std::ops::{Deref, DerefMut};

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use pegasus::{BuildJobError, JobConf, ServerConf};
//use pegasus_server::job_pb as pb;
use pegasus_server::pb as pegasus_pb;
use prost::Message;

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    plan: Vec<algebra_pb::PhysicalOpr>,
}

impl Default for Plan {
    fn default() -> Self {
        Plan { plan: vec![] }
    }
}

impl Deref for Plan {
    type Target = Vec<algebra_pb::PhysicalOpr>;

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
    pub fn repartition(&mut self, repartition: algebra_pb::Repartition) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Repartition(repartition);
        self.plan.push(op.into());
        self
    }

    pub fn shuffle(&mut self, shuffle_key: Option<i32>) -> &mut Self {
        let shuffle = algebra_pb::repartition::Shuffle { shuffle_key };
        let repartition = algebra_pb::Repartition {
            strategy: Some(algebra_pb::repartition::Strategy::ToAnother(shuffle)),
        };
        self.repartition(repartition)
    }

    pub fn broadcast(&mut self) -> &mut Self {
        let repartition = algebra_pb::Repartition {
            strategy: Some(algebra_pb::repartition::Strategy::ToOthers(
                algebra_pb::repartition::Broadcast {},
            )),
        };
        self.repartition(repartition)
    }

    pub fn scan(&mut self, scan: algebra_pb::Scan) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Scan(scan);
        self.plan.push(op.into());
        self
    }

    pub fn project(&mut self, project: algebra_pb::Project) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Project(project);
        self.plan.push(op.into());
        self
    }

    pub fn select(&mut self, select: algebra_pb::Select) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Select(select);
        self.plan.push(op.into());
        self
    }

    pub fn group(&mut self, group: algebra_pb::GroupBy) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::GroupBy(group);
        self.plan.push(op.into());
        self
    }

    pub fn order(&mut self, order: algebra_pb::OrderBy) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::OrderBy(order);
        self.plan.push(op.into());
        self
    }

    pub fn dedup(&mut self, dedup: algebra_pb::Dedup) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Dedup(dedup);
        self.plan.push(op.into());
        self
    }

    pub fn unfold(&mut self, unfold: algebra_pb::Unfold) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Unfold(unfold);
        self.plan.push(op.into());
        self
    }

    pub fn limit(&mut self, limit: algebra_pb::Limit) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Limit(limit);
        self.plan.push(op.into());
        self
    }

    // pub fn apply(&mut self, apply: algebra_pb::PhysicalApply) -> &mut Self {
    //     let op = algebra_pb::physical_opr::operator::OpKind::Apply(apply);
    //     self.plan.push(op.into());
    //     self
    // }

    pub fn apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, alias: Option<i32>,
    ) -> &mut Self {
        let apply = algebra_pb::PhysicalApply {
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            keys: vec![],
            sub_plan: Some(algebra_pb::PhysicalPlan { plan: sub_plan.take() }),
            alias,
        };
        let op = algebra_pb::physical_opr::operator::OpKind::Apply(apply);
        self.plan.push(op.into());
        self
    }

    pub fn seg_apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, keys: Vec<common_pb::Variable>,
        alias: Option<i32>,
    ) -> &mut Self {
        let apply = algebra_pb::PhysicalApply {
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            keys,
            sub_plan: Some(algebra_pb::PhysicalPlan { plan: sub_plan.take() }),
            alias,
        };
        let op = algebra_pb::physical_opr::operator::OpKind::Apply(apply);
        self.plan.push(op.into());
        self
    }

    // // TODO: like union?
    // pub fn join(&mut self, join: algebra_pb::PhysicalJoin) -> &mut Self {
    //     let op = algebra_pb::physical_opr::operator::OpKind::Join(join);
    //     self.plan.push(op.into());
    //     self
    // }

    pub fn join(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_plan: Plan, right_plan: Plan,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self {
        let join = algebra_pb::PhysicalJoin {
            left_keys,
            right_keys,
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            left_plan: Some(algebra_pb::PhysicalPlan { plan: left_plan.take() }),
            right_plan: Some(algebra_pb::PhysicalPlan { plan: right_plan.take() }),
        };
        let op = algebra_pb::physical_opr::operator::OpKind::Join(join);
        self.plan.push(op.into());
        self
    }

    pub fn union(&mut self, mut plans: Vec<Plan>) -> &mut Self {
        let mut sub_plans = vec![];
        for plan in plans.drain(..) {
            sub_plans.push(algebra_pb::PhysicalPlan { plan: plan.take() });
        }
        let union = algebra_pb::PhysicalUnion { sub_plans };
        let op = algebra_pb::physical_opr::operator::OpKind::Union(union);
        self.plan.push(op.into());
        self
    }

    pub fn intersect(&mut self, mut plans: Vec<Plan>, key: i32) -> &mut Self {
        let mut sub_plans = vec![];
        for plan in plans.drain(..) {
            sub_plans.push(algebra_pb::PhysicalPlan { plan: plan.take() });
        }
        let intersect = algebra_pb::PhysicalIntersect { sub_plans, key };
        let op = algebra_pb::physical_opr::operator::OpKind::Intersect(intersect);
        self.plan.push(op.into());
        self
    }

    pub fn get_v(&mut self, get_v: algebra_pb::GetV) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Vertex(get_v);
        self.plan.push(op.into());
        self
    }

    pub fn edge_expand(&mut self, edge: algebra_pb::EdgeExpand) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Edge(edge);
        self.plan.push(op.into());
        self
    }

    pub fn path_expand(&mut self, path: algebra_pb::PathExpand) -> &mut Self {
        let op = algebra_pb::physical_opr::operator::OpKind::Path(path);
        self.plan.push(op.into());
        self
    }

    pub fn take(self) -> Vec<algebra_pb::PhysicalOpr> {
        self.plan
    }
}

#[derive(Default)]
pub struct JobBuilder {
    pub conf: JobConf,
    source: BinaryResource,
    plan: Plan,
    sink: BinaryResource,
}

impl fmt::Debug for JobBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobBuilder")
            .field("source", &self.source)
            .field("plan", &self.plan)
            .field("sink", &self.sink)
            .finish()
    }
}

impl PartialEq for JobBuilder {
    fn eq(&self, other: &JobBuilder) -> bool {
        self.source == other.source && self.plan == other.plan && self.sink == other.sink
    }
}

impl JobBuilder {
    pub fn new(conf: JobConf) -> Self {
        JobBuilder { conf, source: vec![], plan: Default::default(), sink: vec![] }
    }

    pub fn add_dummy_source(&mut self) -> &mut Self {
        self.source = vec![];
        self
    }

    pub fn add_scan_source(&mut self, scan: algebra_pb::Scan) -> &mut Self {
        let src = scan.encode_to_vec();
        self.source = src;
        self
    }

    pub fn repartition(&mut self, route: algebra_pb::Repartition) -> &mut Self {
        self.plan.repartition(route);
        self
    }

    pub fn shuffle(&mut self, shuffle_key: Option<i32>) -> &mut Self {
        self.plan.shuffle(shuffle_key);
        self
    }

    pub fn broadcast(&mut self) -> &mut Self {
        self.plan.broadcast();
        self
    }

    pub fn scan(&mut self, scan: algebra_pb::Scan) -> &mut Self {
        self.plan.scan(scan);
        self
    }

    pub fn project(&mut self, project: algebra_pb::Project) -> &mut Self {
        self.plan.project(project);
        self
    }

    pub fn select(&mut self, select: algebra_pb::Select) -> &mut Self {
        self.plan.select(select);
        self
    }

    pub fn group(&mut self, group: algebra_pb::GroupBy) -> &mut Self {
        self.plan.group(group);
        self
    }

    pub fn order(&mut self, order: algebra_pb::OrderBy) -> &mut Self {
        self.plan.order(order);
        self
    }

    pub fn dedup(&mut self, dedup: algebra_pb::Dedup) -> &mut Self {
        self.plan.dedup(dedup);
        self
    }

    pub fn unfold(&mut self, unfold: algebra_pb::Unfold) -> &mut Self {
        self.plan.unfold(unfold);
        self
    }

    pub fn limit(&mut self, limit: algebra_pb::Limit) -> &mut Self {
        self.plan.limit(limit);
        self
    }

    pub fn apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, alias: Option<i32>,
    ) -> &mut Self {
        self.plan.apply(join_kind, sub_plan, alias);
        self
    }

    pub fn seg_apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, keys: Vec<common_pb::Variable>,
        alias: Option<i32>,
    ) -> &mut Self {
        self.plan
            .seg_apply(join_kind, sub_plan, keys, alias);
        self
    }

    // pub fn join(&mut self, join: algebra_pb::PhysicalJoin) -> &mut Self {
    //     self.plan.join(join);
    //     self
    // }

    pub fn join(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_plan: Plan, right_plan: Plan,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self {
        self.plan
            .join(join_kind, left_plan, right_plan, left_keys, right_keys);
        self
    }

    pub fn union(&mut self, plans: Vec<Plan>) -> &mut Self {
        self.plan.union(plans);
        self
    }

    pub fn intersect(&mut self, plans: Vec<Plan>, key: i32) -> &mut Self {
        self.plan.intersect(plans, key);
        self
    }

    pub fn get_v(&mut self, get_v: algebra_pb::GetV) -> &mut Self {
        self.plan.get_v(get_v);
        self
    }

    pub fn edge_expand(&mut self, edge: algebra_pb::EdgeExpand) -> &mut Self {
        self.plan.edge_expand(edge);
        self
    }

    pub fn path_expand(&mut self, path: algebra_pb::PathExpand) -> &mut Self {
        self.plan.path_expand(path);
        self
    }

    pub fn sink(&mut self, sink: algebra_pb::Sink) {
        let sink = sink.encode_to_vec();
        self.sink = sink;
    }

    pub fn take_plan(self) -> Plan {
        self.plan
    }

    pub fn build(self) -> Result<pegasus_pb::JobRequest, BuildJobError> {
        let conf = pegasus_pb::JobConfig {
            job_id: self.conf.job_id,
            job_name: self.conf.job_name.clone(),
            workers: self.conf.workers,
            time_limit: self.conf.time_limit,
            batch_size: self.conf.batch_size,
            batch_capacity: self.conf.batch_capacity,
            memory_limit: self.conf.memory_limit,
            trace_enable: self.conf.trace_enable,
            servers: match self.conf.servers() {
                ServerConf::Local => Some(pegasus_pb::job_config::Servers::Local(pegasus_pb::Empty {})),
                ServerConf::Partial(servers) => {
                    Some(pegasus_pb::job_config::Servers::Part(pegasus_pb::ServerList {
                        servers: servers.clone(),
                    }))
                }
                ServerConf::All => Some(pegasus_pb::job_config::Servers::All(pegasus_pb::Empty {})),
            },
        };

        let plan = algebra_pb::PhysicalPlan { plan: self.plan.take() };

        Ok(pegasus_pb::JobRequest {
            conf: Some(conf),
            source: self.source,
            plan: plan.encode_to_vec(),
            resource: self.sink,
        })
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
    fn test_job_build_00() {
        let mut builder = JobBuilder::new(JobConf::new("test_build_00"));
        builder
            .add_scan_source(algebra_pb::Scan {
                scan_opt: 0,
                alias: None,
                params: None,
                idx_predicate: None,
            })
            .select(algebra_pb::Select { predicate: None })
            .repartition(algebra_pb::Repartition { strategy: None })
            .project(algebra_pb::Project { mappings: vec![], is_append: false })
            .limit(algebra_pb::Limit { range: None })
            .sink(algebra_pb::Sink { tags: vec![], sink_target: None });
        let plan_len = builder.plan.len();
        let job_req = builder.build().unwrap();
        let source = algebra_pb::Scan { scan_opt: 0, alias: None, params: None, idx_predicate: None };
        let sink = algebra_pb::Sink { tags: vec![], sink_target: None };
        assert_eq!(&job_req.source, &source.encode_to_vec());
        assert_eq!(&job_req.resource, &sink.encode_to_vec());
        assert_eq!(&plan_len, &4);
    }

    #[test]
    fn test_job_build_01() {
        let mut builder = JobBuilder::new(JobConf::new("test_build_01"));
        builder
            .add_dummy_source()
            .join_func(
                pb::join::JoinKind::Inner,
                |src1| {
                    src1.map(vec![]).join_func(
                        pb::join::JoinKind::Inner,
                        |src1_1| {
                            src1_1.map(vec![]);
                        },
                        |src1_2| {
                            src1_2.map(vec![]);
                        },
                        vec![],
                    );
                },
                |src2| {
                    src2.map(vec![]).join_func(
                        pb::join::JoinKind::Inner,
                        |src2_1| {
                            src2_1.map(vec![]);
                        },
                        |src2_2| {
                            src2_2.map(vec![]);
                        },
                        vec![],
                    );
                },
                vec![],
            )
            .sink(vec![6u8; 32]);
        let plan_len = builder.plan.len();
        let job_req = builder.build().unwrap();
        let source = pb::Source { resource: vec![0u8; 32] };
        let sink = pb::Sink { resource: vec![6u8; 32] };
        assert_eq!(&job_req.source, &source.encode_to_vec());
        assert_eq!(&job_req.resource, &sink.encode_to_vec());
        assert_eq!(&plan_len, &1);
    }
}
