//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::convert::TryInto;
use std::fmt;
use std::ops::{Deref, DerefMut};

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use pegasus::{BuildJobError, JobConf, ServerConf};
use pegasus_server::pb as pegasus_pb;
use prost::Message;

#[derive(Clone, Debug, PartialEq)]
pub struct Plan {
    plan: Vec<pb::PhysicalOpr>,
}

impl Default for Plan {
    fn default() -> Self {
        Plan { plan: vec![] }
    }
}

impl Deref for Plan {
    type Target = Vec<pb::PhysicalOpr>;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

impl DerefMut for Plan {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.plan
    }
}

impl Plan {
    pub fn add_dummy_source(&mut self) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Root(pb::RootScan {});
        self.plan.push(op.into());
        self
    }

    pub fn add_scan_source(&mut self, scan: pb::Scan) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Scan(scan);
        self.plan.push(op.into());
        self
    }

    pub fn repartition(&mut self, repartition: pb::Repartition) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Repartition(repartition);
        self.plan.push(op.into());
        self
    }

    pub fn shuffle(&mut self, shuffle_key: Option<KeyId>) -> &mut Self {
        let shuffle = pb::repartition::Shuffle { shuffle_key };
        let repartition = pb::Repartition { strategy: Some(pb::repartition::Strategy::ToAnother(shuffle)) };
        self.repartition(repartition)
    }

    pub fn broadcast(&mut self) -> &mut Self {
        let repartition = pb::Repartition {
            strategy: Some(pb::repartition::Strategy::ToOthers(pb::repartition::Broadcast {})),
        };
        self.repartition(repartition)
    }

    pub fn project(&mut self, project: pb::Project) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Project(project);
        self.plan.push(op.into());
        self
    }

    pub fn select(&mut self, select: algebra_pb::Select) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Select(select);
        self.plan.push(op.into());
        self
    }

    pub fn group(&mut self, group: pb::GroupBy) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::GroupBy(group);
        self.plan.push(op.into());
        self
    }

    pub fn order(&mut self, order: algebra_pb::OrderBy) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::OrderBy(order);
        self.plan.push(op.into());
        self
    }

    pub fn dedup(&mut self, dedup: algebra_pb::Dedup) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Dedup(dedup);
        self.plan.push(op.into());
        self
    }

    pub fn unfold(&mut self, unfold: pb::Unfold) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Unfold(unfold);
        self.plan.push(op.into());
        self
    }

    pub fn limit(&mut self, limit: algebra_pb::Limit) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Limit(limit);
        self.plan.push(op.into());
        self
    }

    pub fn apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, alias: Option<i32>,
    ) -> &mut Self {
        let apply = pb::Apply {
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            keys: vec![],
            sub_plan: Some(pb::PhysicalPlan { plan: sub_plan.take() }),
            alias,
        };
        let op = pb::physical_opr::operator::OpKind::Apply(apply);
        self.plan.push(op.into());
        self
    }

    pub fn seg_apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, keys: Vec<common_pb::Variable>,
        alias: Option<i32>,
    ) -> &mut Self {
        let apply = pb::Apply {
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            keys,
            sub_plan: Some(pb::PhysicalPlan { plan: sub_plan.take() }),
            alias,
        };
        let op = pb::physical_opr::operator::OpKind::Apply(apply);
        self.plan.push(op.into());
        self
    }

    pub fn join(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_plan: Plan, right_plan: Plan,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self {
        let join = pb::Join {
            left_keys,
            right_keys,
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            left_plan: Some(pb::PhysicalPlan { plan: left_plan.take() }),
            right_plan: Some(pb::PhysicalPlan { plan: right_plan.take() }),
        };
        let op = pb::physical_opr::operator::OpKind::Join(join);
        self.plan.push(op.into());
        self
    }

    pub fn union(&mut self, mut plans: Vec<Plan>) -> &mut Self {
        let mut sub_plans = vec![];
        for plan in plans.drain(..) {
            sub_plans.push(pb::PhysicalPlan { plan: plan.take() });
        }
        let union = pb::Union { sub_plans };
        let op = pb::physical_opr::operator::OpKind::Union(union);
        self.plan.push(op.into());
        self
    }

    pub fn intersect(&mut self, mut plans: Vec<Plan>, key: i32) -> &mut Self {
        let mut sub_plans = vec![];
        for plan in plans.drain(..) {
            sub_plans.push(pb::PhysicalPlan { plan: plan.take() });
        }
        let intersect = pb::Intersect { sub_plans, key };
        let op = pb::physical_opr::operator::OpKind::Intersect(intersect);
        self.plan.push(op.into());
        self
    }

    pub fn get_v(&mut self, get_v: pb::GetV) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Vertex(get_v);
        self.plan.push(op.into());
        self
    }

    pub fn edge_expand(&mut self, edge: pb::EdgeExpand) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Edge(edge);
        self.plan.push(op.into());
        self
    }

    pub fn path_expand(&mut self, path: pb::PathExpand) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Path(path);
        self.plan.push(op.into());
        self
    }

    // Notice that this is used to set the meta_data of the **Last Appended OP**
    pub fn with_meta_data(&mut self, meta_data: Vec<pb::physical_opr::MetaData>) {
        if let Some(op) = self.plan.last_mut() {
            op.meta_data = meta_data;
        }
    }

    pub fn sink(&mut self, sink: pb::Sink) {
        let op = pb::physical_opr::operator::OpKind::Sink(sink);
        self.plan.push(op.into());
    }

    pub fn take(self) -> Vec<pb::PhysicalOpr> {
        self.plan
    }
}

#[derive(Default)]
pub struct JobBuilder {
    pub conf: JobConf,
    plan: Plan,
}

impl fmt::Debug for JobBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobBuilder")
            .field("plan", &self.plan)
            .finish()
    }
}

impl PartialEq for JobBuilder {
    fn eq(&self, other: &JobBuilder) -> bool {
        self.plan == other.plan
    }
}

impl JobBuilder {
    pub fn new(conf: JobConf) -> Self {
        JobBuilder { conf, plan: Default::default() }
    }

    /// Dummy node as the root, when the data come from multiple sources actually.
    pub fn add_dummy_source(&mut self) -> &mut Self {
        self.plan.add_dummy_source();
        self
    }

    /// Scan as the source, when the data come from the scan operator.
    /// If the plan is single source, scan would be the root op;
    /// Otherwise, the root is the dummy node, while the real sources are multiple scans.
    pub fn add_scan_source(&mut self, mut scan: algebra_pb::Scan) -> &mut Self {
        let meta_data = scan.meta_data.take();
        self.plan
            .add_scan_source(scan.into())
            .with_meta_data(
                meta_data
                    .map(|meta| vec![meta.into()])
                    .unwrap_or(vec![]),
            );
        self
    }

    pub fn repartition(&mut self, route: pb::Repartition) -> &mut Self {
        self.plan.repartition(route);
        self
    }

    pub fn shuffle(&mut self, shuffle_key: Option<common_pb::NameOrId>) -> &mut Self {
        let shuffle_key = shuffle_key.map(|tag| tag.try_into().unwrap());
        self.plan.shuffle(shuffle_key);
        self
    }

    pub fn broadcast(&mut self) -> &mut Self {
        self.plan.broadcast();
        self
    }

    pub fn project(&mut self, project: algebra_pb::Project) -> &mut Self {
        let meta_data = project.meta_data.clone();
        self.plan
            .project(project.into())
            .with_meta_data(
                meta_data
                    .into_iter()
                    .map(|meta| meta.into())
                    .collect(),
            );
        self
    }

    pub fn select(&mut self, select: algebra_pb::Select) -> &mut Self {
        self.plan.select(select);
        self
    }

    pub fn group(&mut self, group: algebra_pb::GroupBy) -> &mut Self {
        let meta_data = group.meta_data.clone();
        self.plan.group(group.into()).with_meta_data(
            meta_data
                .into_iter()
                .map(|meta| meta.into())
                .collect(),
        );
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

    pub fn unfold(&mut self, mut unfold: algebra_pb::Unfold) -> &mut Self {
        let meta_data = unfold.meta_data.take();
        self.plan.unfold(unfold.into()).with_meta_data(
            meta_data
                .map(|meta| vec![meta.into()])
                .unwrap_or(vec![]),
        );
        self
    }

    pub fn limit(&mut self, limit: algebra_pb::Limit) -> &mut Self {
        self.plan.limit(limit);
        self
    }

    pub fn apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self {
        let alias = alias.map(|tag| tag.try_into().unwrap());
        self.plan.apply(join_kind, sub_plan, alias);
        self
    }

    pub fn apply_func<F>(
        &mut self, join_kind: algebra_pb::join::JoinKind, mut subtask: F,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let alias = alias.map(|tag| tag.try_into().unwrap());
        self.apply(join_kind, sub_plan, alias)
    }

    pub fn seg_apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: Plan, keys: Vec<common_pb::Variable>,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self {
        let alias = alias.map(|tag| tag.try_into().unwrap());
        self.plan
            .seg_apply(join_kind, sub_plan, keys, alias);
        self
    }

    pub fn seg_apply_func<F>(
        &mut self, join_kind: algebra_pb::join::JoinKind, mut subtask: F, keys: Vec<common_pb::Variable>,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self
    where
        F: FnMut(&mut Plan),
    {
        let mut sub_plan = Plan::default();
        subtask(&mut sub_plan);
        let alias = alias.map(|tag| tag.try_into().unwrap());
        self.plan
            .seg_apply(join_kind, sub_plan, keys, alias);
        self
    }

    pub fn join(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_plan: Plan, right_plan: Plan,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self {
        self.plan
            .join(join_kind, left_plan, right_plan, left_keys, right_keys);
        self
    }

    pub fn join_func<FL, FR>(
        &mut self, join_kind: algebra_pb::join::JoinKind, mut left_task: FL, mut right_task: FR,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self
    where
        FL: FnMut(&mut Plan),
        FR: FnMut(&mut Plan),
    {
        let mut left_plan = Plan::default();
        left_task(&mut left_plan);
        let mut right_plan = Plan::default();
        right_task(&mut right_plan);
        self.join(join_kind, left_plan, right_plan, left_keys, right_keys)
    }

    pub fn union(&mut self, plans: Vec<Plan>) -> &mut Self {
        self.plan.union(plans);
        self
    }

    pub fn intersect(&mut self, plans: Vec<Plan>, key: common_pb::NameOrId) -> &mut Self {
        let key = key.try_into().unwrap();
        self.plan.intersect(plans, key);
        self
    }

    pub fn get_v(&mut self, mut get_v: algebra_pb::GetV) -> &mut Self {
        let meta_data = get_v.meta_data.take();
        self.plan.get_v(get_v.into()).with_meta_data(
            meta_data
                .map(|meta| vec![meta.into()])
                .unwrap_or(vec![]),
        );
        self
    }

    pub fn edge_expand(&mut self, mut edge: algebra_pb::EdgeExpand) -> &mut Self {
        let meta_data = edge.meta_data.take();
        self.plan
            .edge_expand(edge.into())
            .with_meta_data(
                meta_data
                    .map(|meta| vec![meta.into()])
                    .unwrap_or(vec![]),
            );
        self
    }

    pub fn path_expand(&mut self, path: algebra_pb::PathExpand) -> &mut Self {
        self.plan.path_expand(path.into());
        self
    }

    pub fn sink(&mut self, sink: algebra_pb::Sink) {
        self.plan.sink(sink.into());
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

        let plan = pb::PhysicalPlan { plan: self.plan.take() };

        Ok(pegasus_pb::JobRequest {
            conf: Some(conf),
            source: vec![],
            plan: plan.encode_to_vec(),
            resource: vec![],
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_job_build_00() {
        let mut builder = JobBuilder::new(JobConf::new("test_build_00"));
        let source_pb = algebra_pb::Scan {
            scan_opt: 0,
            alias: None,
            params: None,
            idx_predicate: None,
            meta_data: None,
        };
        let sink_pb = algebra_pb::Sink { tags: vec![], sink_target: None };
        builder
            .add_scan_source(source_pb.clone())
            .select(algebra_pb::Select { predicate: None })
            .repartition(pb::Repartition { strategy: None })
            .project(algebra_pb::Project { mappings: vec![], is_append: false, meta_data: vec![] })
            .limit(algebra_pb::Limit { range: None })
            .sink(sink_pb.clone());
        let plan_len = builder.plan.len();
        // source, select, repartition, project, limit, sink
        assert_eq!(&plan_len, &6);
    }

    #[test]
    fn test_job_build_01() {
        let mut builder = JobBuilder::new(JobConf::new("test_build_01"));
        let scan1_pb = algebra_pb::Scan {
            scan_opt: 0,
            alias: None,
            params: None,
            idx_predicate: None,
            meta_data: None,
        };
        let scan2_pb = scan1_pb.clone();
        let project_pb = algebra_pb::Project { mappings: vec![], is_append: false, meta_data: vec![] };
        let sink_pb = algebra_pb::Sink { tags: vec![], sink_target: None };

        builder
            .add_dummy_source()
            .join_func(
                algebra_pb::join::JoinKind::Inner,
                |src1| {
                    src1.add_scan_source(scan1_pb.clone().into());
                },
                |src2| {
                    src2.add_scan_source(scan2_pb.clone().into())
                        .project(project_pb.clone().into());
                },
                vec![],
                vec![],
            )
            .sink(sink_pb.clone());
        let plan = builder.plan.clone();
        let plan_len = plan.len();
        // dummy_source, join, sink
        assert_eq!(&plan_len, &3);

        let join_op = plan.plan[1].clone();
        if let Some(pb::physical_opr::operator::OpKind::Join(join)) = join_op.opr.unwrap().op_kind {
            // scan
            assert_eq!(join.left_plan.unwrap().plan.len(), 1);
            // scan, project
            assert_eq!(join.right_plan.unwrap().plan.len(), 2);
        } else {
            assert!(false)
        }

        // another way to build `Join`
        let mut left_builder = JobBuilder::default();
        let mut right_builder = JobBuilder::default();
        left_builder.add_scan_source(scan1_pb);
        right_builder
            .add_scan_source(scan2_pb)
            .project(project_pb);
        let mut builder2 = JobBuilder::default();
        builder2
            .add_dummy_source()
            .join(
                algebra_pb::join::JoinKind::Inner,
                left_builder.take_plan(),
                right_builder.take_plan(),
                vec![],
                vec![],
            )
            .sink(sink_pb);

        assert_eq!(plan, builder2.take_plan())
    }
}
