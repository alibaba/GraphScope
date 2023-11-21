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
use pegasus::{BuildJobError, JobConf, ServerConf};
use pegasus_server::pb as pegasus_pb;
use prost::Message;

const DEFAULT_PLAN_ID: i32 = i32::MAX;

/// A plan builder used to build a GIE physical JobRequest.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanBuilder {
    id: i32,
    plan: Vec<pb::PhysicalOpr>,
}

impl Default for PlanBuilder {
    fn default() -> Self {
        PlanBuilder { id: DEFAULT_PLAN_ID, plan: vec![] }
    }
}

impl Deref for PlanBuilder {
    type Target = Vec<pb::PhysicalOpr>;

    fn deref(&self) -> &Self::Target {
        &self.plan
    }
}

impl DerefMut for PlanBuilder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.plan
    }
}

impl PlanBuilder {
    pub fn new(plan_id: i32) -> Self {
        PlanBuilder { id: plan_id, plan: vec![] }
    }

    pub fn add_dummy_source(&mut self) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Root(pb::Root {});
        self.plan.push(op.into());
        self
    }

    /// Scan as the source, when the data come from the scan operator.
    /// If the plan is single source, scan would be the root op;
    /// Otherwise, the root is the dummy node, while the real sources are multiple scans.
    pub fn add_scan_source(&mut self, mut scan: algebra_pb::Scan) -> &mut Self {
        let meta_data = scan
            .meta_data
            .take()
            .map(|meta| vec![meta.into()])
            .unwrap_or_default();
        let op = pb::physical_opr::operator::OpKind::Scan(scan.into());
        self.plan.push((op, meta_data).into());
        self
    }

    pub fn repartition(&mut self, repartition: pb::Repartition) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Repartition(repartition);
        self.plan.push(op.into());
        self
    }

    pub fn shuffle(&mut self, shuffle_key: Option<common_pb::NameOrId>) -> &mut Self {
        let shuffle_key = shuffle_key.map(|tag| tag.try_into().unwrap());
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

    pub fn project(&mut self, project: algebra_pb::Project) -> &mut Self {
        let meta_data = project
            .meta_data
            .clone()
            .into_iter()
            .map(|meta| meta.into())
            .collect();
        let op = pb::physical_opr::operator::OpKind::Project(project.into());
        self.plan.push((op, meta_data).into());
        self
    }

    pub fn select(&mut self, select: algebra_pb::Select) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Select(select);
        self.plan.push(op.into());
        self
    }

    pub fn group(&mut self, group: algebra_pb::GroupBy) -> &mut Self {
        let meta_data = group
            .meta_data
            .clone()
            .into_iter()
            .map(|meta| meta.into())
            .collect();
        let op = pb::physical_opr::operator::OpKind::GroupBy(group.into());
        self.plan.push((op, meta_data).into());
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

    pub fn unfold(&mut self, mut unfold: algebra_pb::Unfold) -> &mut Self {
        let meta_data = unfold
            .meta_data
            .take()
            .map(|meta| vec![meta.into()])
            .unwrap_or_default();
        let op = pb::physical_opr::operator::OpKind::Unfold(unfold.into());
        self.plan.push((op, meta_data).into());
        self
    }

    pub fn limit(&mut self, limit: algebra_pb::Limit) -> &mut Self {
        let op = pb::physical_opr::operator::OpKind::Limit(limit);
        self.plan.push(op.into());
        self
    }

    pub fn apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: PlanBuilder,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self {
        let alias = alias.map(|tag| tag.try_into().unwrap());
        let apply = pb::Apply {
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            keys: vec![],
            sub_plan: Some(pb::PhysicalPlan { plan: sub_plan.take(), plan_id: DEFAULT_PLAN_ID }),
            alias,
        };
        let op = pb::physical_opr::operator::OpKind::Apply(apply);
        self.plan.push(op.into());
        self
    }

    pub fn apply_func<F>(
        &mut self, join_kind: algebra_pb::join::JoinKind, mut subtask: F,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self
    where
        F: FnMut(&mut PlanBuilder),
    {
        let mut sub_plan = PlanBuilder::default();
        subtask(&mut sub_plan);
        self.apply(join_kind, sub_plan, alias)
    }

    pub fn seg_apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: PlanBuilder,
        keys: Vec<common_pb::Variable>, alias: Option<common_pb::NameOrId>,
    ) -> &mut Self {
        let alias = alias.map(|tag| tag.try_into().unwrap());
        let apply = pb::Apply {
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            keys,
            sub_plan: Some(pb::PhysicalPlan { plan: sub_plan.take(), plan_id: DEFAULT_PLAN_ID }),
            alias,
        };
        let op = pb::physical_opr::operator::OpKind::Apply(apply);
        self.plan.push(op.into());
        self
    }

    pub fn seg_apply_func<F>(
        &mut self, join_kind: algebra_pb::join::JoinKind, mut subtask: F, keys: Vec<common_pb::Variable>,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self
    where
        F: FnMut(&mut PlanBuilder),
    {
        let mut sub_plan = PlanBuilder::default();
        subtask(&mut sub_plan);
        self.seg_apply(join_kind, sub_plan, keys, alias)
    }

    pub fn join(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_plan: PlanBuilder, right_plan: PlanBuilder,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self {
        let join = pb::Join {
            left_keys,
            right_keys,
            join_kind: unsafe { ::std::mem::transmute(join_kind) },
            left_plan: Some(pb::PhysicalPlan { plan: left_plan.take(), plan_id: DEFAULT_PLAN_ID }),
            right_plan: Some(pb::PhysicalPlan { plan: right_plan.take(), plan_id: DEFAULT_PLAN_ID }),
        };
        let op = pb::physical_opr::operator::OpKind::Join(join);
        self.plan.push(op.into());
        self
    }

    pub fn join_func<FL, FR>(
        &mut self, join_kind: algebra_pb::join::JoinKind, mut left_task: FL, mut right_task: FR,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self
    where
        FL: FnMut(&mut PlanBuilder),
        FR: FnMut(&mut PlanBuilder),
    {
        let mut left_plan = PlanBuilder::default();
        left_task(&mut left_plan);
        let mut right_plan = PlanBuilder::default();
        right_task(&mut right_plan);
        self.join(join_kind, left_plan, right_plan, left_keys, right_keys)
    }

    pub fn union(&mut self, mut plans: Vec<PlanBuilder>) -> &mut Self {
        let mut sub_plans = vec![];
        for plan in plans.drain(..) {
            sub_plans.push(pb::PhysicalPlan { plan: plan.take(), plan_id: DEFAULT_PLAN_ID });
        }
        let union = pb::Union { sub_plans };
        let op = pb::physical_opr::operator::OpKind::Union(union);
        self.plan.push(op.into());
        self
    }

    pub fn intersect(&mut self, mut plans: Vec<PlanBuilder>, key: common_pb::NameOrId) -> &mut Self {
        let key = key.try_into().unwrap();
        let mut sub_plans = vec![];
        for plan in plans.drain(..) {
            sub_plans.push(pb::PhysicalPlan { plan: plan.take(), plan_id: DEFAULT_PLAN_ID });
        }
        let intersect = pb::Intersect { sub_plans, key };
        let op = pb::physical_opr::operator::OpKind::Intersect(intersect);
        self.plan.push(op.into());
        self
    }

    pub fn get_v(&mut self, mut get_v: algebra_pb::GetV) -> &mut Self {
        let meta_data = get_v
            .meta_data
            .take()
            .map(|meta| vec![meta.into()])
            .unwrap_or_default();
        let op = pb::physical_opr::operator::OpKind::Vertex(get_v.into());
        self.plan.push((op, meta_data).into());
        self
    }

    pub fn edge_expand(&mut self, mut edge: algebra_pb::EdgeExpand) -> &mut Self {
        let meta_data = edge
            .meta_data
            .take()
            .map(|meta| vec![meta.into()])
            .unwrap_or_default();
        let op = pb::physical_opr::operator::OpKind::Edge(edge.into());
        self.plan.push((op, meta_data).into());
        self
    }

    pub fn path_expand(&mut self, path: algebra_pb::PathExpand) -> &mut Self {
        let mut edge_expand = None;
        if let Some(base) = &path.base {
            if let Some(expd) = &base.edge_expand {
                edge_expand = Some(expd);
            }
        }
        // Set the Metadata of PathExpand to the Metadata of EdgeExpand
        let meta_data = if let Some(edge_expand) = edge_expand {
            edge_expand
                .meta_data
                .clone()
                .map(|meta| vec![meta.into()])
                .unwrap_or_default()
        } else {
            vec![]
        };

        let op = pb::physical_opr::operator::OpKind::Path(path.into());
        self.plan.push((op, meta_data).into());
        self
    }

    pub fn sample(&mut self, sample: algebra_pb::Sample) {
        let op = pb::physical_opr::operator::OpKind::Sample(sample);
        self.plan.push(op.into());
    }

    pub fn sink(&mut self, sink: algebra_pb::Sink) {
        let op = pb::physical_opr::operator::OpKind::Sink(sink.into());
        self.plan.push(op.into());
    }

    pub fn take(self) -> Vec<pb::PhysicalOpr> {
        self.plan
    }

    pub fn get_last_op_mut(&mut self) -> Option<&mut pb::PhysicalOpr> {
        self.plan.last_mut()
    }

    pub fn build(self) -> pb::PhysicalPlan {
        pb::PhysicalPlan { plan: self.plan, plan_id: self.id }
    }
}

/// A job builder used to build a Pegasus JobRequest.
#[derive(Default)]
pub struct JobBuilder {
    pub conf: JobConf,
    plan: PlanBuilder,
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

    pub fn with_plan(plan: PlanBuilder) -> Self {
        JobBuilder { conf: JobConf::default(), plan }
    }

    /// Dummy node as the root, when the data come from multiple sources actually.
    pub fn add_dummy_source(&mut self) -> &mut Self {
        self.plan.add_dummy_source();
        self
    }

    /// Scan as the source, when the data come from the scan operator.
    /// If the plan is single source, scan would be the root op;
    /// Otherwise, the root is the dummy node, while the real sources are multiple scans.
    pub fn add_scan_source(&mut self, scan: algebra_pb::Scan) -> &mut Self {
        self.plan.add_scan_source(scan);
        self
    }

    pub fn repartition(&mut self, route: pb::Repartition) -> &mut Self {
        self.plan.repartition(route);
        self
    }

    pub fn shuffle(&mut self, shuffle_key: Option<common_pb::NameOrId>) -> &mut Self {
        self.plan.shuffle(shuffle_key);
        self
    }

    pub fn broadcast(&mut self) -> &mut Self {
        self.plan.broadcast();
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
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: PlanBuilder,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self {
        self.plan.apply(join_kind, sub_plan, alias);
        self
    }

    pub fn apply_func<F>(
        &mut self, join_kind: algebra_pb::join::JoinKind, subtask: F, alias: Option<common_pb::NameOrId>,
    ) -> &mut Self
    where
        F: FnMut(&mut PlanBuilder),
    {
        self.plan.apply_func(join_kind, subtask, alias);
        self
    }

    pub fn seg_apply(
        &mut self, join_kind: algebra_pb::join::JoinKind, sub_plan: PlanBuilder,
        keys: Vec<common_pb::Variable>, alias: Option<common_pb::NameOrId>,
    ) -> &mut Self {
        self.plan
            .seg_apply(join_kind, sub_plan, keys, alias);
        self
    }

    pub fn seg_apply_func<F>(
        &mut self, join_kind: algebra_pb::join::JoinKind, subtask: F, keys: Vec<common_pb::Variable>,
        alias: Option<common_pb::NameOrId>,
    ) -> &mut Self
    where
        F: FnMut(&mut PlanBuilder),
    {
        self.plan
            .seg_apply_func(join_kind, subtask, keys, alias);
        self
    }

    pub fn join(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_plan: PlanBuilder, right_plan: PlanBuilder,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self {
        self.plan
            .join(join_kind, left_plan, right_plan, left_keys, right_keys);
        self
    }

    pub fn join_func<FL, FR>(
        &mut self, join_kind: algebra_pb::join::JoinKind, left_task: FL, right_task: FR,
        left_keys: Vec<common_pb::Variable>, right_keys: Vec<common_pb::Variable>,
    ) -> &mut Self
    where
        FL: FnMut(&mut PlanBuilder),
        FR: FnMut(&mut PlanBuilder),
    {
        self.plan
            .join_func(join_kind, left_task, right_task, left_keys, right_keys);
        self
    }

    pub fn union(&mut self, plans: Vec<PlanBuilder>) -> &mut Self {
        self.plan.union(plans);
        self
    }

    pub fn intersect(&mut self, plans: Vec<PlanBuilder>, key: common_pb::NameOrId) -> &mut Self {
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

    pub fn sample(&mut self, sample: algebra_pb::Sample) {
        self.plan.sample(sample);
    }

    pub fn sink(&mut self, sink: algebra_pb::Sink) {
        self.plan.sink(sink);
    }

    pub fn take_plan(self) -> PlanBuilder {
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

        let plan = self.plan.build();

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
        let mut builder = PlanBuilder::default();
        let source_pb = algebra_pb::Scan {
            scan_opt: 0,
            alias: None,
            params: None,
            idx_predicate: None,
            is_count_only: false,
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
        let mut builder = PlanBuilder::default();
        let scan1_pb = algebra_pb::Scan {
            scan_opt: 0,
            alias: None,
            params: None,
            idx_predicate: None,
            is_count_only: false,
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

        let join_op = plan[1].clone();
        if let Some(pb::physical_opr::operator::OpKind::Join(join)) = join_op.opr.unwrap().op_kind {
            // scan
            assert_eq!(join.left_plan.unwrap().plan.len(), 1);
            // scan, project
            assert_eq!(join.right_plan.unwrap().plan.len(), 2);
        } else {
            assert!(false)
        }

        // another way to build `Join`
        let mut left_builder = PlanBuilder::default();
        let mut right_builder = PlanBuilder::default();
        left_builder.add_scan_source(scan1_pb);
        right_builder
            .add_scan_source(scan2_pb)
            .project(project_pb);
        let mut builder2 = PlanBuilder::default();
        builder2
            .add_dummy_source()
            .join(algebra_pb::join::JoinKind::Inner, left_builder, right_builder, vec![], vec![])
            .sink(sink_pb);

        assert_eq!(plan, builder2.take())
    }
}
