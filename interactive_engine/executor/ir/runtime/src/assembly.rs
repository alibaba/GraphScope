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
use std::sync::Arc;
use std::vec;

use graph_proxy::apis::cluster_info::ClusterInfo;
use graph_proxy::apis::partitioner::PartitionInfo;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::generated::physical as pb;
use ir_common::generated::physical::physical_opr::operator::OpKind;
use pegasus::api::function::*;
use pegasus::api::{
    Collect, CorrelatedSubTask, Count, Dedup, Filter, Fold, FoldByKey, HasAny, IterCondition, Iteration,
    Join, KeyBy, Limit, Map, Merge, Sink, SortBy, SortLimitBy,
};
use pegasus::stream::Stream;
use pegasus::{BuildJobError, Worker};
use pegasus_server::job::{JobAssembly, JobDesc};
use pegasus_server::job_pb as server_pb;
use prost::Message;

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::functions::{ApplyGen, CompareFunction, FoldGen, GroupGen, JoinKeyGen, KeyFunction};
use crate::process::operator::accum::accumulator::Accumulator;
use crate::process::operator::accum::{SampleAccum, SampleAccumFactoryGen};
use crate::process::operator::filter::FilterFuncGen;
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::operator::keyed::KeyFunctionGen;
use crate::process::operator::map::{FilterMapFuncGen, MapFuncGen};
use crate::process::operator::shuffle::RecordRouter;
use crate::process::operator::sink::{SinkGen, Sinker};
use crate::process::operator::sort::CompareFunctionGen;
use crate::process::operator::source::SourceOperator;
use crate::process::record::{Record, RecordKey};
use crate::router::{DefaultRouter, Router};

type RecordMap = Box<dyn MapFunction<Record, Record>>;
type RecordFilterMap = Box<dyn FilterMapFunction<Record, Record>>;
type RecordFlatMap = Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>;
type RecordFilter = Box<dyn FilterFunction<Record>>;
type RecordLeftJoin = Box<dyn ApplyGen<Record, Vec<Record>, Option<Record>>>;
type RecordShuffle = Box<dyn RouteFunction<Record>>;
type RecordCompare = Box<dyn CompareFunction<Record>>;
type RecordJoin = Box<dyn JoinKeyGen<Record, RecordKey, Record>>;
type RecordKeySelector = Box<dyn KeyFunction<Record, RecordKey, Record>>;
type RecordGroup = Box<dyn GroupGen<Record, RecordKey, Record>>;
type RecordFold = Box<dyn FoldGen<u64, Record>>;

pub struct IRJobAssembly<P: PartitionInfo, C: ClusterInfo> {
    udf_gen: FnGenerator<P, C>,
}

struct FnGenerator<P: PartitionInfo, C: ClusterInfo> {
    router: Arc<dyn Router<P = P, C = C>>,
}

impl<P: PartitionInfo, C: ClusterInfo> Clone for FnGenerator<P, C> {
    fn clone(&self) -> Self {
        Self { router: self.router.clone() }
    }
}

/// An UDF generator for physical operators,
/// which generates the udf that can be executed by the engine.
impl<P: PartitionInfo, C: ClusterInfo> FnGenerator<P, C> {
    fn new(router: Arc<dyn Router<P = P, C = C>>) -> Self {
        FnGenerator { router }
    }

    fn with(partition_info: Arc<P>, cluster_info: Arc<C>) -> Self {
        let router = Arc::new(DefaultRouter::new(partition_info, cluster_info));
        FnGenerator { router }
    }

    fn gen_source(&self, opr: pb::PhysicalOpr) -> FnGenResult<DynIter<Record>> {
        let worker_id = pegasus::get_current_worker();
        let source_opr = SourceOperator::new(opr, self.router.clone())?;
        Ok(source_opr.gen_source(worker_id.index as usize)?)
    }

    fn gen_shuffle(&self, res: &pb::repartition::Shuffle) -> FnGenResult<RecordShuffle> {
        let p = self.router.clone();
        let record_router = RecordRouter::new(p, res.shuffle_key)?;
        Ok(Box::new(record_router))
    }

    fn gen_project(&self, opr: pb::Project) -> FnGenResult<RecordFilterMap> {
        Ok(opr.gen_filter_map()?)
    }

    fn gen_unfold(&self, opr: pb::Unfold) -> FnGenResult<RecordFlatMap> {
        Ok(opr.gen_flat_map()?)
    }

    fn gen_filter(&self, opr: algebra_pb::Select) -> FnGenResult<RecordFilter> {
        Ok(opr.gen_filter()?)
    }

    fn gen_cmp(&self, opr: algebra_pb::OrderBy) -> FnGenResult<RecordCompare> {
        Ok(opr.gen_cmp()?)
    }

    fn gen_group(&self, opr: pb::GroupBy) -> FnGenResult<RecordGroup> {
        Ok(Box::new(opr))
    }

    fn gen_fold(&self, opr: pb::GroupBy) -> FnGenResult<RecordFold> {
        Ok(Box::new(opr))
    }

    fn gen_apply(&self, opr: pb::Apply) -> FnGenResult<RecordLeftJoin> {
        Ok(Box::new(opr))
    }

    fn gen_join(&self, opr: pb::Join) -> FnGenResult<RecordJoin> {
        Ok(Box::new(opr))
    }

    fn gen_dedup(&self, opr: algebra_pb::Dedup) -> FnGenResult<RecordKeySelector> {
        Ok(opr.gen_key()?)
    }

    fn gen_vertex(&self, opr: pb::GetV) -> FnGenResult<RecordFilterMap> {
        Ok(opr.gen_filter_map()?)
    }

    fn gen_both_vertex(&self, opr: pb::GetV) -> FnGenResult<RecordFlatMap> {
        Ok(opr.gen_flat_map()?)
    }

    fn gen_edge_expand(&self, opr: pb::EdgeExpand) -> FnGenResult<RecordFlatMap> {
        Ok(opr.gen_flat_map()?)
    }

    fn gen_edge_expand_collection(&self, opr: pb::EdgeExpand) -> FnGenResult<RecordFilterMap> {
        Ok(opr.gen_filter_map()?)
    }

    fn gen_general_edge_expand_collection(
        &self, opr: pb::EdgeExpand, opr2: Option<pb::GetV>,
    ) -> FnGenResult<RecordFilterMap> {
        Ok((opr, opr2).gen_filter_map()?)
    }

    fn gen_path_start(&self, opr: pb::PathExpand) -> FnGenResult<RecordFilterMap> {
        Ok(opr.gen_filter_map()?)
    }

    fn gen_path_end(&self, opr: pb::PathExpand) -> FnGenResult<RecordMap> {
        Ok(opr.gen_map()?)
    }

    fn gen_path_condition(&self, opr: pb::PathExpand) -> FnGenResult<RecordFilter> {
        Ok(opr.gen_filter()?)
    }

    fn gen_coin(&self, opr: algebra_pb::Sample) -> FnGenResult<RecordFilter> {
        Ok(opr.gen_filter()?)
    }

    fn gen_sample(&self, opr: algebra_pb::Sample) -> FnGenResult<SampleAccum> {
        Ok(opr.gen_accum()?)
    }

    fn gen_sink(&self, opr: pb::PhysicalOpr) -> FnGenResult<Sinker> {
        Ok(opr.gen_sink()?)
    }
}

impl<P: PartitionInfo, C: ClusterInfo> IRJobAssembly<P, C> {
    pub fn new(router: Arc<dyn Router<P = P, C = C>>) -> Self {
        let udf_gen = FnGenerator::new(router);
        IRJobAssembly { udf_gen }
    }

    pub fn with(partition_info: Arc<P>, cluster_info: Arc<C>) -> Self {
        let udf_gen = FnGenerator::with(partition_info, cluster_info);
        IRJobAssembly { udf_gen }
    }

    fn install(
        &self, mut stream: Stream<Record>, plan: &[pb::PhysicalOpr],
    ) -> Result<Stream<Record>, BuildJobError> {
        let mut prev_op_kind = pb::physical_opr::operator::OpKind::Root(pb::Root {});
        for op in &plan[..] {
            let op_kind = to_op_kind(op)?;
            match op_kind {
                OpKind::Repartition(repartition) => {
                    let repartition_strategy = repartition.strategy.as_ref().ok_or_else(|| {
                        FnGenError::from(ParsePbError::EmptyFieldError(
                            "Empty repartition strategy".to_string(),
                        ))
                    })?;
                    match repartition_strategy {
                        pb::repartition::Strategy::ToAnother(shuffle) => {
                            let router = self.udf_gen.gen_shuffle(shuffle)?;
                            stream = stream.repartition(move |t| router.route(t));
                        }
                        pb::repartition::Strategy::ToOthers(_) => stream = stream.broadcast(),
                    }
                }
                OpKind::Project(project) => {
                    let func = self.udf_gen.gen_project(project)?;
                    stream = stream.filter_map_with_name("Project", move |input| func.exec(input))?;
                }
                OpKind::Select(select) => {
                    let func = self.udf_gen.gen_filter(select)?;
                    stream = stream.filter(move |input| func.test(input))?;
                }
                OpKind::Unfold(unfold) => {
                    let func = self.udf_gen.gen_unfold(unfold)?;
                    stream = stream.flat_map_with_name("Unfold", move |input| func.exec(input))?;
                }
                OpKind::Limit(limit) => {
                    let range = limit.range.ok_or_else(|| {
                        FnGenError::from(ParsePbError::EmptyFieldError("pb::Limit::range".to_string()))
                    })?;
                    // e.g., `limit(10)` would be translate as `Range{lower=0, upper=10}`
                    if range.upper <= range.lower || range.lower != 0 {
                        Err(FnGenError::from(ParsePbError::ParseError(format!(
                            "range {:?} in Limit Operator",
                            range
                        ))))?;
                    }
                    stream = stream.limit(range.upper as u32)?;
                }
                OpKind::OrderBy(order) => {
                    let cmp = self.udf_gen.gen_cmp(order.clone())?;
                    if let Some(range) = order.limit {
                        if range.upper <= range.lower || range.lower != 0 {
                            Err(FnGenError::from(ParsePbError::ParseError(format!(
                                "range {:?} in Order Operator",
                                range
                            ))))?;
                        }
                        stream = stream.sort_limit_by(range.upper as u32, move |a, b| cmp.compare(a, b))?;
                    } else {
                        stream = stream.sort_by(move |a, b| cmp.compare(a, b))?;
                    }
                }
                OpKind::GroupBy(group) => {
                    if group.mappings.is_empty() {
                        // fold case
                        let fold = self.udf_gen.gen_fold(group)?;
                        if let server_pb::AccumKind::Cnt = fold.get_accum_kind() {
                            let fold_map = fold.gen_fold_map()?;
                            stream = stream
                                .count()?
                                .map(move |cnt| fold_map.exec(cnt))?
                                .into_stream()?;
                        } else {
                            // TODO: optimize this by fold_partition + fold
                            let fold_accum = fold.gen_fold_accum()?;
                            stream = stream
                                .fold(fold_accum, || {
                                    |mut accumulator, next| {
                                        accumulator.accum(next)?;
                                        Ok(accumulator)
                                    }
                                })?
                                .map(move |mut accum| Ok(accum.finalize()?))?
                                .into_stream()?;
                        }
                    } else {
                        // group case
                        let group = self.udf_gen.gen_group(group)?;
                        let group_key = group.gen_group_key()?;
                        let group_accum = group.gen_group_accum()?;
                        let group_map = group.gen_group_map()?;
                        stream = stream
                            .key_by(move |record| group_key.get_kv(record))?
                            .fold_partition_by_key(group_accum, || {
                                |mut accumulator, next| {
                                    accumulator.accum(next)?;
                                    Ok(accumulator)
                                }
                            })?
                            .unfold(|kv_map| {
                                Ok(kv_map
                                    .into_iter()
                                    .map(|(key, mut accumulator)| {
                                        accumulator.finalize().map(|value| (key, value))
                                    })
                                    .collect::<Result<Vec<_>, _>>()?
                                    .into_iter())
                            })?
                            .map(move |key_value| group_map.exec(key_value))?;
                    }
                }
                OpKind::Dedup(dedup) => {
                    let selector = self.udf_gen.gen_dedup(dedup)?;
                    stream = stream
                        .key_by(move |record| selector.get_kv(record))?
                        .dedup()?
                        .map(|pair| Ok(pair.value))?;
                }
                OpKind::Union(union) => {
                    let (mut ori_stream, sub_stream) = stream.copied()?;
                    stream = self.install(sub_stream, &union.sub_plans[0].plan[..])?;
                    for subtask in &union.sub_plans[1..] {
                        let copied = ori_stream.copied()?;
                        ori_stream = copied.0;
                        stream = self
                            .install(copied.1, &subtask.plan[..])?
                            .merge(stream)?;
                    }
                }
                OpKind::Apply(apply) => {
                    if apply.keys.is_empty() {
                        // apply
                        let apply_gen = self.udf_gen.gen_apply(apply.clone())?;
                        let join_kind = apply_gen.get_join_kind();
                        let join_func = apply_gen.gen_left_join_func()?;
                        let sub_task = apply.sub_plan.as_ref().ok_or_else(|| {
                            BuildJobError::Unsupported("Task is missing in Apply".to_string())
                        })?;
                        stream = match join_kind {
                            JoinKind::Semi => stream
                                .apply(|sub_start| {
                                    let has_sub = self
                                        .install(sub_start, &sub_task.plan[..])?
                                        .any()?;
                                    Ok(has_sub)
                                })?
                                .filter_map(
                                    move |(parent, has_sub)| {
                                        if has_sub {
                                            Ok(Some(parent))
                                        } else {
                                            Ok(None)
                                        }
                                    },
                                )?,
                            JoinKind::Anti => stream
                                .apply(|sub_start| {
                                    let has_sub = self
                                        .install(sub_start, &sub_task.plan[..])?
                                        .any()?;
                                    Ok(has_sub)
                                })?
                                .filter_map(
                                    move |(parent, has_sub)| {
                                        if has_sub {
                                            Ok(None)
                                        } else {
                                            Ok(Some(parent))
                                        }
                                    },
                                )?,
                            JoinKind::Inner | JoinKind::LeftOuter => stream
                                .apply(|sub_start| {
                                    let sub_end = self
                                        .install(sub_start, &sub_task.plan[..])?
                                        .collect::<Vec<Record>>()?;
                                    Ok(sub_end)
                                })?
                                .filter_map(move |(parent, sub)| join_func.exec(parent, sub))?,
                            _ => Err(BuildJobError::Unsupported(format!(
                                "Do not support join_kind {:?} in Apply",
                                join_kind
                            )))?,
                        };
                    } else {
                        // segment apply
                        Err(FnGenError::unsupported_error("SegmentApply Operator"))?
                    }
                }
                OpKind::Join(join) => {
                    let joiner = self.udf_gen.gen_join(join.clone())?;
                    let left_key_selector = joiner.gen_left_kv_fn()?;
                    let right_key_selector = joiner.gen_right_kv_fn()?;
                    let join_kind = joiner.get_join_kind();
                    let left_task = join
                        .left_plan
                        .as_ref()
                        .ok_or_else(|| FnGenError::ParseError("left_task is missing in merge".into()))?;
                    let right_task = join
                        .right_plan
                        .as_ref()
                        .ok_or_else(|| FnGenError::ParseError("right_task is missing in merge".into()))?;
                    let (left_stream, right_stream) = stream.copied()?;
                    let left_stream = self
                        .install(left_stream, &left_task.plan[..])?
                        .key_by(move |record| left_key_selector.get_kv(record))?;
                    let right_stream = self
                        .install(right_stream, &right_task.plan[..])?
                        .key_by(move |record| right_key_selector.get_kv(record))?;
                    stream = match join_kind {
                        JoinKind::Inner => left_stream
                            .inner_join(right_stream)?
                            .map(|(left, right)| Ok(left.value.join(right.value, None)))?,
                        JoinKind::LeftOuter => {
                            left_stream
                                .left_outer_join(right_stream)?
                                .map(|(left, right)| {
                                    let left = left.ok_or_else(|| {
                                        FnExecError::unexpected_data_error(
                                            "left is None in left outer join",
                                        )
                                    })?;
                                    if let Some(right) = right {
                                        // TODO(bingqing): Specify HeadJoinOpt if necessary
                                        Ok(left.value.join(right.value, None))
                                    } else {
                                        Ok(left.value)
                                    }
                                })?
                        }
                        JoinKind::RightOuter => left_stream
                            .right_outer_join(right_stream)?
                            .map(|(left, right)| {
                                let right = right.ok_or_else(|| {
                                    FnExecError::unexpected_data_error("right is None in right outer join")
                                })?;
                                if let Some(left) = left {
                                    Ok(left.value.join(right.value, None))
                                } else {
                                    Ok(right.value)
                                }
                            })?,
                        JoinKind::FullOuter => {
                            left_stream
                                .full_outer_join(right_stream)?
                                .map(|(left, right)| match (left, right) {
                                    (Some(left), Some(right)) => Ok(left.value.join(right.value, None)),
                                    (Some(left), None) => Ok(left.value),
                                    (None, Some(right)) => Ok(right.value),
                                    (None, None) => {
                                        unreachable!()
                                    }
                                })?
                        }
                        JoinKind::Semi => left_stream
                            .semi_join(right_stream)?
                            .map(|left| Ok(left.value))?,
                        JoinKind::Anti => left_stream
                            .anti_join(right_stream)?
                            .map(|left| Ok(left.value))?,
                        JoinKind::Times => Err(BuildJobError::Unsupported(
                            "JoinKind of Times is not supported yet".to_string(),
                        ))?,
                    }
                }
                OpKind::Intersect(intersect) => {
                    // The subplan in intersect can be:
                    //     1) (repartition) + EdgeExpand (ExpandV) which is to expand and intersect on id-only vertices;
                    //        or (repartition) + EdgeExpand (ExpandV) + (repartition) + GetV(Itself), which is to expand and intersect on vertices.
                    //        In this case, GetV(Itself) usually stands for further filtering on the intersected vertices.
                    //     2) (repartition) + EdgeExpand (ExpandE) + GetV(Adj), which is to expand and intersect on vertices.
                    //        In this case, EdgeExpand and GetV are not fused, usually with alias in EdgeExpand; Not supported yet;
                    //        or (repartition) + EdgeExpand (ExpandE) + GetV(Adj) + (repartition) + GetV(Itself), which is to expand and intersect on vertices.
                    //        In this case, EdgeExpand and GetV are not fused, usually with alias in EdgeExpand;
                    //        And GetV(Itself) usually stands for further filtering on the intersected vertices.
                    //     3) (repartition) + PathExpand + GetV(EndV), which is to expand paths and intersect on the end vertices.
                    //        or (repartition) + PathExpand + GetV(EndV) + (repartition) + GetV(Itself), which is to expand paths and intersect on the end vertices.
                    //        And GetV(Itself) usually stands for further filtering on the intersected vertices.
                    // Specifically, we slightly modify the plan due to the implementation, as follows:
                    //     1) basically, we extract all the last edge_expand step (ExpandV or ExpandE+GetV), for intersection;
                    //     2) the intersect results is a collection, we need to add an Unfold op after intersect;
                    //     3) if there are some further filters on the intersected vertices on distributed graph database,
                    //        we need to add a repartition + Auxilia op after Unfold to filter the intersected vertices.
                    //     4) for the cases of PathExpand, we need to pre-expand the path and endV, and then intersect on the last edge_expand.
                    //     5) specifically, if the edge_expands to intersect are all ExpandV, we can apply the optimized intersect implementation
                    //        i.e., ExpandIntersect which won't preserve any edges during the intersect
                    let mut intersected_expands = vec![];
                    let mut pre_expands = vec![];
                    let mut auxilia: Option<pb::GetV> = None;
                    let mut auxilia_repartition = None;
                    for mut subplan in intersect.sub_plans {
                        let subplan_clone = subplan.clone();
                        let mut last_op = subplan.plan.pop().ok_or_else(|| {
                            FnGenError::from(ParsePbError::EmptyFieldError(
                                "subplan in pb::Intersect::plan".to_string(),
                            ))
                        })?;

                        // if the last opr is Auxilia, move it after intersect
                        if let OpKind::Vertex(mut vertex) = to_op_kind(&last_op)? {
                            if vertex.opt == pb::get_v::VOpt::Itself as i32 {
                                vertex.tag = Some(intersect.key);
                                auxilia = Some(vertex.clone());
                                if subplan
                                    .plan
                                    .last()
                                    .map(|op| op.is_repartition())
                                    .unwrap_or(false)
                                {
                                    auxilia_repartition = subplan.plan.pop();
                                }
                                last_op = subplan.plan.pop().ok_or_else(|| {
                                    FnGenError::unsupported_error(&format!(
                                        "subplan with only getV in pb::Intersect::plan {:?}",
                                        vertex,
                                    ))
                                })?;
                            }
                        }

                        // then, process subplans after removing the last Auxilia
                        let last_op_kind = to_op_kind(&last_op)?;
                        match last_op_kind {
                            // case 1: EdgeExpandV
                            OpKind::Edge(mut expand) => {
                                expand.alias = Some(intersect.key.clone());
                                if let Some(opr) = subplan.plan.last() {
                                    if opr.is_repartition() {
                                        intersected_expands.push((subplan.plan.pop(), expand, None));
                                    } else {
                                        Err(FnGenError::unsupported_error(&format!(
                                            "Subplan in Intersection in EdgeExpandV {:?}",
                                            PhysicalPlanPrinter(&subplan_clone),
                                        )))?
                                    }
                                } else {
                                    intersected_expands.push((None, expand, None));
                                }
                            }
                            // case 2/3: PathExpand/EdgeExpand + GetV
                            OpKind::Vertex(mut get_v) => {
                                let prev_opr_kind = to_op_kind(&subplan.plan.pop().ok_or_else(|| {
                                    FnGenError::unsupported_error(&format!(
                                        "subplan with only getV in pb::Intersect::plan {:?}",
                                        get_v,
                                    ))
                                })?)?;
                                match prev_opr_kind {
                                    OpKind::Edge(edge_expand) => {
                                        // case2: ExpandE + GetV(Adj)
                                        if get_v.opt == pb::get_v::VOpt::Itself as i32 {
                                            Err(FnGenError::unsupported_error(&format!(
                                                "Subplan in Intersection in EdgeExpandE+GetV {:?}",
                                                PhysicalPlanPrinter(&subplan_clone),
                                            )))?
                                        }
                                        // note that this get_v won't take filters, as it should be translated to auxilia.
                                        if let Some(params) = &get_v.params {
                                            if params.has_predicates() || params.has_columns() {
                                                Err(FnGenError::unsupported_error(&format!(
                                                    "Subplan in Intersection in EdgeExpandE+GetV {:?}",
                                                    PhysicalPlanPrinter(&subplan_clone),
                                                )))?
                                            }
                                        }
                                        if let Some(opr) = subplan.plan.last() {
                                            if opr.is_repartition() {
                                                intersected_expands.push((
                                                    subplan.plan.pop(),
                                                    edge_expand,
                                                    Some(get_v),
                                                ));
                                            } else {
                                                Err(FnGenError::unsupported_error(&format!(
                                                    "Subplan in Intersection in EdgeExpandE+GetV {:?}",
                                                    PhysicalPlanPrinter(&subplan_clone),
                                                )))?
                                            }
                                        } else {
                                            intersected_expands.push((None, edge_expand, Some(get_v)));
                                        }
                                    }
                                    OpKind::Path(mut path_expand) => {
                                        // case3: PathExpand + GetV(EndV)
                                        if get_v.opt != pb::get_v::VOpt::End as i32 {
                                            Err(FnGenError::unsupported_error(&format!(
                                                "Subplan in Intersection in PathExpand + GetV {:?}",
                                                PhysicalPlanPrinter(&subplan_clone),
                                            )))?
                                        }
                                        let path_repartition = if let Some(opr) = subplan.plan.last() {
                                            if opr.is_repartition() {
                                                subplan.plan.pop()
                                            } else {
                                                Err(FnGenError::unsupported_error(&format!(
                                                    "Subplan in Intersection in PathExpand + GetV {:?}",
                                                    PhysicalPlanPrinter(&subplan_clone),
                                                )))?
                                            }
                                        } else {
                                            None
                                        };
                                        // the case of expand paths and intersect on the end vertices
                                        // Process path_expand as follows:
                                        // 1. If path_expand range from 0, it is unsupported;
                                        // 2. If it is path_expand(1,2), optimized as edge_expand;
                                        // 3. Otherwise, translate path_expand(l,h) to path_expand(l-1, h-1) + endV() + edge_expand,
                                        //    and the last edge_expand is the one to intersect.
                                        //    Notice that if we have predicates for vertices in path_expand, or for the last vertex of path_expand,
                                        //    do the filtering after intersection.
                                        // TODO: there might be a bug here:
                                        // if path_expand has an alias which indicates that the path would be referred later, it may not as expected.
                                        let path_expand_base =
                                            path_expand.base.as_ref().ok_or_else(|| {
                                                FnGenError::ParseError(
                                                    "PathExpand::base in Pattern is empty".into(),
                                                )
                                            })?;
                                        let base_edge_expand = path_expand_base
                                            .edge_expand
                                            .as_ref()
                                            .ok_or_else(|| {
                                                FnGenError::ParseError(
                                                    "PathExpand::base::edge_expand is empty".into(),
                                                )
                                            })?;
                                        // current only support expand_opt = ExpandV
                                        if base_edge_expand.expand_opt
                                            != pb::edge_expand::ExpandOpt::Vertex as i32
                                        {
                                            Err(FnGenError::unsupported_error(&format!(
                                                "PathExpand in Intersection with expand {:?}",
                                                base_edge_expand
                                            )))?
                                        }
                                        // pick the last edge expand out from the path expand
                                        let hop_range =
                                            path_expand.hop_range.as_mut().ok_or_else(|| {
                                                FnGenError::ParseError(
                                                    "pb::PathExpand::hop_range is empty".into(),
                                                )
                                            })?;
                                        if hop_range.lower < 1 {
                                            Err(FnGenError::unsupported_error(&format!(
                                                "PathExpand in Intersection with lower range of {:?}",
                                                hop_range.lower
                                            )))?
                                        }
                                        let mut edge_expand = base_edge_expand.clone();
                                        let mut edge_repartition = None;
                                        if hop_range.lower == 1 && hop_range.upper == 2 {
                                            // optimized Path(1..2) to as EdgeExpand
                                            edge_expand.v_tag = path_expand.start_tag;
                                            edge_expand.alias = get_v.alias;
                                            edge_repartition = path_repartition.clone();
                                        } else {
                                            // translate path_expand(l,h) to path_expand(l-1, h-1) + endV() + edge_expand,
                                            edge_expand.v_tag = None;
                                            // edge expand should carry endv's alias, which is the intersect key.
                                            edge_expand.alias = get_v.alias.clone();
                                            get_v.alias.take();
                                            hop_range.lower -= 1;
                                            hop_range.upper -= 1;
                                            // pre expand path_expand(l-1, h-1)
                                            if let Some(repartition) = path_repartition.clone() {
                                                pre_expands.push(repartition);
                                            }
                                            pre_expands.push(path_expand.into());
                                            pre_expands.push(get_v.into());
                                            if path_repartition.is_some() {
                                                edge_repartition = Some(
                                                    pb::Repartition {
                                                        strategy: Some(
                                                            pb::repartition::Strategy::ToAnother(
                                                                pb::repartition::Shuffle {
                                                                    shuffle_key: None,
                                                                },
                                                            ),
                                                        ),
                                                    }
                                                    .into(),
                                                );
                                            }
                                        }
                                        // and then expand and intersect on the last edge_expand
                                        intersected_expands.push((
                                            edge_repartition.clone(),
                                            edge_expand,
                                            None,
                                        ));
                                    }

                                    _ => Err(FnGenError::unsupported_error(&format!(
                                        "Subplan in Intersection to intersect: {:?}",
                                        PhysicalPlanPrinter(&subplan),
                                    )))?,
                                }
                            }

                            _ => Err(FnGenError::unsupported_error(&format!(
                                "Opr in Intersection to intersect: {:?}",
                                last_op_kind
                            )))?,
                        }
                    }

                    // pre-expanding for the path_expand case
                    if !pre_expands.is_empty() {
                        stream = self.install(stream, &pre_expands)?;
                    }
                    // process intersect of edge_expands
                    let is_optimized = intersected_expands
                        .iter()
                        .all(|(_, _, get_v)| get_v.is_none());
                    let mut intersect_expand_funcs = Vec::with_capacity(intersected_expands.len());
                    for (repartition, expand, get_v) in intersected_expands {
                        let expand_func = if !is_optimized {
                            self.udf_gen
                                .gen_general_edge_expand_collection(expand, get_v)?
                        } else {
                            self.udf_gen
                                .gen_edge_expand_collection(expand)?
                        };
                        intersect_expand_funcs.push((repartition, expand_func));
                    }
                    // intersect of edge_expands
                    for (repartition, expand_intersect_func) in intersect_expand_funcs {
                        if let Some(repartition) = repartition {
                            stream = self.install(stream, &vec![repartition])?;
                        }
                        stream = stream.filter_map_with_name("ExpandIntersect", move |input| {
                            expand_intersect_func.exec(input)
                        })?;
                    }
                    // unfold the intersection
                    let unfold =
                        pb::Unfold { tag: Some(intersect.key.into()), alias: Some(intersect.key.into()) };
                    stream = self.install(stream, &vec![unfold.into()])?;

                    // add vertex filters
                    if let Some(mut auxilia) = auxilia {
                        auxilia.tag = Some(intersect.key.into());
                        if let Some(auxilia_repartition) = auxilia_repartition {
                            stream = self.install(stream, &vec![auxilia_repartition, auxilia.into()])?;
                        } else {
                            stream = self.install(stream, &vec![auxilia.into()])?;
                        }
                    }
                }
                OpKind::Vertex(vertex) => {
                    let vertex_opt: algebra_pb::get_v::VOpt = unsafe { std::mem::transmute(vertex.opt) };
                    match vertex_opt {
                        algebra_pb::get_v::VOpt::Both => {
                            let func = self.udf_gen.gen_both_vertex(vertex)?;
                            stream = stream.flat_map_with_name("GetV", move |input| func.exec(input))?;
                        }
                        _ => {
                            let func = self.udf_gen.gen_vertex(vertex)?;
                            stream = stream.filter_map_with_name("GetV", move |input| func.exec(input))?;
                        }
                    }
                }
                OpKind::Edge(edge) => {
                    let func = self.udf_gen.gen_edge_expand(edge)?;
                    stream = stream.flat_map_with_name("EdgeExpand", move |input| func.exec(input))?;
                }
                OpKind::Path(path) => {
                    let mut base = path.base.clone().ok_or_else(|| {
                        FnGenError::from(ParsePbError::EmptyFieldError("pb::PathExpand::base".to_string()))
                    })?;
                    let range = path.hop_range.as_ref().ok_or_else(|| {
                        FnGenError::from(ParsePbError::EmptyFieldError(
                            "pb::PathExpand::hop_range".to_string(),
                        ))
                    })?;
                    if range.upper <= range.lower || range.lower < 0 || range.upper <= 0 {
                        Err(FnGenError::from(ParsePbError::ParseError(format!(
                            "range {:?} in PathExpand Operator",
                            range
                        ))))?;
                    }
                    // path start
                    let path_start_func = self.udf_gen.gen_path_start(path.clone())?;
                    stream = stream
                        .filter_map_with_name("PathStart", move |input| path_start_func.exec(input))?;
                    // path base expand
                    let mut base_expand_plan = vec![];
                    // process edge_expand
                    let edge_expand = base.edge_expand.take().ok_or_else(|| {
                        FnGenError::from(ParsePbError::ParseError(format!(
                            "empty EdgeExpand of ExpandBase in PathExpand Operator {:?}",
                            base
                        )))
                    })?;
                    if (pb::path_expand::ResultOpt::AllVE
                        == unsafe { std::mem::transmute(path.result_opt) }
                        || pb::path_expand::PathOpt::Trail == unsafe { std::mem::transmute(path.path_opt) })
                        && pb::edge_expand::ExpandOpt::Vertex
                            == unsafe { std::mem::transmute(edge_expand.expand_opt) }
                    {
                        // the case when base expand is expand vertex, but needs to expand edges + vertices since the result opt is ALLVE
                        // TODO: in the new compilation stack, this case will not happen.
                        let mut edge_expand_e = edge_expand.clone();
                        edge_expand_e.expand_opt = pb::edge_expand::ExpandOpt::Edge as i32;
                        let alias = edge_expand_e.alias.take();
                        let get_v =
                            pb::GetV { opt: pb::get_v::VOpt::Other as i32, tag: None, params: None, alias };
                        base_expand_plan.push(edge_expand_e.into());
                        base_expand_plan.push(get_v.into());
                    } else {
                        base_expand_plan.push(edge_expand.into());
                    }
                    let repartition = if let OpKind::Repartition(_) = &prev_op_kind {
                        // the case when base expand needs repartition
                        Some(
                            pb::Repartition {
                                strategy: Some(pb::repartition::Strategy::ToAnother(
                                    pb::repartition::Shuffle { shuffle_key: None },
                                )),
                            }
                            .into(),
                        )
                    } else {
                        None
                    };
                    // process get_v
                    if let Some(mut getv) = base.get_v.take() {
                        if (pb::get_v::VOpt::Itself as i32) == getv.opt {
                            // the case of expandv + auxilia (to deal with filtering on vertices).
                            if let Some(repartition) = repartition {
                                base_expand_plan.push(repartition);
                            }
                            base_expand_plan.push(getv.clone().into());
                        } else {
                            // the case of expande + getv
                            // specifically, if getv has predicates or columns,
                            // separate it as getv(getAdj) + auxilia (for filter/get columns)
                            let needs_separate = getv
                                .params
                                .as_ref()
                                .map_or(false, |params| params.has_predicates() || params.has_columns());
                            if needs_separate {
                                let alias = getv.alias.take();
                                let params = getv.params.take();
                                let auxilia = pb::GetV {
                                    opt: pb::get_v::VOpt::Itself as i32,
                                    tag: None,
                                    params: params,
                                    alias,
                                };
                                base_expand_plan.push(getv.clone().into());
                                if let Some(repartition) = repartition {
                                    base_expand_plan.push(repartition);
                                }
                                base_expand_plan.push(auxilia.into());
                            } else {
                                base_expand_plan.push(getv.clone().into());
                                if let Some(repartition) = repartition {
                                    base_expand_plan.push(repartition);
                                }
                            }
                        }
                    } else {
                        // the case of expandv
                        if let Some(repartition) = repartition {
                            base_expand_plan.push(repartition);
                        }
                    }

                    for _ in 0..range.lower {
                        stream = self.install(stream, &base_expand_plan)?;
                    }
                    let times = range.upper - range.lower - 1;
                    if times > 0 {
                        if path.condition.is_some() {
                            let mut until = IterCondition::max_iters(times as u32);
                            let func = self.udf_gen.gen_path_condition(path.clone())?;
                            until.set_until(func);
                            // Notice that if UNTIL condition set, we expand path without `Emit`
                            stream = stream
                                .iterate_until(until, |start| self.install(start, &base_expand_plan[..]))?;
                        } else {
                            let (mut hop_stream, copied_stream) = stream.copied()?;
                            stream = copied_stream;
                            for _ in 0..times {
                                hop_stream = self.install(hop_stream, &base_expand_plan[..])?;
                                let copied = hop_stream.copied()?;
                                hop_stream = copied.0;
                                stream = stream.merge(copied.1)?;
                            }
                        }
                    }
                    // path end to add path_alias if exists
                    if path.alias.is_some() {
                        let path_end_func = self.udf_gen.gen_path_end(path)?;
                        stream = stream.map_with_name("PathEnd", move |input| path_end_func.exec(input))?;
                    }
                }
                OpKind::Scan(scan) => {
                    let udf_gen = self.udf_gen.clone();
                    stream = stream.flat_map(move |_| {
                        let scan_iter = udf_gen.gen_source(scan.clone().into());
                        Ok(scan_iter?)
                    })?;
                }
                OpKind::Sample(sample) => {
                    if let Some(sample_weight) = &sample.sample_weight {
                        if sample_weight.tag.is_some() || sample_weight.property.is_some() {
                            return Err(FnGenError::from(ParsePbError::ParseError(
                                "sample_weight is not supported yet".to_string(),
                            )))?;
                        }
                    }
                    if let Some(sample_type) = &sample.sample_type {
                        match &sample_type.inner {
                            // the case of Coin
                            Some(algebra_pb::sample::sample_type::Inner::SampleByRatio(_)) => {
                                let func = self.udf_gen.gen_coin(sample)?;
                                stream = stream.filter(move |input| func.test(input))?;
                            }
                            // the case of Sample
                            Some(algebra_pb::sample::sample_type::Inner::SampleByNum(_)) => {
                                let partial_sample_accum = self.udf_gen.gen_sample(sample)?;
                                let sample_accum = partial_sample_accum.clone();
                                stream = stream
                                    .fold_partition(partial_sample_accum, move || {
                                        move |mut sample_accum, next| {
                                            sample_accum.accum(next)?;
                                            Ok(sample_accum)
                                        }
                                    })?
                                    .unfold(move |mut sample_accum| Ok(sample_accum.finalize()?))?
                                    .fold(sample_accum, move || {
                                        move |mut sample_accum, next| {
                                            sample_accum.accum(next)?;
                                            Ok(sample_accum)
                                        }
                                    })?
                                    .unfold(move |mut sample_accum| Ok(sample_accum.finalize()?))?
                            }
                            None => Err(FnGenError::from(ParsePbError::EmptyFieldError(
                                "pb::Sample::sample_type.inner".to_string(),
                            )))?,
                        }
                    } else {
                        Err(FnGenError::from(ParsePbError::EmptyFieldError(
                            "pb::Sample::sample_type".to_string(),
                        )))?;
                    }
                }
                OpKind::Root(_) => {
                    // do nothing, as it is a dummy node
                }
                OpKind::Sink(_) => {
                    // this would be processed in assemble, and cannot be reached when install.
                    Err(FnGenError::unsupported_error("unreachable sink in install"))?
                }
                OpKind::ProcedureCall(procedure_call) => Err(FnGenError::unsupported_error(&format!(
                    "ProcedureCall Operator {:?}",
                    procedure_call
                )))?,
            }

            prev_op_kind = to_op_kind(op)?;
        }
        Ok(stream)
    }
}

impl<P: PartitionInfo, C: ClusterInfo> JobAssembly<Record> for IRJobAssembly<P, C> {
    fn assemble(&self, plan: &JobDesc, worker: &mut Worker<Record, Vec<u8>>) -> Result<(), BuildJobError> {
        worker.dataflow(move |input, output| {
            let physical_plan = decode::<pb::PhysicalPlan>(&plan.plan)?;
            if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
                debug!("{:#?}", PhysicalPlanPrinter(&physical_plan));
            }
            // input from a dummy record to trigger the computation
            let source = input.input_from(vec![Record::default()])?;
            let plan_len = physical_plan.plan.len();
            let stream = self.install(source, &physical_plan.plan[0..plan_len - 1])?;
            let sink_opr = physical_plan.plan.last().ok_or_else(|| {
                FnGenError::from(ParsePbError::EmptyFieldError("empty job plan".to_string()))
            })?;
            let ec = self.udf_gen.gen_sink(sink_opr.clone())?;
            match ec {
                Sinker::DefaultSinker(default_sinker) => stream
                    .map(move |record| default_sinker.exec(record))?
                    .sink_into(output),
                #[cfg(feature = "with_v6d")]
                Sinker::GraphSinker(graph_sinker) => {
                    return stream
                        .fold_partition(graph_sinker, || {
                            |mut accumulator, next| {
                                accumulator.accum(next)?;
                                Ok(accumulator)
                            }
                        })?
                        .map(|mut accumulator| Ok(accumulator.finalize()?))?
                        .into_stream()?
                        .map(|_r| Ok(vec![]))?
                        .sink_into(output)
                }
            }
        })
    }
}

#[inline]
fn decode<T: Message + Default>(binary: &[u8]) -> FnGenResult<T> {
    Ok(T::decode(binary)?)
}

#[inline]
fn to_op_kind(opr: &pb::PhysicalOpr) -> FnGenResult<OpKind> {
    Ok(opr.try_into()?)
}

struct PhysicalPlanPrinter<'a>(&'a pb::PhysicalPlan);
struct PhysicalOprPrinter<'a>(&'a pb::PhysicalOpr);

impl<'a> std::fmt::Debug for PhysicalPlanPrinter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plan")
            .field(
                "operations",
                &self
                    .0
                    .plan
                    .iter()
                    .map(PhysicalOprPrinter)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<'a> std::fmt::Debug for PhysicalOprPrinter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let opr = self.0;
        if let Some(opr) = &opr.opr {
            if let Some(op_kind) = &opr.op_kind {
                match op_kind {
                    OpKind::Apply(apply) => f
                        .debug_struct("Apply")
                        .field("keys", &apply.keys)
                        .field(
                            "sub_plan",
                            &apply
                                .sub_plan
                                .as_ref()
                                .map(|plan| PhysicalPlanPrinter(plan)),
                        )
                        .finish(),
                    OpKind::Join(join) => f
                        .debug_struct("Join")
                        .field(
                            "left_plan",
                            &join
                                .left_plan
                                .as_ref()
                                .map(|plan| PhysicalPlanPrinter(plan)),
                        )
                        .field(
                            "right_plan",
                            &join
                                .right_plan
                                .as_ref()
                                .map(|plan| PhysicalPlanPrinter(plan)),
                        )
                        .field("join_type", &join.join_kind)
                        .field("left_keys", &join.left_keys)
                        .field("right_keys", &join.right_keys)
                        .finish(),
                    OpKind::Union(union) => f
                        .debug_struct("Union")
                        .field(
                            "sub_plans",
                            &union
                                .sub_plans
                                .iter()
                                .map(|plan| PhysicalPlanPrinter(plan))
                                .collect::<Vec<_>>(),
                        )
                        .finish(),
                    OpKind::Intersect(intersect) => f
                        .debug_struct("Intersect")
                        .field(
                            "sub_plans",
                            &intersect
                                .sub_plans
                                .iter()
                                .map(|plan| PhysicalPlanPrinter(plan))
                                .collect::<Vec<_>>(),
                        )
                        .finish(),
                    _ => f
                        .debug_struct("PhysicalOpr")
                        .field("opr", op_kind)
                        .finish(),
                }
            } else {
                f.debug_struct("Empty PhysicalOprOpKind")
                    .finish()
            }
        } else {
            f.debug_struct("Empty PhysicalOpr").finish()
        }
    }
}
