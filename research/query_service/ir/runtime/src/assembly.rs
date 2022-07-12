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

use std::sync::Arc;

use graph_proxy::apis::Partitioner;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::algebra::join::JoinKind;
use ir_common::generated::algebra::logical_plan::operator::Opr as AlgebraOpr;
use ir_common::generated::common as common_pb;
use pegasus::api::function::*;
use pegasus::api::{
    Collect, CorrelatedSubTask, Count, Dedup, EmitKind, Filter, Fold, FoldByKey, HasAny, IterCondition,
    Iteration, Join, KeyBy, Limit, Map, MapWithName, Merge, PartitionByKey, Sink, SortBy, SortLimitBy,
};
use pegasus::codec::{Decode, Encode};
use pegasus::stream::Stream;
use pegasus::{BuildJobError, Worker};
use pegasus_server::job::{JobAssembly, JobDesc};
use pegasus_server::job_pb as server_pb;
use prost::Message;

use crate::error::{FnExecError, FnGenResult};
use crate::process::functions::{ApplyGen, CompareFunction, FoldGen, GroupGen, JoinKeyGen, KeyFunction};
use crate::process::operator::accum::accumulator::Accumulator;
use crate::process::operator::filter::FilterFuncGen;
use crate::process::operator::flatmap::FlatMapFuncGen;
use crate::process::operator::group::{FoldFactoryGen, GroupFunctionGen};
use crate::process::operator::join::JoinFunctionGen;
use crate::process::operator::keyed::KeyFunctionGen;
use crate::process::operator::map::{FilterMapFuncGen, MapFuncGen};
use crate::process::operator::shuffle::RecordRouter;
use crate::process::operator::sink::{SinkGen, Sinker};
use crate::process::operator::sort::CompareFunctionGen;
use crate::process::operator::source::SourceOperator;
use crate::process::operator::subtask::RecordLeftJoinGen;
use crate::process::record::{Record, RecordKey};

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
type BinaryResource = Vec<u8>;

pub struct IRJobAssembly {
    udf_gen: FnGenerator,
}

struct FnGenerator {
    partitioner: Arc<dyn Partitioner>,
}

impl FnGenerator {
    fn new(partitioner: Arc<dyn Partitioner>) -> Self {
        FnGenerator { partitioner }
    }

    fn gen_source(&self, opr: AlgebraOpr) -> FnGenResult<DynIter<Record>> {
        let worker_id = pegasus::get_current_worker();
        let source_opr = SourceOperator::new(
            opr,
            worker_id.local_peers as usize,
            worker_id.index,
            self.partitioner.clone(),
        )?;
        Ok(source_opr.gen_source(worker_id.index as usize)?)
    }

    fn gen_shuffle(&self, res: &BinaryResource) -> FnGenResult<RecordShuffle> {
        let p = self.partitioner.clone();
        let num_workers = pegasus::get_current_worker().local_peers as usize;
        let shuffle_key = decode::<common_pb::NameOrIdKey>(res)?;
        let record_router = RecordRouter::new(p, num_workers, shuffle_key)?;
        Ok(Box::new(record_router))
    }

    fn gen_map(&self, opr: AlgebraOpr) -> FnGenResult<RecordMap> {
        Ok(opr.gen_map()?)
    }

    fn gen_filter_map(&self, opr: AlgebraOpr) -> FnGenResult<RecordFilterMap> {
        Ok(opr.gen_filter_map()?)
    }

    fn gen_flat_map(&self, opr: AlgebraOpr) -> FnGenResult<RecordFlatMap> {
        Ok(opr.gen_flat_map()?)
    }

    fn gen_filter(&self, opr: AlgebraOpr) -> FnGenResult<RecordFilter> {
        Ok(opr.gen_filter()?)
    }

    fn gen_cmp(&self, opr: AlgebraOpr) -> FnGenResult<RecordCompare> {
        Ok(opr.gen_cmp()?)
    }

    fn gen_group(&self, opr: AlgebraOpr) -> FnGenResult<RecordGroup> {
        Ok(opr.gen_group()?)
    }

    fn gen_fold(&self, opr: AlgebraOpr) -> FnGenResult<RecordFold> {
        Ok(opr.gen_fold()?)
    }

    fn gen_subtask(&self, opr: AlgebraOpr) -> FnGenResult<RecordLeftJoin> {
        Ok(opr.gen_subtask()?)
    }

    fn gen_join(&self, opr: AlgebraOpr) -> FnGenResult<RecordJoin> {
        Ok(opr.gen_join()?)
    }

    fn gen_key(&self, opr: AlgebraOpr) -> FnGenResult<RecordKeySelector> {
        Ok(opr.gen_key()?)
    }

    fn gen_sink(&self, opr: AlgebraOpr) -> FnGenResult<Sinker> {
        Ok(opr.gen_sink()?)
    }
}

impl IRJobAssembly {
    pub fn new<D: Partitioner>(partitioner: D) -> Self {
        IRJobAssembly { udf_gen: FnGenerator::new(Arc::new(partitioner)) }
    }

    fn parse(&self, res: &BinaryResource) -> FnGenResult<AlgebraOpr> {
        let mut opr = decode::<algebra_pb::logical_plan::Operator>(res)?;
        opr.opr
            .take()
            .ok_or(ParsePbError::EmptyFieldError("algebra op is empty".to_string()).into())
    }

    fn install(
        &self, mut stream: Stream<Record>, plan: &[server_pb::OperatorDef],
    ) -> Result<Stream<Record>, BuildJobError> {
        for op in &plan[..] {
            if let Some(ref op_kind) = op.op_kind {
                match op_kind {
                    server_pb::operator_def::OpKind::Comm(comm) => match &comm.ch_kind {
                        Some(server_pb::communicate::ChKind::ToAnother(exchange)) => {
                            let router = self.udf_gen.gen_shuffle(&exchange.resource)?;
                            stream = stream.repartition(move |t| router.route(t));
                        }
                        Some(server_pb::communicate::ChKind::ToOne(_)) => {
                            stream = stream.aggregate();
                        }
                        Some(server_pb::communicate::ChKind::ToOthers(_)) => stream = stream.broadcast(),
                        None => {}
                    },
                    server_pb::operator_def::OpKind::Map(map) => {
                        let opr = self.parse(&map.resource)?;
                        let name = opr.get_name();
                        let func = self.udf_gen.gen_map(opr)?;
                        stream = stream.map_with_name(&name, move |input| func.exec(input))?;
                    }
                    server_pb::operator_def::OpKind::FilterMap(filter_map) => {
                        let opr = self.parse(&filter_map.resource)?;
                        let name = opr.get_name();
                        let func = self.udf_gen.gen_filter_map(opr)?;
                        stream = stream.filter_map_with_name(&name, move |input| func.exec(input))?;
                    }
                    server_pb::operator_def::OpKind::FlatMap(flat_map) => {
                        let opr = self.parse(&flat_map.resource)?;
                        let name = opr.get_name();
                        let func = self.udf_gen.gen_flat_map(opr)?;
                        stream = stream.flat_map_with_name(&name, move |input| func.exec(input))?;
                    }
                    server_pb::operator_def::OpKind::Filter(filter) => {
                        let opr = self.parse(&filter.resource)?;
                        let func = self.udf_gen.gen_filter(opr)?;
                        stream = stream.filter(move |input| func.test(input))?;
                    }
                    server_pb::operator_def::OpKind::Limit(n) => {
                        stream = stream.limit(n.limit)?;
                    }
                    server_pb::operator_def::OpKind::Sort(sort) => {
                        let opr = self.parse(&sort.compare)?;
                        let cmp = self.udf_gen.gen_cmp(opr)?;
                        if sort.limit > 0 {
                            stream =
                                stream.sort_limit_by(sort.limit as u32, move |a, b| cmp.compare(a, b))?;
                        } else {
                            stream = stream.sort_by(move |a, b| cmp.compare(a, b))?;
                        }
                    }
                    server_pb::operator_def::OpKind::Fold(fold) => {
                        let opr = self.parse(&fold.resource)?;
                        let fold = self.udf_gen.gen_fold(opr)?;
                        if let server_pb::AccumKind::Cnt = fold.get_accum_kind() {
                            let fold_map = fold.gen_fold_map()?;
                            stream = stream
                                .count()?
                                .map(move |cnt| fold_map.exec(cnt))?
                                .into_stream()?;
                        } else {
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
                    }
                    server_pb::operator_def::OpKind::Group(group) => {
                        let opr = self.parse(&group.resource)?;
                        let group = self.udf_gen.gen_group(opr)?;
                        let group_key = group.gen_group_key()?;
                        let group_accum = group.gen_group_accum()?;
                        let group_map = group.gen_group_map()?;
                        stream = stream
                            .key_by(move |record| group_key.get_kv(record))?
                            .fold_by_key(group_accum, || {
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

                    server_pb::operator_def::OpKind::Dedup(dedup) => {
                        let opr = self.parse(&dedup.resource)?;
                        let selector = self.udf_gen.gen_key(opr)?;
                        stream = stream
                            .key_by(move |record| selector.get_kv(record))?
                            .dedup()?
                            .map(|pair| Ok(pair.value))?;
                    }
                    server_pb::operator_def::OpKind::Merge(merge) => {
                        let (mut ori_stream, sub_stream) = stream.copied()?;
                        stream = self.install(sub_stream, &merge.tasks[0].plan[..])?;
                        for subtask in &merge.tasks[1..] {
                            let copied = ori_stream.copied()?;
                            ori_stream = copied.0;
                            stream = self
                                .install(copied.1, &subtask.plan[..])?
                                .merge(stream)?;
                        }
                    }
                    server_pb::operator_def::OpKind::Iterate(iter) => {
                        let until = if let Some(condition) = iter
                            .until
                            .as_ref()
                            .and_then(|f| Some(self.parse(&f.resource)))
                        {
                            let cond = self.udf_gen.gen_filter(condition?)?;
                            let mut until = IterCondition::new();
                            until.until(move |input| cond.test(input));
                            until.max_iters = iter.max_iters;
                            until
                        } else {
                            IterCondition::max_iters(iter.max_iters)
                        };
                        if let Some(ref iter_body) = iter.body {
                            stream = stream
                                .iterate_until(until, |start| self.install(start, &iter_body.plan[..]))?;
                        } else {
                            Err("iteration body can't be empty;")?
                        }
                    }
                    server_pb::operator_def::OpKind::IterateEmit(iter_emit) => {
                        let until = if let Some(condition) = iter_emit
                            .until
                            .as_ref()
                            .and_then(|f| Some(self.parse(&f.resource)))
                        {
                            let cond = self.udf_gen.gen_filter(condition?)?;
                            let mut until = IterCondition::new();
                            until.until(move |input| cond.test(input));
                            until.max_iters = iter_emit.max_iters;
                            until
                        } else {
                            IterCondition::max_iters(iter_emit.max_iters)
                        };
                        if let Some(ref iter_body) = iter_emit.body {
                            stream = stream.iterate_emit_until(until, EmitKind::After, |start| {
                                self.install(start, &iter_body.plan[..])
                            })?;
                        } else {
                            Err("iteration body can't be empty;")?
                        }
                    }
                    server_pb::operator_def::OpKind::Apply(sub) => {
                        let opr = self.parse(
                            &sub.join
                                .as_ref()
                                .ok_or("should have subtask_kind")?
                                .resource,
                        )?;
                        let apply_gen = self.udf_gen.gen_subtask(opr)?;
                        let join_kind = apply_gen.get_join_kind();
                        let join_func = apply_gen.gen_left_join_func()?;
                        let sub_task = sub
                            .task
                            .as_ref()
                            .ok_or(BuildJobError::Unsupported("Task is missing in Apply".to_string()))?;
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
                    }
                    server_pb::operator_def::OpKind::SegApply(_) => {
                        Err(BuildJobError::Unsupported("SegApply is not supported yet".to_string()))?
                    }
                    server_pb::operator_def::OpKind::Join(join) => {
                        let opr = self.parse(&join.resource)?;
                        let joiner = self.udf_gen.gen_join(opr)?;
                        let left_key_selector = joiner.gen_left_kv_fn()?;
                        let right_key_selector = joiner.gen_right_kv_fn()?;
                        let join_kind = joiner.get_join_kind();
                        let left_task = join
                            .left_task
                            .as_ref()
                            .ok_or("left_task is missing in merge")?;
                        let right_task = join
                            .right_task
                            .as_ref()
                            .ok_or("right_task is missing in merge")?;
                        let (left_stream, right_stream) = stream.copied()?;
                        let left_stream = self
                            .install(left_stream, &left_task.plan[..])?
                            .key_by(move |record| left_key_selector.get_kv(record))?
                            // TODO(bingqing): remove this when new keyed-join in gaia-x is ready;
                            .partition_by_key();
                        let right_stream = self
                            .install(right_stream, &right_task.plan[..])?
                            .key_by(move |record| right_key_selector.get_kv(record))?
                            // TODO(bingqing): remove this when new keyed-join in gaia-x is ready;
                            .partition_by_key();
                        stream =
                            match join_kind {
                                JoinKind::Inner => left_stream
                                    .inner_join(right_stream)?
                                    .map(|(left, right)| Ok(left.value.join(right.value, None)))?,
                                JoinKind::LeftOuter => {
                                    left_stream
                                        .left_outer_join(right_stream)?
                                        .map(|(left, right)| {
                                            let left = left.ok_or(FnExecError::unexpected_data_error(
                                                "left is None in left outer join",
                                            ))?;
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
                                        let right = right.ok_or(FnExecError::unexpected_data_error(
                                            "right is None in right outer join",
                                        ))?;
                                        if let Some(left) = left {
                                            Ok(left.value.join(right.value, None))
                                        } else {
                                            Ok(right.value)
                                        }
                                    })?,
                                JoinKind::FullOuter => left_stream.full_outer_join(right_stream)?.map(
                                    |(left, right)| match (left, right) {
                                        (Some(left), Some(right)) => Ok(left.value.join(right.value, None)),
                                        (Some(left), None) => Ok(left.value),
                                        (None, Some(right)) => Ok(right.value),
                                        (None, None) => {
                                            unreachable!()
                                        }
                                    },
                                )?,
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
                    server_pb::operator_def::OpKind::KeyBy(_) => {
                        Err(BuildJobError::Unsupported("KeyBy is not supported yet".to_string()))?
                    }
                }
            } else {
                Err("Unknown operator with empty kind;")?;
            }
        }
        Ok(stream)
    }
}

impl JobAssembly for IRJobAssembly {
    fn assemble(&self, plan: &JobDesc, worker: &mut Worker<Vec<u8>, Vec<u8>>) -> Result<(), BuildJobError> {
        worker.dataflow(move |input, output| {
            let source = decode::<server_pb::Source>(&plan.input)?;
            let source_iter = self
                .udf_gen
                .gen_source(self.parse(&source.resource)?)?;
            let source = input
                .input_from(source_iter.map(|record| {
                    let mut buf: Vec<u8> = vec![];
                    record.write_to(&mut buf).unwrap();
                    buf
                }))?
                .map(|buf| {
                    let record = Record::read_from(&mut buf.as_slice()).unwrap();
                    Ok(record)
                })?;
            let task = decode::<server_pb::TaskPlan>(&plan.plan)?;
            let stream = self.install(source, &task.plan)?;

            let sink = decode::<server_pb::Sink>(&plan.resource)?;
            let ec = self
                .udf_gen
                .gen_sink(self.parse(&sink.resource)?)?;
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
