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

use crate::generated as pb;
use crate::process::traversal::step::accum::Accumulator;
use crate::process::traversal::step::functions::{CompareFunction, EncodeFunction, KeyFunction};
use crate::process::traversal::step::*;
use crate::process::traversal::traverser::Traverser;
use crate::{str_to_dyn_error, Partitioner};
use pegasus::api::function::*;
use pegasus::api::{Collect, CorrelatedSubTask, Dedup, Filter, Fold, FoldByKey, IterCondition, Iteration, KeyBy, Limit, Map, Merge, Sink, SortBy, Source, Count, SortLimitBy};
use pegasus::result::ResultSink;
use pegasus::stream::Stream;
use pegasus::BuildJobError;
use pegasus_server::pb as server_pb;
use pegasus_server::pb::OperatorDef;
use pegasus_server::service::JobParser;
use pegasus_server::JobRequest;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;

type TraverserMap = Box<dyn MapFunction<Traverser, Traverser>>;
type TraverserFlatMap = Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>;
type TraverserFilter = Box<dyn FilterFunction<Traverser>>;
type TraverserCompare = Box<dyn CompareFunction<Traverser>>;
type TraverserLeftJoin = Box<dyn BinaryFunction<Traverser, Vec<Traverser>, Option<Traverser>>>;
type TraverserKey = Box<dyn KeyFunction<Traverser, Traverser, Traverser>>;
type TraverserEncode = Box<dyn EncodeFunction<Traverser, pb::protobuf::Result>>;
type TraverserGroupEncode =
    Box<dyn EncodeFunction<HashMap<Traverser, TraverserAccumulator>, pb::protobuf::Result>>;
type TraverserShuffle = Box<dyn RouteFunction<Traverser>>;
type BinaryResource = Vec<u8>;

pub struct GremlinJobCompiler {
    udf_gen: FnGenerator,
    num_servers: usize,
    server_index: u64,
}

struct FnGenerator {
    partitioner: Arc<dyn Partitioner>,
}

impl FnGenerator {
    fn new(partitioner: Arc<dyn Partitioner>) -> Self {
        FnGenerator { partitioner }
    }

    fn gen_source(&self, res: &BinaryResource) -> Result<DynIter<Traverser>, BuildJobError> {
        let mut step = decode::<pb::gremlin::GremlinStep>(res)?;
        let worker_id = pegasus::get_current_worker();
        let step = graph_step_from(
            &mut step,
            worker_id.local_peers as usize,
            worker_id.index,
            self.partitioner.clone(),
        )?;
        Ok(step.gen_source(worker_id.index as usize))
    }

    fn gen_shuffle(&self) -> Result<TraverserShuffle, BuildJobError> {
        let p = self.partitioner.clone();
        let num_workers = pegasus::get_current_worker().local_peers as usize;
        Ok(Box::new(Router { p, num_workers }))
    }

    fn gen_map(&self, res: &BinaryResource) -> Result<TraverserMap, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_map()?)
    }

    fn gen_flat_map(&self, res: &BinaryResource) -> Result<TraverserFlatMap, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_flat_map()?)
    }

    fn gen_filter(&self, res: &BinaryResource) -> Result<TraverserFilter, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_filter()?)
    }

    fn gen_subtask(&self, res: &BinaryResource) -> Result<TraverserLeftJoin, BuildJobError> {
        let joiner = decode::<pb::gremlin::SubTaskJoiner>(res)?;
        Ok(joiner.gen_subtask()?)
    }

    fn gen_cmp(&self, res: &BinaryResource) -> Result<TraverserCompare, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_cmp()?)
    }

    fn gen_key(&self, res: &BinaryResource) -> Result<TraverserKey, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_key()?)
    }

    fn gen_accum(&self, accum_kind: i32) -> Result<TraverserAccumulator, BuildJobError> {
        let accum_kind: server_pb::AccumKind = unsafe { std::mem::transmute(accum_kind) };
        Ok(accum_kind.gen_accum()?)
    }

    fn gen_sink(&self) -> Result<TraverserEncode, BuildJobError> {
        Ok(Box::new(TraverserSinkEncoder))
    }

    fn gen_group_sink(&self) -> Result<TraverserGroupEncode, BuildJobError> {
        Ok(Box::new(TraverserSinkEncoder))
    }
}

impl GremlinJobCompiler {
    pub fn new<D: Partitioner>(partitioner: D, num_servers: usize, server_index: u64) -> Self {
        GremlinJobCompiler {
            udf_gen: FnGenerator::new(Arc::new(partitioner)),
            num_servers,
            server_index,
        }
    }

    pub fn get_num_servers(&self) -> usize {
        self.num_servers
    }

    pub fn get_server_index(&self) -> u64 {
        self.server_index
    }

    pub fn get_partitioner(&self) -> Arc<dyn Partitioner> {
        self.udf_gen.partitioner.clone()
    }

    pub fn install(
        &self, mut stream: Stream<Traverser>, plan: &[OperatorDef],
    ) -> Result<Stream<Traverser>, BuildJobError> {
        for op in &plan[..] {
            if let Some(ref op_kind) = op.op_kind {
                match op_kind {
                    server_pb::operator_def::OpKind::Comm(comm) => match &comm.ch_kind {
                        Some(server_pb::communicate::ChKind::ToAnother(_)) => {
                            let router = self.udf_gen.gen_shuffle()?;
                            stream = stream.repartition(move |t| router.route(t));
                        }
                        Some(server_pb::communicate::ChKind::ToOne(_)) => {
                            stream = stream.aggregate();
                        }
                        Some(server_pb::communicate::ChKind::ToOthers(_)) => {
                            stream = stream.broadcast()
                        }
                        None => {}
                    },
                    server_pb::operator_def::OpKind::Map(map) => {
                        let func = self.udf_gen.gen_map(&map.resource)?;
                        stream = stream.map(move |input| func.exec(input))?;
                    }
                    server_pb::operator_def::OpKind::FlatMap(flat_map) => {
                        let func = self.udf_gen.gen_flat_map(&flat_map.resource)?;
                        stream = stream.flat_map(move |input| func.exec(input))?;
                    }
                    server_pb::operator_def::OpKind::Filter(filter) => {
                        let func = self.udf_gen.gen_filter(&filter.resource)?;
                        stream = stream.filter(move |input| func.test(input))?;
                    }
                    server_pb::operator_def::OpKind::Limit(n) => {
                        stream = stream.limit(n.limit)?;
                    }
                    server_pb::operator_def::OpKind::Order(order) => {
                        let cmp = self.udf_gen.gen_cmp(&order.compare)?;
                        if order.limit > 0 {
                            // TODO(bingqing): use sort_limit_by when pegasus is ready
                            stream = stream
                                .sort_limit_by(order.limit as u32, move |a, b| cmp.compare(a, b))?;
                        } else {
                            stream = stream.sort_by(move |a, b| cmp.compare(a, b))?;
                        }
                    }
                    server_pb::operator_def::OpKind::Fold(fold) => {
                        let accum_kind: server_pb::AccumKind =
                            unsafe { std::mem::transmute(fold.accum) };
                        match accum_kind {
                            server_pb::AccumKind::Cnt => {
                                stream = stream
                                    .count()?
                                    .map(|cnt| Ok(Traverser::Object(cnt.into())))?
                                    .into_stream()?;
                            }
                            _ => {
                                let accum = self.udf_gen.gen_accum(fold.accum)?;
                                stream = stream
                                    .fold(accum, || {
                                        move |mut accum, next| {
                                            accum.accum(next).map_err(|e| {
                                                str_to_dyn_error(&format!("accum failure: {}", e))
                                            })?;
                                            Ok(accum)
                                        }
                                    })?
                                    .map(|mut accum| Ok(accum.finalize()))?
                                    .into_stream()?;
                            }
                        }
                    }
                    server_pb::operator_def::OpKind::Group(group) => {
                        if group.unfold.is_none() {
                            Err("only support group unfold or sink for now")?;
                        }
                        // Group unfold by default here. Group sink will be processed in sink.
                        let selector = self.udf_gen.gen_key(group.resource.as_ref())?;
                        let accum = self.udf_gen.gen_accum(group.accum)?;
                        stream = stream
                            .key_by(move |trav| selector.select_key(trav))?
                            .fold_by_key(accum, || {
                                |mut accum, next| {
                                    accum.accum(next).map_err(|e| {
                                        str_to_dyn_error(&format!("group accum failure: {}", e))
                                    })?;
                                    Ok(accum)
                                }
                            })?
                            .unfold(|map| {
                                Ok(map
                                    .into_iter()
                                    .map(|(trav, mut accum)| (trav, accum.finalize())))
                            })?
                            .map(|pair| Ok(Traverser::with(pair)))?;
                    }

                    server_pb::operator_def::OpKind::Dedup(_) => {
                        // TODO: only support dedup by itself for now
                        stream = stream.dedup()?;
                    }
                    server_pb::operator_def::OpKind::Union(union) => {
                        let (mut ori_stream, sub_stream) = stream.copied()?;
                        stream = self.install(sub_stream, &union.branches[0].plan[..])?;
                        for subtask in &union.branches[1..] {
                            let copied = ori_stream.copied()?;
                            ori_stream = copied.0;
                            stream = self.install(copied.1, &subtask.plan[..])?.merge(stream)?;
                        }
                    }
                    server_pb::operator_def::OpKind::Iterate(iter) => {
                        let until = if let Some(condition) =
                            iter.until.as_ref().and_then(|f| Some(f.resource.as_ref()))
                        {
                            let cond = self.udf_gen.gen_filter(condition)?;
                            let mut until = IterCondition::new();
                            until.until(move |input| cond.test(input));
                            until.max_iters = iter.max_iters;
                            until
                        } else {
                            IterCondition::max_iters(iter.max_iters)
                        };
                        if let Some(ref iter_body) = iter.body {
                            stream = stream.iterate_until(until, |start| {
                                self.install(start, &iter_body.plan[..])
                            })?;
                        } else {
                            Err("iteration body can't be empty;")?
                        }
                    }
                    server_pb::operator_def::OpKind::Subtask(sub) => {
                        let join_func = self.udf_gen.gen_subtask(
                            sub.join.as_ref().expect("should have subtask_kind").resource.as_ref(),
                        )?;

                        if let Some(ref body) = sub.task {
                            stream = stream
                                .apply(|sub_start| {
                                    let sub_end = self
                                        .install(sub_start, &body.plan[..])?
                                        .collect::<Vec<Traverser>>()?;
                                    Ok(sub_end)
                                })?
                                .filter_map(move |(parent, sub)| join_func.exec(parent, sub))?;
                        }
                    }
                }
            } else {
                Err("Unknown operator with empty kind;")?;
            }
        }
        Ok(stream)
    }

    fn sink(
        &self, stream: Stream<Traverser>, sink: Option<&server_pb::Sink>,
        output: ResultSink<pb::protobuf::Result>,
    ) -> Result<(), BuildJobError> {
        let ec = self.udf_gen.gen_sink()?;
        if let Some(sink) = sink {
            match sink.sinker.as_ref() {
                Some(server_pb::sink::Sinker::Fold(fold)) => {
                    let accum = self.udf_gen.gen_accum(fold.accum)?;
                    let accum_kind: server_pb::AccumKind =
                        unsafe { std::mem::transmute(fold.accum) };
                    match accum_kind {
                        server_pb::AccumKind::Cnt => stream
                            .count()?
                            .map(|cnt| Ok(Traverser::Object(cnt.into())))?
                            .map(move |trav| ec.encode(trav))?
                            .sink_into(output),
                        _ => stream
                            .fold(accum, || {
                                move |mut accum, next| {
                                    accum.accum(next).map_err(|e| {
                                        str_to_dyn_error(&format!("accum failure: {}", e))
                                    })?;
                                    Ok(accum)
                                }
                            })?
                            .map(move |mut accum| ec.encode(accum.finalize()))?
                            .sink_into(output),
                    }
                }
                Some(server_pb::sink::Sinker::Group(group)) => {
                    let selector = self.udf_gen.gen_key(group.resource.as_ref())?;
                    let accum = self.udf_gen.gen_accum(group.accum)?;
                    let group_ec = self.udf_gen.gen_group_sink()?;
                    stream
                        .key_by(move |trav| selector.select_key(trav))?
                        .fold_by_key(accum, || {
                            |mut accum, next| {
                                accum.accum(next).map_err(|e| {
                                    str_to_dyn_error(&format!("accum failure: {}", e))
                                })?;
                                Ok(accum)
                            }
                        })?
                        .map(move |pair| group_ec.encode(pair))?
                        .sink_into(output)
                }
                _ => stream.map(move |trav| ec.encode(trav))?.sink_into(output),
            }
        } else {
            stream.map(move |trav| ec.encode(trav))?.sink_into(output)
        }
    }
}

impl JobParser<Traverser, pb::protobuf::Result> for GremlinJobCompiler {
    fn parse(
        &self, plan: &JobRequest, input: &mut Source<Traverser>,
        output: ResultSink<pb::protobuf::Result>,
    ) -> Result<(), BuildJobError> {
        if let Some(source) = plan.source.as_ref() {
            let source = input.input_from(self.udf_gen.gen_source(source.resource.as_ref())?)?;
            let stream = if let Some(task) = plan.plan.as_ref() {
                self.install(source, &task.plan)?
            } else {
                source
            };

            self.sink(stream, plan.sink.as_ref(), output)
        } else {
            Err("source of job not found".into())
        }
    }
}

#[inline]
fn decode<T: Message + Default>(binary: &[u8]) -> Result<T, BuildJobError> {
    Ok(T::decode(binary).map_err(|e| format!("protobuf decode failure: {}", e))?)
}
