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

use crate::process::traversal::step::*;
// use crate::process::traversal::step::{BySubJoin, HasAnyJoin};
use crate::process::traversal::traverser::Traverser;
use crate::Partitioner;
// use crate::TraverserSinkEncoder;
use pegasus::api::function::*;
use pegasus::api::{
    Collect, CorrelatedSubTask, Count, Dedup, Filter, Fold, FoldByKey, IterCondition, Iteration,
    KeyBy, Limit, Map, Reduce, Sink, SortBy, Source,
};
use pegasus::result::ResultSink;
use pegasus::BuildJobError;
// use pegasus_common::collections::CollectionFactory;

// use pegasus_server::factory::{CompileResult, FoldFunction, GroupFunction, JobCompiler};
use crate::functions::{CompareFunction, EncodeFunction, KeyFunction};
use crate::generated as pb;
use pegasus::stream::Stream;
use pegasus_server::pb as server_pb;

use pegasus_server::pb::{AccumKind, OperatorDef};
use pegasus_server::service::JobParser;
use pegasus_server::JobRequest;
use prost::Message;
use std::cmp::Ordering;

use std::collections::HashMap;
use std::sync::Arc;

type TraverserMap = Box<dyn MapFunction<Traverser, Traverser>>;
type TraverserFlatMap = Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>;
type TraverserFilter = Box<dyn FilterFunction<Traverser>>;
type TraverserCompare = Box<dyn CompareFunction<Traverser>>;
type TraverserLeftJoin = Box<dyn BinaryFunction<Traverser, Vec<Traverser>, Traverser>>;
type TraverserKey = Box<dyn KeyFunction<Traverser, Traverser, Traverser>>;
// type TraverserGroup = Box<dyn GroupFunction<Traverser, Traverser, Traverser>>;
type TraverserEncode = Box<dyn EncodeFunction<Traverser>>;
type TraverserGroupEncode = Box<dyn EncodeFunction<HashMap<Traverser, Vec<Traverser>>>>;
type TraverserGroupCountEncode = Box<dyn EncodeFunction<HashMap<Traverser, i32>>>;
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

    fn gen_subtask(&self, _res: &BinaryResource) -> Result<TraverserLeftJoin, BuildJobError> {
        todo!()
    }

    fn gen_cmp(&self, res: &BinaryResource) -> Result<TraverserCompare, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_cmp()?)
    }

    fn gen_key(&self, res: &BinaryResource) -> Result<TraverserKey, BuildJobError> {
        let step = decode::<pb::gremlin::GremlinStep>(res)?;
        Ok(step.gen_key()?)
    }

    fn gen_sink(&self) -> Result<TraverserEncode, BuildJobError> {
        Ok(Box::new(TraverserSinkEncoder))
    }

    fn gen_group_sink(&self) -> Result<TraverserGroupEncode, BuildJobError> {
        Ok(Box::new(TraverserSinkEncoder))
    }

    fn gen_group_count_sink(&self) -> Result<TraverserGroupCountEncode, BuildJobError> {
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

    fn install(
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
                        // TODO(bingqing): should set order_key for traverser, and then directly compare traverser
                        let cmp = self.udf_gen.gen_cmp(&order.compare)?;
                        if order.limit > 0 {
                            // TODO(bingqing): top-k
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
                                    .into_stream()?
                                    // TODO(bingqing): consider traverser type?
                                    .map(|v| Ok(Traverser::with(v)))?;
                            }
                            server_pb::AccumKind::Sum => {
                                stream = stream
                                    .reduce(|| {
                                        |mut sum, trav| {
                                            sum = sum + trav;
                                            Ok(sum)
                                        }
                                    })?
                                    .into_stream()?;
                            }
                            server_pb::AccumKind::Max => {
                                stream = stream
                                    .reduce(|| {
                                        move |max, curr| {
                                            let ord = max.cmp(&curr);
                                            match ord {
                                                Ordering::Less => Ok(curr),
                                                _ => Ok(max),
                                            }
                                        }
                                    })?
                                    .into_stream()?;
                            }
                            server_pb::AccumKind::Min => {
                                stream = stream
                                    .reduce(|| {
                                        move |min, curr| {
                                            let ord = min.cmp(&curr);
                                            match ord {
                                                Ordering::Greater => Ok(curr),
                                                _ => Ok(min),
                                            }
                                        }
                                    })?
                                    .into_stream()?;
                            }
                            server_pb::AccumKind::ToList => {
                                stream = stream
                                    .fold(Vec::new(), || {
                                        |mut list, trav| {
                                            list.push(trav);
                                            Ok(list)
                                        }
                                    })?
                                    .into_stream()?
                                    .map(|vec| Ok(Traverser::with(vec)))?;
                            }
                            _ => {
                                return Err(format!(
                                    "Unsupported accum_kind {:?} in fold",
                                    accum_kind
                                )
                                .into());
                            }
                        }
                    }

                    server_pb::operator_def::OpKind::Group(group) => {
                        let selector = self.udf_gen.gen_key(group.resource.as_ref())?;
                        let accum_kind: server_pb::AccumKind =
                            unsafe { std::mem::transmute(group.accum) };
                        match accum_kind {
                            AccumKind::Cnt => {
                                stream = stream
                                    .key_by(move |trav| selector.select_key(trav))?
                                    .fold_by_key(0, || |cnt, _| Ok(cnt + 1))?
                                    .unfold(|map| Ok(map.into_iter()))?
                                    // TODO(bingqing): consider traverser type
                                    .map(|pair| Ok(Traverser::with(pair)))?;
                            }
                            AccumKind::ToList => {
                                stream = stream
                                    .key_by(move |trav| selector.select_key(trav))?
                                    .fold_by_key(Vec::new(), || {
                                        |mut list, trav| {
                                            list.push(trav);
                                            Ok(list)
                                        }
                                    })?
                                    .unfold(|map| Ok(map.into_iter()))?
                                    .map(|pair| Ok(Traverser::with(pair)))?;
                            }
                            _ => {
                                return Err(format!(
                                    "Unsupported accum_kind {:?} in group",
                                    accum_kind
                                )
                                .into());
                            }
                        }
                    }

                    server_pb::operator_def::OpKind::Dedup(_) => {
                        // 1. set dedup key if needed;
                        // 2. dedup by dedup key;
                        // TODO(bingqing): dedup by itself for now
                        let selector = |trav: Traverser| (trav.clone(), trav);
                        stream = stream
                            .key_by(move |trav| Ok(selector(trav)))?
                            .dedup()?
                            .map(|pair| Ok(pair.value))?;
                    }
                    server_pb::operator_def::OpKind::Union(_) => {
                        todo!()
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
                                .map(move |(parent, sub)| join_func.exec(parent, sub))?;
                        }
                    }
                }
            } else {
                Err("Unknown operator with empty kind;")?;
            }
        }
        Ok(stream)
    }
}

impl JobParser<Traverser, Vec<u8>> for GremlinJobCompiler {
    fn parse(
        &self, plan: &JobRequest, input: &mut Source<Traverser>, output: ResultSink<Vec<u8>>,
    ) -> Result<(), BuildJobError> {
        if let Some(source) = plan.source.as_ref() {
            let source = input.input_from(self.udf_gen.gen_source(source.resource.as_ref())?)?;
            let stream = if let Some(task) = plan.plan.as_ref() {
                self.install(source, &task.plan)?
            } else {
                source
            };

            let ec = self.udf_gen.gen_sink()?;
            if let Some(sink) = plan.sink.as_ref() {
                match sink.sinker.as_ref() {
                    Some(server_pb::sink::Sinker::Fold(_fold)) => {
                        // need to first fold and then sink
                        todo!()
                    }
                    Some(server_pb::sink::Sinker::Group(group)) => {
                        let selector = self.udf_gen.gen_key(group.resource.as_ref())?;
                        // TODO: it's better like the old version, that gen_group and get_key and sink together?
                        let accum_kind: server_pb::AccumKind =
                            unsafe { std::mem::transmute(group.accum) };
                        match accum_kind {
                            AccumKind::Cnt => {
                                let group_ec = self.udf_gen.gen_group_count_sink()?;
                                stream
                                    .key_by(move |trav| selector.select_key(trav))?
                                    .fold_by_key(0, || |cnt, _| Ok(cnt + 1))?
                                    .map(move |pair| group_ec.encode(pair))?
                                    .sink_into(output)
                            }

                            AccumKind::ToList => {
                                let group_ec = self.udf_gen.gen_group_sink()?;
                                stream
                                    .key_by(move |trav| selector.select_key(trav))?
                                    .fold_by_key(Vec::new(), || {
                                        |mut list, trav| {
                                            list.push(trav);
                                            Ok(list)
                                        }
                                    })?
                                    .map(move |map|
                                      //  map.drain()
                                        group_ec.encode(map))?
                                    .sink_into(output)
                            }
                            _ => {
                                return Err(format!(
                                    "Have not support accum_kind {:?} in group yet",
                                    accum_kind
                                )
                                .into());
                            }
                        }
                    }
                    _ => stream.map(move |trav| ec.encode(trav))?.sink_into(output),
                }
            } else {
                stream.map(move |trav| ec.encode(trav))?.sink_into(output)
            }
        } else {
            Err("source of job not found".into())
        }
    }
}

#[inline]
fn decode<T: Message + Default>(binary: &[u8]) -> Result<T, BuildJobError> {
    Ok(T::decode(binary).map_err(|e| format!("protobuf decode failure: {}", e))?)
}
