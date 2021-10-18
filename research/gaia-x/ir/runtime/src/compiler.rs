//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use crate::graph::partitioner::Partitioner;
use crate::process::operator::source::source_op_from;
use crate::process::record::Record;
use ir_common::error::str_to_dyn_error;
use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::result as result_pb;
use pegasus::api::function::*;
use pegasus::api::{
    Collect, CorrelatedSubTask, Dedup, Filter, Fold, FoldByKey, IterCondition, Iteration, KeyBy,
    Limit, Map, Merge, Sink, SortBy, SortLimitBy, Source,
};
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

type RecordMap = Box<dyn MapFunction<Record, Record>>;
type RecordFlatMap = Box<dyn FlatMapFunction<Record, Record, Target = DynIter<Record>>>;
type RecordFilter = Box<dyn FilterFunction<Record>>;
type RecordLeftJoin = Box<dyn BinaryFunction<Record, Vec<Record>, Option<Record>>>;
type RecordEncode = Box<dyn MapFunction<Record, result_pb::Result>>;
type RecordShuffle = Box<dyn RouteFunction<Record>>;
type BinaryResource = Vec<u8>;

pub struct IRJobCompiler {
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

    fn gen_source(&self, res: &BinaryResource) -> Result<DynIter<Record>, BuildJobError> {
        let mut step = decode::<algebra_pb::logical_plan::Operator>(res)?;
        let worker_id = pegasus::get_current_worker();
        let step = source_op_from(
            &mut step,
            worker_id.local_peers as usize,
            worker_id.index,
            self.partitioner.clone(),
        )
        .map_err(|e| format!("{}", e))?;
        Ok(step.gen_source(worker_id.index as usize)?)
    }

    fn gen_shuffle(&self) -> Result<RecordShuffle, BuildJobError> {
        todo!()
        // let p = self.partitioner.clone();
        // let num_workers = pegasus::get_current_worker().local_peers as usize;
        // Ok(Box::new(Router { p, num_workers }))
    }

    fn gen_map(&self, _res: &BinaryResource) -> Result<RecordMap, BuildJobError> {
        todo!()
        // let step = decode::<algebra_pb::logical_plan::Operator>(res)?;
        // Ok(step.gen_map()?)
    }

    fn gen_flat_map(&self, _res: &BinaryResource) -> Result<RecordFlatMap, BuildJobError> {
        todo!()
        // let step = decode::<algebra_pb::logical_plan::Operator>(res)?;
        // Ok(step.gen_flat_map()?)
    }

    fn gen_filter(&self, _res: &BinaryResource) -> Result<RecordFilter, BuildJobError> {
        todo!()
        // let step = decode::<algebra_pb::logical_plan::Operator>(res)?;
        // Ok(step.gen_filter()?)
    }

    fn gen_subtask(&self, _res: &BinaryResource) -> Result<RecordLeftJoin, BuildJobError> {
        todo!()
    }

    fn gen_sink(&self) -> Result<RecordEncode, BuildJobError> {
        todo!()
    }
}

impl IRJobCompiler {
    pub fn new<D: Partitioner>(partitioner: D, num_servers: usize, server_index: u64) -> Self {
        IRJobCompiler {
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
        &self,
        mut stream: Stream<Record>,
        plan: &[OperatorDef],
    ) -> Result<Stream<Record>, BuildJobError> {
        for op in &plan[..] {
            if let Some(ref op_kind) = op.op_kind {
                match op_kind {
                    server_pb::operator_def::OpKind::Comm(comm) => match &comm.ch_kind {
                        Some(server_pb::communicate::ChKind::ToAnother(_)) => {
                            // TODO: shuffle by key
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
                        if order.limit > 0 {
                            stream =
                                stream.sort_limit_by(order.limit as u32, move |a, b| a.cmp(&b))?;
                        } else {
                            stream = stream.sort_by(move |a, b| a.cmp(&b))?;
                        }
                    }
                    server_pb::operator_def::OpKind::Fold(fold) => {
                        todo!()
                    }
                    server_pb::operator_def::OpKind::Group(group) => {
                        todo!()
                    }

                    server_pb::operator_def::OpKind::Dedup(_) => {
                        todo!()
                    }
                    server_pb::operator_def::OpKind::Union(union) => {
                        if union.branches.len() < 2 {
                            Err("invalid branch sizes in union")?;
                        }
                        // // TODO: engine bug here
                        // let (mut ori_stream, sub_stream) = stream.copied()?;
                        // stream = self.install(sub_stream, &union.branches[0].plan[..])?;
                        // for subtask in &union.branches[1..] {
                        //     let copied = ori_stream.copied()?;
                        //     ori_stream = copied.0;
                        //     stream = self.install(copied.1, &subtask.plan[..])?.merge(stream)?;
                        // }
                        // TODO(bingqing): remove the condition when merge is ready
                        if union.branches.len() != 2 {
                            Err("Only support union 2 branches for now")?;
                        }
                        let (ori_stream, sub_stream) = stream.copied()?;
                        stream = self.install(ori_stream, &union.branches[0].plan[..])?;
                        stream = self
                            .install(sub_stream, &union.branches[1].plan[..])?
                            .merge(stream)?;
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
                            sub.join
                                .as_ref()
                                .expect("should have subtask_kind")
                                .resource
                                .as_ref(),
                        )?;

                        if let Some(ref body) = sub.task {
                            stream = stream
                                .apply(|sub_start| {
                                    let sub_end = self
                                        .install(sub_start, &body.plan[..])?
                                        .collect::<Vec<Record>>()?;
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
}

impl JobParser<Record, result_pb::Result> for IRJobCompiler {
    fn parse(
        &self,
        plan: &JobRequest,
        input: &mut Source<Record>,
        output: ResultSink<result_pb::Result>,
    ) -> Result<(), BuildJobError> {
        todo!()
        // if let Some(source) = plan.source.as_ref() {
        //     let source = input.input_from(self.udf_gen.gen_source(source.resource.as_ref())?)?;
        //     let stream = if let Some(task) = plan.plan.as_ref() {
        //         self.install(source, &task.plan)?
        //     } else {
        //         source
        //     };
        //     let ec = self.udf_gen.gen_sink()?;
        //     stream
        //         .map(move |record| ec.encode(record))?
        //         .sink_into(output)
        // } else {
        //     Err("source of job not found".into())
        // }
    }
}

#[inline]
fn decode<T: Message + Default>(binary: &[u8]) -> Result<T, BuildJobError> {
    Ok(T::decode(binary).map_err(|e| format!("protobuf decode failure: {}", e))?)
}
