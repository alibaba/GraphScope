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
use crate::{generated as pb, Element};
// use crate::TraverserSinkEncoder;
use pegasus::api::function::*;
use pegasus::api::{Filter, IterCondition, Iteration, Limit, Map, Sort, SortBy, Source};
use pegasus::result::ResultSink;
use pegasus::BuildJobError;
// use pegasus_common::collections::CollectionFactory;
use pegasus_common::collections::{Collection, Set};
// use pegasus_server::factory::{CompileResult, FoldFunction, GroupFunction, JobCompiler};
use crate::functions::CompareFunction;
use pegasus::stream::Stream;
use pegasus_server::pb as server_pb;
use pegasus_server::pb::channel_def::ChKind;
use pegasus_server::pb::operator_def::OpKind;
use pegasus_server::pb::{ChannelDef, OperatorDef};
use pegasus_server::service::JobParser;
use pegasus_server::JobRequest;
use prost::Message;
use std::cmp::Ordering;
use std::sync::Arc;

type TraverserMap = Box<dyn MapFunction<Traverser, Traverser>>;
type TraverserFlatMap = Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>;
type TraverserFilter = Box<dyn FilterFunction<Traverser>>;
type TraverserCompare = Box<dyn CompareFunction<Traverser>>;
type BinaryResource = Vec<u8>;

pub struct GremlinJobCompiler {
    partitioner: Arc<dyn Partitioner>,
    udf_gen: FnGenerator,
    num_servers: usize,
    server_index: u64,
}

struct FnGenerator {}

impl FnGenerator {
    fn new() -> Self {
        FnGenerator {}
    }

    fn gen_source(
        &self, res: &BinaryResource, num_servers: usize, server_index: u64,
    ) -> Result<DynIter<Traverser>, BuildJobError> {
        // let mut step = decode::<pb::gremlin::GremlinStep>(&res.resource)?;
        // let worker_id = pegasus::get_current_worker().expect("unreachable");
        // let num_workers = worker_id.peers as usize / num_servers;
        // let mut step = graph_step_from(&mut step, num_servers)?;
        // step.set_num_workers(num_workers);
        // step.set_server_index(server_index);
        // Ok(step.gen_source(Some(worker_id.index as usize)))
        todo!()
    }

    fn gen_map(&self, res: &BinaryResource) -> Result<TraverserMap, BuildJobError> {
        // let step = decode::<pb::gremlin::GremlinStep>(&res.resource)?;
        // Ok(step.gen_map()?)
        todo!()
    }

    fn gen_flat_map(&self, res: &BinaryResource) -> Result<TraverserFlatMap, BuildJobError> {
        // let step = decode::<pb::gremlin::GremlinStep>(&res.resource)?;
        // Ok(step.gen_flat_map()?)
        todo!()
    }

    fn gen_filter(&self, res: &BinaryResource) -> Result<TraverserFilter, BuildJobError> {
        // let step = decode::<pb::gremlin::GremlinStep>(&res.resource)?;
        // Ok(step.gen_filter()?)
        todo!()
    }

    fn gen_cmp(&self, res: &BinaryResource) -> Result<TraverserCompare, BuildJobError> {
        // let step = decode::<pb::gremlin::GremlinStep>(&res.resource)?;
        // Ok(step.gen_filter()?)
        todo!()
    }
}

impl GremlinJobCompiler {
    pub fn new<D: Partitioner>(partitioner: D, num_servers: usize, server_index: u64) -> Self {
        GremlinJobCompiler {
            partitioner: Arc::new(partitioner),
            udf_gen: FnGenerator::new(),
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
        self.partitioner.clone()
    }

    fn install(
        &self, mut stream: Stream<Traverser>, plan: &[OperatorDef],
    ) -> Result<Stream<Traverser>, BuildJobError> {
        for op in &plan[..] {
            match &op.ch {
                Some(ch) => match &ch.ch_kind {
                    None => {}
                    Some(server_pb::channel_def::ChKind::ToAnother(_)) => {
                        let p = self.partitioner.clone();
                        let worker_id = pegasus::get_current_worker();
                        let num_workers = worker_id.local_peers as usize / self.num_servers;
                        stream = stream.repartition(move |t| {
                            if let Some(e) = t.get_element() {
                                p.get_partition(&e.id(), num_workers)
                            } else {
                                Ok(0)
                            }
                        });
                    }
                    Some(server_pb::channel_def::ChKind::ToOne(_)) => {
                        stream = stream.aggregate();
                    }
                    Some(server_pb::channel_def::ChKind::ToLocal(_)) => {}
                    Some(server_pb::channel_def::ChKind::ToOthers(_)) => {
                        stream = stream.broadcast()
                    }
                },
                _ => {}
            }
            if let Some(ref op_kind) = op.op_kind {
                match op_kind {
                    server_pb::operator_def::OpKind::Shuffle(_) => match &op.ch {
                        Some(ch) => match &ch.ch_kind {
                            Some(server_pb::channel_def::ChKind::ToAnother(_)) => {
                                let p = self.partitioner.clone();
                                let worker_id = pegasus::get_current_worker();
                                let num_workers = worker_id.local_peers as usize / self.num_servers;
                                stream = stream.repartition(move |t| {
                                    if let Some(e) = t.get_element() {
                                        p.get_partition(&e.id(), num_workers)
                                    } else {
                                        Ok(0)
                                    }
                                });
                            }
                            _ => Err("invalid channel before exchange")?,
                        },
                        None => Err("invalid channel before exchange")?,
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
                        stream = stream.limit(n.limit)?.aggregate().limit(n.limit)?;
                    }
                    server_pb::operator_def::OpKind::Order(order) => {
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
                                todo!()
                                // stream = stream
                                //     .count()?
                                //     .aggregate_to(0)
                                //     .sum(0)?
                                //     .map_fn(|v| Ok(Traverser::count(v)))?;
                            }
                            server_pb::AccumKind::Sum => {
                                todo!()
                                // stream = stream
                                //     .sum(Traverser::sum(0))?
                                //     .aggregate_to(0)
                                //     .sum(Traverser::sum(0))?
                            }
                            server_pb::AccumKind::Max => {
                                todo!()
                                // stream = stream
                                //     .max()?
                                //     .filter_map_fn(|v| Ok(v))?
                                //     .aggregate_to(0)
                                //     .max()?
                                //     .filter_map_fn(|v| Ok(v))?;
                            }
                            server_pb::AccumKind::Min => {
                                todo!()
                                // stream = stream
                                //     .min()?
                                //     .filter_map_fn(|v| Ok(v))?
                                //     .aggregate_to(0)
                                //     .min()?
                                //     .filter_map_fn(|v| Ok(v))?;
                            }
                            server_pb::AccumKind::ToList => {
                                todo!()
                            }
                            server_pb::AccumKind::ToSet => {
                                todo!()
                            }
                            server_pb::AccumKind::Custom => {
                                todo!()
                            }
                        }
                    }

                    server_pb::operator_def::OpKind::Group(_group) => {
                        todo!()
                        // if let Some(kind) = group.fold.as_ref().and_then(|f| f.kind.as_ref()) {
                        //     match kind {
                        //         Kind::Cnt(_) => {
                        //             stream = stream
                        //                 .reduce_by_with(TraverserGroupKey, Counter::default)?
                        //                 .map_fn(|(k, v)| Ok(Traverser::group_count(k, v)))?;
                        //         }
                        //         Kind::Sum(_) => {
                        //             todo!()
                        //         }
                        //         Kind::Max(_) => {
                        //             todo!()
                        //         }
                        //         Kind::Min(_) => {
                        //             todo!()
                        //         }
                        //         Kind::List(_) => {
                        //             stream = stream
                        //                 .reduce_by(TraverserGroupKey)?
                        //                 .map_fn(|(k, v)| Ok(Traverser::group_by(k, v)))?;
                        //         }
                        //         Kind::Set(_) => {
                        //             todo!()
                        //         }
                        //         Kind::Custom(_) => {
                        //             todo!()
                        //         }
                        //     }
                        // } else {
                        //     Err("group function not found;")?;
                        // }
                    }
                    // OpKind::Unfold(_) => {
                    //     stream = stream.flat_map_fn(|t| Ok(t.unfold()))?;
                    // }
                    server_pb::operator_def::OpKind::Dedup(_) => {
                        // 1. set dedup key if needed;
                        // 2. dedup by dedup key;
                        todo!()
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
                        todo!()
                        // if let Some(ref body) = sub.task {
                        //     let sub_start = stream.clone();
                        //     let sub_end = sub_start
                        //         .fork_subtask(|start| self.install(start, &body.plan[..]))?;
                        //     match self
                        //         .udf_gen
                        //         .get_subtask_kind(sub.join.as_ref().expect("join notfound"))?
                        //     {
                        //         SubTraversalKind::GetGroupKey => {
                        //             stream = stream.join_subtask_fn(sub_end, |_info| {
                        //                 |parent, sub| {
                        //                     let mut p =
                        //                         std::mem::replace(parent, Default::default());
                        //                     p.set_group_key(sub);
                        //                     Ok(Some(p))
                        //                 }
                        //             })?;
                        //         }
                        //         SubTraversalKind::GetOrderKey => {
                        //             stream = stream.join_subtask_fn(sub_end, |_info| {
                        //                 |parent, sub| {
                        //                     let mut p =
                        //                         std::mem::replace(parent, Default::default());
                        //                     p.add_order_key(sub);
                        //                     Ok(Some(p))
                        //                 }
                        //             })?;
                        //         }
                        //         SubTraversalKind::GetDedupKey => {
                        //             stream = stream.join_subtask_fn(sub_end, |_info| {
                        //                 |parent, sub| {
                        //                     let mut p =
                        //                         std::mem::replace(parent, Default::default());
                        //                     p.set_dedup_key(sub);
                        //                     Ok(Some(p))
                        //                 }
                        //             })?;
                        //         }
                        //         SubTraversalKind::PrepareSelect(tag) => {
                        //             stream = stream.join_subtask_fn(sub_end, |_info| {
                        //                 move |parent, sub| {
                        //                     let mut p =
                        //                         std::mem::replace(parent, Default::default());
                        //                     p.add_select_result(tag, sub);
                        //                     Ok(Some(p))
                        //                 }
                        //             })?;
                        //         }
                        //         SubTraversalKind::WherePredicate => {
                        //             stream = stream.join_subtask_fn(sub_end, |_info| {
                        //                 |parent, _sub| {
                        //                     let p = std::mem::replace(parent, Default::default());
                        //                     Ok(Some(p))
                        //                 }
                        //             })?;
                        //         }
                        //         SubTraversalKind::GetGroupValue => {
                        //             stream = stream.join_subtask_fn(sub_end, |_info| {
                        //                 |parent, sub| {
                        //                     let mut p =
                        //                         std::mem::replace(parent, Default::default());
                        //                     p.update_group_value(sub);
                        //                     Ok(Some(p))
                        //                 }
                        //             })?;
                        //         }
                        //     }
                        // }
                    }
                }
            } else {
                Err("Unknown operator with empty kind;")?;
            }
        }
        Ok(stream)
    }
}

impl JobParser<Traverser, Traverser> for GremlinJobCompiler {
    fn parse(
        &self, _plan: &JobRequest, _input: &mut Source<Traverser>, _output: ResultSink<Traverser>,
    ) -> Result<(), BuildJobError> {
        unimplemented!()
    }
}
