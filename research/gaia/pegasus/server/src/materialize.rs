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

use crate::desc::{
    AccumKind, ChannelDesc, DedupDesc, GroupByDesc, LimitDesc, OpKind, OperatorDesc, RepeatDesc,
    SortByDesc, SubtaskDesc, UnionDesc,
};
use crate::factory::JobCompiler;
use crate::AnyData;
use pegasus::api::accum::{CountAccum, MaxAccum, MinAccum};
use pegasus::api::function::*;
use pegasus::api::{
    Binary, Count, Dedup, Exchange, Filter, Group, Iteration, Limit, LoopCondition, Map, OrderBy,
    ResultSet, SubTask, SubtaskResult, RANGES,
};
use pegasus::communication::{Aggregate, Broadcast, Channel, Pipeline};
use pegasus::stream::Stream;
use pegasus::BuildJobError;
use std::sync::Arc;

pub fn exec<D: AnyData>(
    stream: &Stream<D>, plan: &[OperatorDesc], factory: &Arc<dyn JobCompiler<D>>,
) -> Result<Stream<D>, BuildJobError> {
    if plan.is_empty() {
        Err("should be unreachable, plan length = 0;")?
    }
    let mut owned_stream = install(stream, &plan[0], factory)?;
    for op in &plan[1..] {
        match &op.op_kind {
            &OpKind::Sink => break,
            _ => {
                owned_stream = install(&owned_stream, op, factory)?;
            }
        }
    }
    Ok(owned_stream)
}

fn install<D: AnyData>(
    stream: &Stream<D>, op: &OperatorDesc, factory: &Arc<dyn JobCompiler<D>>,
) -> Result<Stream<D>, BuildJobError> {
    match &op.op_kind {
        &OpKind::Map => {
            let channel = gen_channel(&op.ch_kind, factory)?;
            let func = factory.map(op.resource.get())?;
            Ok(stream.map(channel, func)?)
        }
        &OpKind::Flatmap => {
            let channel = gen_channel(&op.ch_kind, factory)?;
            let func = factory.flat_map(op.resource.get())?;
            Ok(stream.flat_map(channel, func)?)
        }
        &OpKind::Filter => {
            let func = factory.filter(op.resource.get())?;
            Ok(stream.filter_with_fn(move |v| func.exec(v))?)
        }
        &OpKind::Exchange => match &op.ch_kind {
            ChannelDesc::Exchange(res) => {
                let route = factory.shuffle(res.get())?;
                Ok(stream.exchange(route)?)
            }
            _ => Err("invalid channel before exchange")?,
        },
        &OpKind::Limit => {
            let r = op.get_resource::<LimitDesc>().expect("parse limit resource failure;");
            Ok(stream.limit(r.range, r.size)?)
        }
        &OpKind::Count => {
            let global = op.get_resource::<u8>().expect("parse count resource failure;");
            let range = RANGES[*global as usize];
            Ok(stream.count(range)?.map(Pipeline, map!(|p| Ok(D::with(p))))?)
        }
        &OpKind::Sort => {
            let res = op.get_resource::<SortByDesc>().expect("parse sort resource failure;");
            let func = factory.compare(res.cmp.get())?;
            if let Some(limit) = res.limit {
                Ok(stream.top_by(limit as u32, res.range, func)?)
            } else {
                Ok(stream.sort_by(res.range, func)?)
            }
        }
        &OpKind::Group => {
            let res = op.get_resource::<GroupByDesc>().expect("downcast group by desc failure;");
            let key_func = factory.key(res.key_func.get())?;
            match &res.accum {
                AccumKind::Count => Ok(stream
                    .group_by_with_accum(res.range, key_func, CountAccum::new())?
                    .map(Pipeline, map!(|p| Ok(D::with(p))))?),
                AccumKind::Sum => unimplemented!(),
                AccumKind::Max(cmp) => {
                    let cmp = factory.compare(cmp.get())?;
                    Ok(stream
                        .group_by_with_accum(res.range, key_func, MaxAccum::new(cmp))?
                        .map(Pipeline, map!(|p| Ok(D::with(p))))?)
                }
                AccumKind::Min(cmp) => {
                    let cmp = factory.compare(cmp.get())?;
                    Ok(stream
                        .group_by_with_accum(res.range, key_func, MinAccum::new(cmp))?
                        .map(Pipeline, map!(|p| Ok(D::with(p))))?)
                }
                AccumKind::ToList => Ok(stream
                    .group_by(res.range, key_func)?
                    .map(Pipeline, map!(|p| Ok(D::with(p))))?),
                AccumKind::ToSet => {
                    unimplemented!()
                    // stream = stream
                    //     .group_by_with_accum(res.range, key_func, HashSetAccum::new())?
                    //     .map(Pipeline, |p| D::with(p))?;
                }
                AccumKind::Custom(acc) => {
                    let accum = factory.accumulate(acc.get())?;
                    Ok(stream
                        .group_by_with_accum(res.range, key_func, accum)?
                        .map(Pipeline, map!(|p| Ok(D::with(p))))?)
                }
            }
        }
        &OpKind::Repeat => {
            let r = op
                .resource
                .as_any_ref()
                .downcast_ref::<RepeatDesc>()
                .expect("should be unreachable, downcast RepeatDesc failure; ");
            if let Some(ref res) = r.until {
                let condition = factory.filter(res.get())?;
                let mut until = LoopCondition::<D>::max_iters(r.times);
                until.until(condition);
                Ok(stream.iterate_until(until, |start| {
                    crate::materialize::exec(&start, &r.body, factory)
                })?)
            } else {
                Ok(stream
                    .iterate(r.times, |start| crate::materialize::exec(&start, &r.body, factory))?)
            }
        }
        &OpKind::Subtask => {
            let s = op
                .resource
                .as_any_ref()
                .downcast_ref::<SubtaskDesc>()
                .expect("should be unreachable, downcast SubtaskDesc failure;");
            let forked = stream.fork_subtask(|start| exec(&start, &s.subtask, factory))?;
            if let Some(ref joiner) = s.joiner {
                let func = factory.left_join(joiner.get())?;
                Ok(stream.join_subtask(forked, move |p, s| func.exec(p, s))?)
            } else {
                Ok(forked.flat_map(
                    Pipeline,
                    flat_map!(|r: SubtaskResult<D>| {
                        let data = match r.take() {
                            ResultSet::Data(d) => d,
                            _ => vec![],
                        };
                        data.into_iter().map(|i| Ok(i))
                    }),
                )?)
            }
        }
        &OpKind::Union => {
            let union_desc = op
                .resource
                .as_any_ref()
                .downcast_ref::<UnionDesc>()
                .expect("should be unreachable, downcast UnionDesc failure;");
            if union_desc.tasks.len() < 2 {
                return Err(BuildJobError::Unsupported("union tasks length = 0;".to_owned()));
            }
            let mut s = exec(stream, &union_desc.tasks[0], factory)?;
            for task in &union_desc.tasks[1..] {
                let subtask = exec(stream, task, factory)?;
                s = s.binary("union", &subtask, Pipeline, Pipeline, |_| {
                    |input, output| {
                        input.left_for_each(|dataset| {
                            output.forward(dataset)?;
                            Ok(())
                        })?;
                        input.right_for_each(|dataset| {
                            output.forward(dataset)?;
                            Ok(())
                        })
                    }
                })?;
            }
            Ok(s)
        }
        &OpKind::Dedup => {
            let dedup_desc = op
                .resource
                .as_any_ref()
                .downcast_ref::<DedupDesc>()
                .expect("should be unreachable, downcast DedupDesc failure;");
            if let Some(ref res) = dedup_desc.set {
                let set_factory = factory.set(res.get())?;
                Ok(stream.dedup_with(dedup_desc.range, set_factory)?)
            } else {
                Err("custom set lost")?
            }
        }
        _ => unimplemented!(),
    }
}

#[inline]
fn gen_channel<D: AnyData>(
    ch: &ChannelDesc, factory: &Arc<dyn JobCompiler<D>>,
) -> Result<Channel<D>, BuildJobError> {
    match ch {
        ChannelDesc::Pipeline => Ok(Pipeline.into()),
        ChannelDesc::Exchange(res) => Ok(factory.shuffle(res.get())?.into()),
        ChannelDesc::Broadcast(None) => Ok(Broadcast.into()),
        ChannelDesc::Broadcast(Some(res)) => Ok(factory.broadcast(res.get())?.into()),
        ChannelDesc::Aggregate(target) => Ok(Aggregate(*target as u64).into()),
    }
}
