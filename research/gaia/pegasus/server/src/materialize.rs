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

use crate::factory::JobCompiler;
use crate::generated::protocol as pb;
use crate::generated::protocol::AccumKind;
use crate::AnyData;
use pegasus::api::accum::ToListAccum;
use pegasus::api::function::*;
use pegasus::api::{
    Binary, Count, Dedup, Exchange, Filter, Fold, Group, Iteration, KeyBy, Limit, LoopCondition,
    Map, OrderBy, ResultSet, SubTask, SubtaskResult, RANGES,
};
use pegasus::codec::{shade_codec, ShadeCodec};
use pegasus::communication::{Aggregate, Broadcast, Channel, Pipeline};
use pegasus::stream::Stream;
use pegasus::{never_clone, BuildJobError, NeverClone};
use pegasus_common::collections::MapFactory;
use std::sync::Arc;

pub fn exec<D: AnyData>(
    stream: &Stream<D>, plan: &[pb::OperatorDef], factory: &Arc<dyn JobCompiler<D>>,
) -> Result<Stream<D>, BuildJobError> {
    if plan.is_empty() {
        Err("should be unreachable, plan length = 0;")?
    }
    let mut owned_stream = install(stream, &plan[0], factory)?;
    for op in &plan[1..] {
        owned_stream = install(&owned_stream, op, factory)?;
    }
    Ok(owned_stream)
}

fn install<D: AnyData>(
    stream: &Stream<D>, op: &pb::OperatorDef, factory: &Arc<dyn JobCompiler<D>>,
) -> Result<Stream<D>, BuildJobError> {
    let ch = gen_channel(op.ch.as_ref(), factory)?;
    match &op.op_kind {
        Some(pb::operator_def::OpKind::Shuffle(_)) => match &op.ch {
            Some(ch) => match &ch.ch_kind {
                Some(pb::channel_def::ChKind::ToAnother(route)) => {
                    let route = factory.shuffle(&route.resource)?;
                    stream.exchange(route)
                }
                _ => Err("invalid channel before exchange")?,
            },
            None => Err("invalid channel before exchange")?,
        },
        Some(pb::operator_def::OpKind::Map(map)) => {
            let func = factory.map(&map.resource)?;
            stream.map(ch, func)
        }
        Some(pb::operator_def::OpKind::FlatMap(flatmap)) => {
            let func = factory.flat_map(&flatmap.resource)?;
            stream.flat_map(ch, func)
        }
        Some(pb::operator_def::OpKind::Filter(filter)) => {
            let func = factory.filter(&filter.resource)?;
            stream.filter(func)
        }
        Some(pb::operator_def::OpKind::Limit(limit)) => {
            let range = RANGES[limit.range as usize];
            stream.limit(range, limit.limit)
        }
        Some(pb::operator_def::OpKind::Order(order)) => {
            let range = RANGES[order.range as usize];
            let func = factory.compare(&order.compare)?;
            if order.limit > 0 {
                stream.top_by(order.limit as u32, range, func)
            } else {
                stream.sort_by(range, func)
            }
        }
        Some(pb::operator_def::OpKind::Fold(fold)) => {
            let range = RANGES[fold.range as usize];
            let accum_kind: pb::AccumKind = unsafe { std::mem::transmute(fold.accum) };
            let unfold_res = if let Some(flat_map) = &fold.unfold {
                &flat_map.resource
            } else {
                Err("unfold func lost")?
            };
            match accum_kind {
                AccumKind::Cnt => {
                    let funcs = factory.fold(&vec![], unfold_res, &vec![])?;
                    let unfold_func = funcs.fold_unfold()?;
                    stream
                        .count(range)?
                        .flat_map_with_fn(Pipeline, move |c| unfold_func.exec(Box::new(c)))
                }
                AccumKind::ToList => {
                    let funcs = factory.fold(&vec![], unfold_res, &vec![])?;
                    let unfold_func = funcs.fold_unfold()?;
                    stream
                        .fold_with_accum(range, ToListAccum::new())?
                        .flat_map_with_fn(Pipeline, move |l| unfold_func.exec(Box::new(l)))
                }
                _ => unimplemented!(),
            }
        }
        Some(pb::operator_def::OpKind::Group(group)) => {
            let range = RANGES[group.range as usize];
            let unfold_func = if let Some(flat_map) = &group.unfold {
                &flat_map.resource
            } else {
                Err("unfold func lost")?
            };
            let funcs = factory.group(&group.map, unfold_func, &vec![])?;
            let key_func = funcs.key()?;
            let map_factory = funcs.map_factory()?;
            let shade_map = ShadeMapFactory { inner: map_factory, _ph: std::marker::PhantomData };
            let unfold_func = funcs.unfold()?;
            stream
                .key_by(key_func)?
                .group_with_map(range, shade_map)?
                .flat_map_with_fn(Pipeline, move |shade| unfold_func.exec(shade.take().take()))
        }
        Some(pb::operator_def::OpKind::Iterate(iter)) => {
            let mut cond = LoopCondition::max_iters(iter.max_iters);
            if let Some(ref until) = iter.until {
                let until = factory.filter(&until.resource)?;
                cond.until(until);
            }
            let body = iter.body.as_ref().ok_or("iteration body not found")?;
            stream
                .iterate_until(cond, |start| crate::materialize::exec(&start, &body.plan, factory))
        }
        Some(pb::operator_def::OpKind::Subtask(subtask)) => {
            let body = subtask.task.as_ref().ok_or("subtask body not found")?;
            let forked = stream
                .fork_subtask(|start| crate::materialize::exec(&start, &body.plan, factory))?;
            if let Some(ref joiner) = subtask.join {
                let func = factory.left_join(&joiner.resource)?;
                stream.join_subtask(forked, move |p, s| func.exec(p, s))
            } else {
                forked.flat_map(
                    Pipeline,
                    flat_map!(|r: SubtaskResult<D>| {
                        let data = match r.take() {
                            ResultSet::Data(d) => d,
                            _ => vec![],
                        };
                        Ok(data.into_iter().map(|i| Ok(i)))
                    }),
                )
            }
        }
        Some(pb::operator_def::OpKind::Union(union)) => {
            if union.branches.len() < 2 {
                Err("invalid branch sizes in union")?;
            }

            let mut s = crate::materialize::exec(stream, &union.branches[0].plan, factory)?;
            for task in &union.branches[1..] {
                let subtask = exec(stream, &task.plan, factory)?;
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
        Some(pb::operator_def::OpKind::Dedup(dedup)) => {
            let range = RANGES[dedup.range as usize];
            let set_factory = factory.set_factory(&dedup.set)?;
            stream.dedup_with(range, set_factory)
        }

        _ => unimplemented!(),
    }
}

#[inline]
fn gen_channel<D: AnyData>(
    ch: Option<&pb::ChannelDef>, factory: &Arc<dyn JobCompiler<D>>,
) -> Result<Channel<D>, BuildJobError> {
    Ok(match ch {
        Some(ch) => match &ch.ch_kind {
            Some(pb::channel_def::ChKind::ToLocal(_)) => Pipeline.into(),
            Some(pb::channel_def::ChKind::ToAnother(route)) => {
                factory.shuffle(&route.resource)?.into()
            }
            Some(pb::channel_def::ChKind::ToOne(aggre)) => Aggregate(aggre.target as u64).into(),
            Some(pb::channel_def::ChKind::ToOthers(broadcast)) => {
                if broadcast.resource.is_empty() {
                    Broadcast.into()
                } else {
                    let route = factory.broadcast(&broadcast.resource)?;
                    route.into()
                }
            }
            None => Pipeline.into(),
        },
        _ => Pipeline.into(),
    })
}

pub struct ShadeMapFactory<D: Send + Eq, F: MapFactory<D, D>> {
    inner: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Send + Eq, F: MapFactory<D, D>> ShadeMapFactory<D, F> {
    pub fn new(inner: F) -> Self {
        ShadeMapFactory { inner, _ph: std::marker::PhantomData }
    }
}

impl<D: Send + Eq, F: MapFactory<D, D>> MapFactory<D, D> for ShadeMapFactory<D, F> {
    type Target = NeverClone<ShadeCodec<F::Target>>;

    fn create(&self) -> Self::Target {
        never_clone(shade_codec(self.inner.create()))
    }
}
