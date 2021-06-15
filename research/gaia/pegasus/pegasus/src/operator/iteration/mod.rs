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

use crate::api::meta::{OperatorKind, Priority, ScopePrior};
use crate::api::notify::Notification;
use crate::api::{EnterScope, Iteration, LeaveScope, LoopCondition, Unary, UnaryNotify};
use crate::communication::output::{OutputDelta, OutputProxy};
use crate::communication::{Broadcast, Channel, Pipeline};
use crate::communication::{Input, Output};
use crate::data::DataSet;
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::cancel::CancelSignal;
use crate::operator::CancelGuard;
use crate::stream::Stream;
use crate::{Data, Tag};
use pegasus_common::codec::*;
use std::cmp::Ordering;
use std::sync::Arc;

mod feedback;
mod merge_switch;
use crate::api::function::{FnResult, MultiRouteFunction};
use feedback::Feedback;
use merge_switch::MergeSwitch;

impl<D: Data> Iteration<D> for Stream<D> {
    fn iterate<F>(&self, max_iters: u32, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
    {
        let until = LoopCondition::max_iters(max_iters);
        self.iterate_until(until, func)
    }

    fn iterate_until<F>(&self, until: LoopCondition<D>, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>,
    {
        let max_iters = until.max_iters;
        if max_iters == 0 {
            return BuildJobError::unsupported("invalid iteration parameter: max_iters = 0;");
        }

        let (leave, into_loop, index) = {
            let enter = self.enter()?;
            let pre_loop =
                enter.unary_with_notify("pre_loop", Pipeline, |_| PreIteration::<D>::new())?;
            let index = self.index() as usize;
            let channel = Box::new(IntoIterationSync::<D>::new(index, self.peers() as usize))
                as Box<dyn MultiRouteFunction<IterationSync<D>>>;
            let mut ms = pre_loop.add_operator("merge_switch", channel, |meta| {
                meta.set_kind(OperatorKind::Map);
                meta.enable_notify();
                meta.set_scope_order(ScopePrior::Prior(Arc::new(IterationPrior)));
                Box::new(MergeSwitch::new(meta, until))
            })?;

            ms.set_cancel_guard(LoopCancelGuard::new());
            let leave_loop = enter.spawn::<D>(&mut ms);
            let into_loop = enter.spawn::<D>(&mut ms);
            (leave_loop, into_loop, ms.get_index())
        };

        let after_loop = func(into_loop)?;
        let (fb_data, fb_vote) = {
            let mut feedback = after_loop.add_operator("feedback", Pipeline, |meta| {
                meta.set_kind(OperatorKind::Map);
                meta.enable_notify();
                meta.set_output_delta(OutputDelta::Advance);
                Box::new(Feedback::<D>::new(meta.scope_depth, max_iters))
            })?;
            feedback.set_cancel_guard(FeedbackCancelGuard::new(after_loop.scope_depth));
            let fb_data = after_loop.spawn::<D>(&mut feedback);
            let fb_vote = after_loop.spawn::<u32>(&mut feedback);
            (fb_data, fb_vote)
        };

        fb_data.connect_to(index, Pipeline.into())?;
        let mut vote_ch: Channel<u32> = Broadcast.into();
        vote_ch.forbid_cancel();
        fb_vote.connect_to(index, vote_ch)?;

        leave.owned_leave()
    }
}

struct LoopCancelGuard {
    pop: Vec<Tag>,
}

impl LoopCancelGuard {
    pub fn new() -> Self {
        LoopCancelGuard { pop: Vec::new() }
    }
}

impl CancelGuard for LoopCancelGuard {
    fn cancel(&mut self, signal: CancelSignal, outputs: &[Box<dyn OutputProxy>]) -> &mut Vec<Tag> {
        if signal.port == 0 {
            if outputs[0].skip(signal.ch_index, &signal.tag) {
                outputs[1].skip(0, &signal.tag);
                self.pop.push(signal.tag.clone());
            }
        } else if signal.port == 1 {
            outputs[1].skip(signal.ch_index, &signal.tag);
        }
        &mut self.pop
    }
}

struct FeedbackCancelGuard {
    scope_depth: usize,
    pop: Vec<Tag>,
}

impl FeedbackCancelGuard {
    pub fn new(scope_depth: usize) -> Self {
        FeedbackCancelGuard { scope_depth, pop: Vec::new() }
    }
}

impl CancelGuard for FeedbackCancelGuard {
    fn cancel(&mut self, signal: CancelSignal, outputs: &[Box<dyn OutputProxy>]) -> &mut Vec<Tag> {
        if signal.port == 0 {
            outputs[0].skip(signal.ch_index, &signal.tag);
            if signal.tag.len() < self.scope_depth {
                self.pop.push(signal.take());
            } else if signal.tag.len() == self.scope_depth {
                let tag = signal.take();
                self.pop.push(tag.retreat());
            }
        }
        &mut self.pop
    }
}

struct IterationPrior;

impl Priority for IterationPrior {
    #[inline]
    fn compare(&self, a: &Tag, b: &Tag) -> Ordering {
        a.current_uncheck().cmp(&b.current_uncheck()).reverse()
    }
}

struct PreIteration<D> {
    _ph: std::marker::PhantomData<D>,
}

impl<D> PreIteration<D> {
    pub fn new() -> Self {
        PreIteration { _ph: std::marker::PhantomData }
    }
}

#[derive(Debug, Clone)]
enum IterationSync<D: Data> {
    Data(DataSet<D>),
    Sync,
}

impl<D: Data> Encode for IterationSync<D> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            IterationSync::Data(_) => {
                unreachable!("data should be pipeline;");
            }
            IterationSync::Sync => writer.write_u8(1),
        }
    }
}

impl<D: Data> Decode for IterationSync<D> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mode = reader.read_u8()?;
        if mode == 0 {
            unreachable!("data would be pipeline;");
        } else {
            Ok(IterationSync::Sync)
        }
    }
}

impl<D: Data> UnaryNotify<D, IterationSync<D>> for PreIteration<D> {
    type NotifyResult = Option<IterationSync<D>>;

    fn on_receive(
        &mut self, input: &mut Input<D>, output: &mut Output<IterationSync<D>>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        input.for_each_batch(|dataset| {
            let data = std::mem::replace(dataset, DataSet::<D>::empty());
            output.give(IterationSync::Data(data))?;
            Ok(())
        })
    }

    fn on_notify(&mut self, _: &Notification) -> Self::NotifyResult {
        Some(IterationSync::Sync)
    }
}

struct IntoIterationSync<D: Data> {
    index: Vec<u64>,
    peers: Vec<u64>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data> IntoIterationSync<D> {
    pub fn new(index: usize, len: usize) -> Self {
        let mut peers = Vec::with_capacity(len);
        for i in 0..len {
            peers.push(i as u64);
        }
        IntoIterationSync { index: vec![index as u64], peers, _ph: std::marker::PhantomData }
    }
}

impl<D: Data> MultiRouteFunction<IterationSync<D>> for IntoIterationSync<D> {
    fn route(&self, data: &IterationSync<D>) -> FnResult<&[u64]> {
        Ok(match data {
            IterationSync::Data(_) => self.index.as_slice(),
            _ => &self.peers[..],
        })
    }
}
