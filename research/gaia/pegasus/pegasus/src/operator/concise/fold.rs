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

use crate::api::accum::{AccumFactory, Accumulator};
use crate::api::meta::OperatorKind;
use crate::api::notify::Notification;
use crate::api::{Fold, Range, Unary, UnaryNotify};
use crate::communication::{Aggregate, Channel, Input, Output, Pipeline};
use crate::errors::{BuildJobError, JobExecError};
use crate::stream::Stream;
use crate::{Data, Tag};
use std::collections::HashMap;

struct FoldHandle<I, O, F> {
    seed: O,
    state: HashMap<Tag, O>,
    func: F,
    _ph: std::marker::PhantomData<I>,
}

impl<I: Data, O: Data, F: Fn(&mut O, I)> FoldHandle<I, O, F> {
    pub fn new(seed: O, func: F) -> Self {
        FoldHandle { seed, state: HashMap::new(), func, _ph: std::marker::PhantomData }
    }
}

impl<I: Data, O: Data, F: FnMut(&mut O, I) + Send + 'static> UnaryNotify<I, O>
    for FoldHandle<I, O, F>
{
    type NotifyResult = Vec<O>;

    fn on_receive(&mut self, input: &mut Input<I>, _: &mut Output<O>) -> Result<(), JobExecError> {
        input.subscribe_notify();
        input.for_each_batch(|dataset| {
            if let Some(seed) = self.state.get_mut(&dataset.tag) {
                for datum in dataset.drain(..) {
                    (self.func)(seed, datum);
                }
            } else {
                let mut seed = self.seed.clone();
                for datum in dataset.drain(..) {
                    (self.func)(&mut seed, datum);
                }
                self.state.insert(dataset.tag(), seed);
            }
            Ok(())
        })?;
        Ok(())
    }

    fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
        let mut result = vec![];
        if let Some(state) = self.state.remove(&n.tag) {
            result.push(state);
        }
        result
    }
}

struct FoldAccumHandle<I, A: AccumFactory<I>> {
    accum_factory: A,
    state: HashMap<Tag, A::Target>,
    _ph: std::marker::PhantomData<I>,
}

impl<I, A: AccumFactory<I>> FoldAccumHandle<I, A> {
    pub fn new(factory: A) -> Self {
        FoldAccumHandle {
            accum_factory: factory,
            state: HashMap::new(),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<I: Data, A: AccumFactory<I> + 'static> UnaryNotify<I, A::Target> for FoldAccumHandle<I, A>
where
    A::Target: Data,
{
    type NotifyResult = Vec<A::Target>;

    fn on_receive(
        &mut self, input: &mut Input<I>, _: &mut Output<A::Target>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        input.for_each_batch(|dataset| {
            if let Some(accum) = self.state.get_mut(&dataset.tag) {
                for datum in dataset.drain(..) {
                    accum.accum(datum)?;
                }
            } else {
                let mut accum = self.accum_factory.create();
                for datum in dataset.drain(..) {
                    accum.accum(datum)?;
                }
                self.state.insert(dataset.tag(), accum);
            }
            Ok(())
        })?;
        Ok(())
    }

    fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
        let mut result = vec![];
        if let Some(state) = self.state.remove(&n.tag) {
            result.push(state);
        }
        result
    }
}

impl<I: Data> Fold<I> for Stream<I> {
    fn fold<O, C, F>(&self, seed: O, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: Fn(&mut O, I) + Send + 'static,
    {
        self.unary_with_notify("fold", channel, |meta| {
            meta.set_kind(OperatorKind::Clip);
            FoldHandle::new(seed, func)
        })
    }

    fn fold_with_accum<A>(
        &self, range: Range, accum_factory: A,
    ) -> Result<Stream<A::Target>, BuildJobError>
    where
        A: AccumFactory<I> + 'static,
        A::Target: Data,
    {
        match range {
            Range::Local => self.unary_with_notify("fold_with_accum", Pipeline, |meta| {
                meta.set_kind(OperatorKind::Clip);
                FoldAccumHandle::new(accum_factory)
            }),
            Range::Global => self.unary_with_notify("fold_with_accum", Aggregate(0), |meta| {
                meta.set_kind(OperatorKind::Clip);
                FoldAccumHandle::new(accum_factory)
            }),
        }
    }
}
