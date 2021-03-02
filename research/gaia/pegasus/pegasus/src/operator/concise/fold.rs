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

use crate::api::meta::OperatorKind;
use crate::api::notify::Notification;
use crate::api::{Fold, Unary, UnaryNotify};
use crate::communication::{Channel, Input, Output};
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

impl<I: Data, O: Data> Fold<I, O> for Stream<I> {
    fn fold<C, F>(&self, seed: O, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        C: Into<Channel<I>>,
        F: Fn(&mut O, I) + Send + 'static,
    {
        self.unary_with_notify("fold", channel, |meta| {
            meta.set_kind(OperatorKind::Clip);
            FoldHandle::new(seed, func)
        })
    }
}
