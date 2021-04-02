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

use crate::api::concise::reduce::barrier::Barrier;
use crate::api::notify::Notification;
use crate::api::state::StateMap;
use crate::api::{Range, Unary, UnaryNotify};
use crate::communication::{Aggregate, Input, Output, Pipeline};
use crate::errors::JobExecError;
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::collections::{Collection, CollectionFactory, DefaultCollectionFactory};

struct BarrierHandle<D: Data, C: CollectionFactory<D>> {
    factory: C,
    container: StateMap<C::Target>,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data, C: CollectionFactory<D>> BarrierHandle<D, C> {
    pub fn new(factory: C, container: StateMap<C::Target>) -> Self {
        BarrierHandle { factory, container, _ph: std::marker::PhantomData }
    }
}

impl<D: Data, C: CollectionFactory<D> + 'static> UnaryNotify<D, C::Target> for BarrierHandle<D, C>
where
    C::Target: Data,
{
    type NotifyResult = Vec<C::Target>;

    fn on_receive(
        &mut self, input: &mut Input<D>, _: &mut Output<C::Target>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        let factory = &self.factory;
        let container = self.container.entry(&input.tag).or_insert_with(|| factory.create());

        input.for_each_batch(|data| {
            for datum in data.drain(..) {
                container.add(datum)?;
            }
            Ok(())
        })?;

        Ok(())
    }

    fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
        self.container.notify(n);
        let notified = self.container.extract_notified();
        assert_eq!(notified.len(), 1);
        let result = notified.remove(0).1;
        vec![result]
    }
}

impl<D: Data> Barrier<D> for Stream<D> {
    fn barrier<C: Collection<D> + Data + Default>(
        &self, range: Range,
    ) -> Result<Stream<C>, BuildJobError> {
        match range {
            Range::Local => self.unary_with_notify("barrier", Pipeline, |meta| {
                let state = StateMap::new(meta);
                let factory = DefaultCollectionFactory::new();
                BarrierHandle::<D, DefaultCollectionFactory<D, C>>::new(factory, state)
            }),
            Range::Global => {
                // TODO: change aggregate to worker 0 into aggregate by tag;
                self.unary_with_notify("barrier", Aggregate(0), |meta| {
                    let state = StateMap::new(meta);
                    let factory = DefaultCollectionFactory::new();
                    BarrierHandle::<D, DefaultCollectionFactory<D, C>>::new(factory, state)
                })
            }
        }
    }

    fn barrier_with<C>(&self, range: Range, factory: C) -> Result<Stream<C::Target>, BuildJobError>
    where
        C: CollectionFactory<D> + 'static,
        C::Target: Data,
    {
        match range {
            Range::Local => self.unary_with_notify("barrier", Pipeline, |meta| {
                let state = StateMap::new(meta);
                BarrierHandle::<D, C>::new(factory, state)
            }),
            Range::Global => {
                // TODO: change aggregate to worker 0 into aggregate by tag;
                self.unary_with_notify("barrier", Aggregate(0), |meta| {
                    let state = StateMap::new(meta);
                    BarrierHandle::<D, C>::new(factory, state)
                })
            }
        }
    }
}
