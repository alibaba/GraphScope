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

use crate::api::accum::{AccumFactory, Accumulator, ToListAccum};
use crate::api::function::*;
use crate::api::meta::OperatorMeta;
use crate::api::notify::Notification;
use crate::api::state::StateMap;
use crate::api::{Group, Map, Range, Unary, UnaryNotify};
use crate::codec::{shade_codec, ShadeCodec};
use crate::communication::{Channel, Input, Output, Pipeline};
use crate::errors::JobExecError;
use crate::operator::concise::{never_clone, NeverClone};
use crate::preclude::Aggregate;
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::rc::RcPointer;
use std::collections::HashMap;

impl<D: Data> Group<D> for Stream<D> {
    fn group_by_with_accum<F, A, O>(
        &self, range: Range, key: F, accum_gen: A,
    ) -> Result<Stream<(<F as KeyFunction<D>>::Target, O)>, BuildJobError>
    where
        O: Data,
        F: KeyFunction<D>,
        A: AccumFactory<D, O> + 'static,
        A::Target: 'static,
    {
        match range {
            Range::Local => group(self, Pipeline, key, accum_gen),
            Range::Global => {
                if accum_gen.is_associative() {
                    let accum = RcPointer::new(accum_gen);
                    group(self, Pipeline, key, accum.clone())?
                        .unary_with_notify("global group", Aggregate(0), |meta| {
                            GroupMerge::new(meta, accum)
                        })?
                        .flat_map_with_fn(Pipeline, |groups| {
                            let x = groups.take();
                            x.into_iter().map(|(k, mut v)| Ok((k, v.finalize())))
                        })
                } else {
                    group(self, Aggregate(0), key, accum_gen)
                }
            }
        }
    }

    fn group_by<F>(
        &self, range: Range, key: F,
    ) -> Result<Stream<(<F as KeyFunction<D>>::Target, Vec<D>)>, BuildJobError>
    where
        F: KeyFunction<D>,
    {
        match range {
            Range::Local => group(self, Pipeline, key, ToListAccum::new()),
            Range::Global => {
                let stream = group(self, Pipeline, key, ToListAccum::new())?;
                stream
                    .unary_with_notify("global group", Aggregate(0), |meta| {
                        GroupMerge::new(meta, ToListAccum::new())
                    })?
                    .flat_map_with_fn(Pipeline, |groups| {
                        let x = groups.take();
                        x.into_iter().map(|(k, mut v)| Ok((k, v.finalize())))
                    })
            }
        }
    }
}

struct GroupByHandler<I, O, K: KeyFunction<I>, A: AccumFactory<I, O>> {
    multi_states: StateMap<HashMap<K::Target, NeverClone<A::Target>>>,
    key_func: K,
    accum_factory: A,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, K: KeyFunction<I>, A: AccumFactory<I, O>> GroupByHandler<I, O, K, A> {
    pub fn new(meta: &OperatorMeta, key_func: K, accum_factory: A) -> Self {
        GroupByHandler {
            multi_states: StateMap::new(meta),
            key_func,
            accum_factory,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<I: Data, O: Data, K: KeyFunction<I>, A: AccumFactory<I, O> + 'static>
    UnaryNotify<I, ShadeCodec<HashMap<K::Target, NeverClone<A::Target>>>>
    for GroupByHandler<I, O, K, A>
where
    A::Target: 'static,
{
    type NotifyResult = Vec<ShadeCodec<HashMap<K::Target, NeverClone<A::Target>>>>;

    fn on_receive(
        &mut self, input: &mut Input<I>,
        _: &mut Output<ShadeCodec<HashMap<K::Target, NeverClone<A::Target>>>>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        let mut multi_states = std::mem::replace(&mut self.multi_states, StateMap::default());
        let state = multi_states.entry(&input.tag).or_insert_with(HashMap::new);
        let result = input.for_each_batch(|data_set| {
            for data in data_set.drain(..) {
                let key = self.key_func.get_key(&data);
                if let Some(accum) = state.get_mut(&key) {
                    accum.accum(data);
                } else {
                    let mut accum = never_clone(self.accum_factory.create());
                    accum.accum(data);
                    state.insert(key.into_owned(), accum);
                }
            }
            Ok(())
        });
        self.multi_states = multi_states;
        result
    }

    fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
        self.multi_states.notify(n);
        let notified = self.multi_states.extract_notified();
        assert_eq!(notified.len(), 1);
        let result = notified.remove(0).1;
        vec![shade_codec(result)]
    }
}

struct GroupMerge<D, K: Key, V, A: AccumFactory<D, V>> {
    multi_states: StateMap<HashMap<K, NeverClone<A::Target>>>,
    accum_factory: A,
    _ph: std::marker::PhantomData<(D, V)>,
}

impl<D, K: Key, V, A: AccumFactory<D, V>> GroupMerge<D, K, V, A> {
    pub fn new(meta: &OperatorMeta, accum_factory: A) -> Self {
        GroupMerge {
            multi_states: StateMap::new(meta),
            accum_factory,
            _ph: std::marker::PhantomData,
        }
    }
}

impl<D: Data, K: Key, V: Data, A: AccumFactory<D, V> + 'static>
    UnaryNotify<Pair<K, V>, ShadeCodec<HashMap<K, NeverClone<A::Target>>>>
    for GroupMerge<D, K, V, A>
where
    A::Target: 'static,
{
    type NotifyResult = Vec<ShadeCodec<HashMap<K, NeverClone<A::Target>>>>;

    fn on_receive(
        &mut self, input: &mut Input<Pair<K, V>>,
        _: &mut Output<ShadeCodec<HashMap<K, NeverClone<A::Target>>>>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        let mut multi_states = std::mem::replace(&mut self.multi_states, StateMap::default());
        let state = multi_states.entry(&input.tag).or_insert_with(HashMap::new);
        let result = input.for_each_batch(|dataset| {
            for (k, v) in dataset.drain(..) {
                if let Some(accum) = state.get_mut(&k) {
                    accum.merge(v);
                } else {
                    let mut accum = self.accum_factory.create();
                    accum.merge(v);
                    state.insert(k, never_clone(accum));
                }
            }
            Ok(())
        });
        self.multi_states = multi_states;
        result
    }

    fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
        self.multi_states.notify(n);
        let notified = self.multi_states.extract_notified();
        assert_eq!(notified.len(), 1);
        let result = notified.remove(0).1;
        vec![shade_codec(result)]
    }
}

fn group<I: Data, O: Data, C, F, A>(
    stream: &Stream<I>, channel: C, key: F, accum: A,
) -> Result<Stream<(<F as KeyFunction<I>>::Target, O)>, BuildJobError>
where
    C: Into<Channel<I>>,
    F: KeyFunction<I>,
    A: AccumFactory<I, O> + 'static,
    A::Target: 'static,
{
    stream
        .unary_with_notify("group_by", channel, |meta| GroupByHandler::new(meta, key, accum))?
        .flat_map_with_fn(Pipeline, |groups| {
            groups.take().into_iter().map(|(k, mut v)| Ok((k, v.finalize())))
        })
}
