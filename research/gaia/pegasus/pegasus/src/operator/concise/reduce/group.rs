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

use crate::api::accum::{AccumFactory, Accumulator, ToVecAccum};
use crate::api::function::*;
use crate::api::group::KeyBy;
use crate::api::meta::OperatorMeta;
use crate::api::notify::Notification;
use crate::api::state::StateMap;
use crate::api::{Group, Map, Range, Unary, UnaryNotify};
use crate::communication::{Input, Output, Pipeline};
use crate::errors::JobExecError;
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::collections::{Map as MapContainer, MapFactory};
use pegasus_common::downcast::AsAny;
use std::collections::HashMap;
use std::hash::Hash;

impl<D: Data + Keyed> Group<D> for Stream<D> {
    fn group_by(
        &self, range: Range,
    ) -> Result<Stream<HashMap<D::Key, Vec<D::Value>>>, BuildJobError>
    where
        D::Key: Data + Hash + Eq + Partition,
        D::Value: Data + AsAny,
    {
        self.group_with_accum(range, ToVecAccum::new())
    }

    fn group_with_map<F>(
        &self, range: Range, map_factory: F,
    ) -> Result<Stream<F::Target>, BuildJobError>
    where
        D::Key: Send + Eq + Partition,
        D::Value: Send,
        F: MapFactory<D::Key, D::Value> + 'static,
        F::Target: Data,
    {
        match range {
            Range::Local => self.unary_with_notify("group_by", Pipeline, |meta| {
                GroupByHandler::new(meta, map_factory)
            }),
            Range::Global => {
                let route = box_route!(move |t: &D| {
                    if let Ok(k) = t.get_key() {
                        k.get_partition().unwrap_or(0)
                    } else {
                        0
                    }
                });
                self.unary_with_notify("group_by", route, |meta| {
                    GroupByHandler::new(meta, map_factory)
                })
            }
        }
    }

    fn group_with_accum<A>(
        &self, range: Range, accum_factory: A,
    ) -> Result<Stream<HashMap<D::Key, A::Target>>, BuildJobError>
    where
        A: AccumFactory<D::Value> + 'static,
        A::Target: Data + 'static,
        D::Key: Data + Hash + Eq + Partition,
    {
        match range {
            Range::Local => self.unary_with_notify("group_with_accum", Pipeline, |meta| {
                GroupAccumHandler::new(meta, accum_factory)
            }),
            Range::Global => {
                let route = box_route!(move |t: &D| {
                    if let Ok(k) = t.get_key() {
                        k.get_partition().unwrap_or(0)
                    } else {
                        0
                    }
                });
                self.unary_with_notify("group_with_accum", route, |meta| {
                    GroupAccumHandler::new(meta, accum_factory)
                })
            }
        }
    }
}

impl<D: Data> KeyBy<D> for Stream<D> {
    fn key_by<F>(&self, key_selector: F) -> Result<Stream<Pair<F::Key, D>>, BuildJobError>
    where
        F::Key: Data,
        F: KeyFunction<D>,
    {
        self.map_with_fn(Pipeline, move |v| {
            let key = key_selector.select_key(&v)?;
            Ok((Some(key), Some(v)))
        })
    }
}

struct GroupByHandler<I: Keyed, F, M> {
    map_factory: F,
    multi_states: StateMap<M>,
    _ph: std::marker::PhantomData<I>,
}

impl<I: Keyed, F, M> GroupByHandler<I, F, M> {
    pub fn new(meta: &OperatorMeta, map_factory: F) -> Self {
        GroupByHandler {
            map_factory,
            multi_states: StateMap::new(meta),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<I: Data + Keyed, F> UnaryNotify<I, F::Target> for GroupByHandler<I, F, F::Target>
where
    I::Key: Eq + Send,
    I::Value: Send,
    F: MapFactory<I::Key, I::Value> + 'static,
    F::Target: Data,
{
    type NotifyResult = Vec<F::Target>;

    fn on_receive(
        &mut self, input: &mut Input<I>, _: &mut Output<F::Target>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        let map_factory = &self.map_factory;
        let mut multi_states = std::mem::replace(&mut self.multi_states, StateMap::default());
        let state = multi_states.entry(&input.tag).or_insert_with(|| map_factory.create());
        let result = input.for_each_batch(|data_set| {
            for mut data in data_set.drain(..) {
                let key = data.take_key()?;
                let value = data.take_value()?;
                state.insert(key, value);
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
        vec![result]
    }
}

struct GroupAccumHandler<I: Keyed, A: AccumFactory<I::Value>, M> {
    accum_factory: A,
    multi_states: StateMap<M>,
    _ph: std::marker::PhantomData<I>,
}

impl<I: Keyed, A: AccumFactory<I::Value>, M> GroupAccumHandler<I, A, M> {
    pub fn new(meta: &OperatorMeta, accum_factory: A) -> Self {
        GroupAccumHandler {
            accum_factory,
            multi_states: StateMap::new(meta),
            _ph: std::marker::PhantomData,
        }
    }
}

impl<I: Data + Keyed, A: AccumFactory<I::Value> + 'static>
    UnaryNotify<I, HashMap<I::Key, A::Target>>
    for GroupAccumHandler<I, A, HashMap<I::Key, A::Target>>
where
    I::Key: Hash + Eq + Data,
    A::Target: Data,
{
    type NotifyResult = Vec<HashMap<I::Key, A::Target>>;

    fn on_receive(
        &mut self, input: &mut Input<I>, _: &mut Output<HashMap<I::Key, A::Target>>,
    ) -> Result<(), JobExecError> {
        input.subscribe_notify();
        let mut multi_states = std::mem::replace(&mut self.multi_states, StateMap::default());
        let state = multi_states.entry(&input.tag).or_insert_with(HashMap::new);
        let result = input.for_each_batch(|data_set| {
            for mut data in data_set.drain(..) {
                let key = data.take_key()?;
                if let Some(accum) = state.get_mut(&key) {
                    accum.accum(data.take_value()?)?;
                } else {
                    let mut accum = self.accum_factory.create();
                    accum.accum(data.take_value()?)?;
                    state.insert(key, accum);
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
        vec![result]
    }
}
