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

use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pegasus_common::codec::ShadeCodec;

use crate::api::function::FnResult;
use crate::api::meta::OperatorInfo;
use crate::api::scope::ScopeDelta;
use crate::api::{Map, Unary};
use crate::communication::channel::ChannelKind;
use crate::communication::output::OutputBuilderImpl;
use crate::communication::Channel;
use crate::dataflow::{DataflowBuilder, OperatorRef};
use crate::errors::BuildJobError;
use crate::graph::{Edge, Port};
use crate::macros::route::*;
use crate::operator::{NotifiableOperator, OperatorCore};
use crate::{Data, JobConf};

#[must_use = "this `Stream` may be consumed"]
pub struct Stream<D: Data> {
    has_cycles: Vec<Arc<AtomicBool>>,
    port: OutputBuilderImpl<D>,
    ch: Channel<D>,
    dfb: DataflowBuilder,
}

impl<D: Data> Deref for Stream<D> {
    type Target = Channel<D>;

    fn deref(&self) -> &Self::Target {
        &self.ch
    }
}

impl<D: Data> DerefMut for Stream<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ch
    }
}

impl<D: Data> Stream<D> {
    pub(crate) fn create(port: OutputBuilderImpl<D>, dfb: &DataflowBuilder) -> Self {
        let ch = Channel::mount_to(&port);
        Stream { has_cycles: vec![Arc::new(Default::default())], port, ch, dfb: dfb.clone() }
    }

    pub fn get_conf(&self) -> Arc<JobConf> {
        self.dfb.config.clone()
    }

    pub fn port(&self) -> Port {
        self.port.get_port()
    }

    pub fn copied(self) -> Result<(Stream<D>, Stream<D>), BuildJobError> {
        if self.ch.is_local() {
            let copy = Stream {
                has_cycles: self.has_cycles.clone(),
                port: self.port.copy_data(),
                ch: self.ch.clone(),
                dfb: self.dfb.clone(),
            };
            Ok((self, copy))
        } else {
            let shuffled = self.unary("shuffle_clone", |_| {
                |input, output| {
                    input.for_each_batch(|dataset| {
                        output.push_batch_mut(dataset)?;
                        Ok(())
                    })
                }
            })?;

            let copy = Stream {
                has_cycles: shuffled.has_cycles.clone(),
                port: shuffled.port.copy_data(),
                ch: shuffled.ch.clone(),
                dfb: shuffled.dfb.clone(),
            };
            Ok((shuffled, copy))
        }
    }

    pub fn aggregate(mut self) -> Stream<D> {
        if self.port.get_scope_level() == 0 {
            self.ch
                .set_channel_kind(ChannelKind::Aggregate(0));
        } else {
            self.ch
                .set_channel_kind(ChannelKind::ShuffleScope);
        }
        self
    }

    pub fn repartition<F>(mut self, route: F) -> Stream<D>
    where
        F: Fn(&D) -> FnResult<u64> + Send + 'static,
    {
        self.ch
            .set_channel_kind(ChannelKind::Shuffle(box_route!(route)));
        self
    }

    pub fn broadcast(mut self) -> Stream<D> {
        self.ch.set_channel_kind(ChannelKind::Broadcast);
        self
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.ch.set_batch_size(batch_size);
    }

    pub fn transform<F, O, T>(mut self, name: &str, op_builder: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let dfb = self.dfb.clone();
        let has_cycles = self.has_cycles.clone();
        let op = self.add_operator(name, op_builder)?;

        let port = op.new_output::<O>();
        let ch = Channel::mount_to(&port);

        Ok(Stream { has_cycles, port, ch, dfb })
    }

    pub fn union_transform<R, O, F, T>(
        mut self, name: &str, mut other: Stream<R>, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        if self.get_scope_level() != other.get_scope_level() {
            BuildJobError::unsupported(format!(
                "Build {} operator failure, can't union streams with different scope level;",
                name
            ))
        } else {
            let mut op = self.add_operator(name, op_builder)?;
            let edge = other.connect(&mut op)?;
            self.dfb.add_edge(edge);
            let dfb = self.dfb.clone();
            let port = op.new_output::<O>();

            let ch = Channel::mount_to(&port);
            Ok(Stream { has_cycles: self.has_cycles.clone(), port, ch, dfb })
        }
    }

    pub fn union_notify_transform<R, O, F, T>(
        mut self, name: &str, mut other: Stream<R>, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        T: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> T,
    {
        if self.get_scope_level() != other.get_scope_level() {
            BuildJobError::unsupported(format!(
                "Build {} operator failure, can't union streams with different scope level;",
                name
            ))
        } else {
            let mut op = self.add_notify_operator(name, op_builder)?;
            let edge = other.connect(&mut op)?;
            self.dfb.add_edge(edge);
            let dfb = self.dfb.clone();
            let output = op.new_output::<O>();
            let ch = Channel::mount_to(&output);
            Ok(Stream { has_cycles: self.has_cycles.clone(), ch, port: output, dfb })
        }
    }

    pub fn binary_branch<F, L, R, T>(
        mut self, name: &str, op_builder: F,
    ) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op = self.add_operator(name, op_builder)?;
        let left = op.new_output::<L>();
        let right = op.new_output::<R>();
        let dfb = self.dfb.clone();
        let ch = Channel::mount_to(&left);
        let left = Stream { has_cycles: self.has_cycles.clone(), port: left, ch, dfb: dfb.clone() };
        let ch = Channel::mount_to(&right);
        let right = Stream { has_cycles: self.has_cycles.clone(), port: right, ch, dfb };
        Ok((left, right))
    }

    pub fn binary_branch_notify<F, L, R, T>(
        mut self, name: &str, op_builder: F,
    ) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        T: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op = self.add_notify_operator(name, op_builder)?;
        let left = op.new_output::<L>();
        let right = op.new_output::<R>();
        let dfb = self.dfb.clone();
        let ch = Channel::mount_to(&left);
        let left = Stream { has_cycles: self.has_cycles.clone(), port: left, ch, dfb: dfb.clone() };
        let ch = Channel::mount_to(&right);
        let right = Stream { has_cycles: self.has_cycles.clone(), port: right, ch, dfb };
        Ok((left, right))
    }

    pub fn sink_by<F, T>(mut self, name: &str, op_builder: F) -> Result<(), BuildJobError>
    where
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op_ref = self.add_operator(name, op_builder)?;
        self.dfb.add_sink(op_ref.get_index());
        Ok(())
    }

    pub fn feedback_to(mut self, op_index: usize) -> Result<(), BuildJobError> {
        if self.get_scope_level() == 0 {
            Err("can't create feedback stream on root scope;")?;
        }
        self.ch.add_delta(ScopeDelta::ToSibling(1));
        let mut op = self.dfb.get_operator(op_index);
        let edge = self.connect(&mut op)?;
        self.dfb.add_edge(edge);
        let cyclic = self.has_cycles.last().expect("unreachable");
        cyclic.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn enter(mut self) -> Result<Self, BuildJobError> {
        self.ch.add_delta(ScopeDelta::ToChild(1));
        self.has_cycles
            .push(Arc::new(Default::default()));
        Ok(self)
    }

    pub fn leave(mut self) -> Result<Self, BuildJobError> {
        self.ch.add_delta(ScopeDelta::ToParent(1));
        self.has_cycles.pop();
        Ok(self)
    }

    pub fn add_operator<O, F>(&mut self, name: &str, builder: F) -> Result<OperatorRef, BuildJobError>
    where
        O: OperatorCore,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let mut op = self
            .dfb
            .add_operator(name, self.get_scope_level(), builder);
        let edge = self.connect(&mut op)?;
        self.dfb.add_edge(edge);
        Ok(op)
    }

    pub fn add_notify_operator<O, F>(
        &mut self, name: &str, builder: F,
    ) -> Result<OperatorRef, BuildJobError>
    where
        O: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let mut op = self
            .dfb
            .add_notify_operator(name, self.get_scope_level(), builder);
        let edge = self.connect(&mut op)?;
        self.dfb.add_edge(edge);
        Ok(op)
    }

    fn connect(&mut self, op: &OperatorRef) -> Result<Edge, BuildJobError> {
        let target = op.next_input_port();
        let ch = std::mem::replace(&mut self.ch, Channel::default());
        let cyclic = self.has_cycles.last().unwrap().clone();
        let channel = ch.connect_to(target, cyclic, &self.dfb)?;
        let (push, pull, notify) = channel.take();
        let ch_info = push.ch_info;
        self.port.set_push(push);
        op.add_input(ch_info, pull, notify, &self.dfb.event_emitter);
        let edge = Edge::new(ch_info);
        Ok(edge)
    }
}

pub struct Single<T>(pub T);

impl<T> ShadeCodec for Single<T> {}

impl<T> Clone for Single<T> {
    fn clone(&self) -> Self {
        unimplemented!("clone not need;")
    }
}

impl<T: Debug> Debug for Single<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub struct SingleItem<D: Send + Sync + Debug + 'static> {
    pub(crate) inner: Stream<Single<D>>,
}

impl<D: Debug + Send + Sync + 'static> SingleItem<D> {
    pub fn new(stream: Stream<Single<D>>) -> Self {
        SingleItem { inner: stream }
    }

    pub fn unfold<Iter, F>(self, f: F) -> Result<Stream<Iter::Item>, BuildJobError>
    where
        Iter: Iterator + Send + 'static,
        Iter::Item: Data,
        F: Fn(D) -> FnResult<Iter> + Send + 'static,
    {
        self.inner.flat_map(move |single| f(single.0))
    }

    pub fn map<T, F>(self, f: F) -> Result<SingleItem<T>, BuildJobError>
    where
        T: Debug + Send + Sync + 'static,
        F: Fn(D) -> FnResult<T> + Send + 'static,
    {
        let stream = self
            .inner
            .map(move |single| f(single.0).map(|o| Single(o)))?;
        Ok(SingleItem::new(stream))
    }
}

impl<D: Data> SingleItem<D> {
    pub fn into_stream(self) -> Result<Stream<D>, BuildJobError> {
        self.inner.map(|single| Ok(single.0))
    }
}
