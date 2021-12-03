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
use std::sync::Arc;

use pegasus_common::codec::ShadeCodec;

use crate::api::function::FnResult;
use crate::api::meta::OperatorInfo;
use crate::api::scope::ScopeDelta;
use crate::api::{Map, Unary};
use crate::communication::channel::{BatchRoute, ChannelKind};
use crate::communication::output::OutputBuilderImpl;
use crate::communication::Channel;
use crate::dataflow::{DataflowBuilder, OperatorRef};
use crate::errors::BuildJobError;
use crate::graph::{Edge, Port};
use crate::macros::route::*;
use crate::operator::{NotifiableOperator, OperatorCore};
use crate::{Data, JobConf};

#[must_use = "this `Stream` must be consumed"]
pub struct Stream<D: Data> {
    /// the upstream this stream flowed;
    upstream: OutputBuilderImpl<D>,
    /// adapter to its upstream output
    ch: Channel<D>,
    /// builder of dataflow plan;
    builder: DataflowBuilder,
    /// static partitions of the stream;
    partitions: usize,
}

impl<D: Data> Stream<D> {
    pub(crate) fn new(upstream: OutputBuilderImpl<D>, dfb: &DataflowBuilder) -> Self {
        let ch = Channel::bind(&upstream);
        let partitions = dfb.worker_id.total_peers() as usize;
        Stream { upstream, ch, builder: dfb.clone(), partitions }
    }
}

impl<D: Data> Stream<D> {
    pub fn get_job_conf(&self) -> Arc<JobConf> {
        self.builder.config.clone()
    }

    pub fn get_upstream_port(&self) -> Port {
        self.upstream.get_port()
    }

    pub fn get_local_batch_size(&self) -> usize {
        self.ch.get_batch_size()
    }

    pub fn get_local_batch_capacity(&self) -> u32 {
        self.ch.get_batch_capacity()
    }

    pub fn get_scope_level(&self) -> u32 {
        self.ch.get_scope_level()
    }

    pub fn set_local_batch_size(&mut self, size: usize) -> &mut Self {
        self.ch.set_batch_size(size);
        self
    }

    pub fn set_local_batch_capacity(&mut self, cap: u32) -> &mut Self {
        self.ch.set_batch_capacity(cap);
        self
    }

    pub fn get_upstream_batch_size(&self) -> usize {
        self.upstream.get_batch_size()
    }

    pub fn get_upstream_batch_capacity(&self) -> u32 {
        self.upstream.get_batch_capacity()
    }

    pub fn set_upstream_batch_size(&mut self, size: usize) -> &mut Self {
        self.upstream.set_batch_size(size);
        self
    }

    pub fn set_upstream_batch_capacity(&mut self, cap: u32) -> &mut Self {
        self.upstream.set_batch_capacity(cap);
        self
    }

    pub fn get_partitions(&self) -> usize {
        self.partitions
    }
}

impl<D: Data> Stream<D> {
    // Special case:
    // aggregate().enter() :
    // aggregate().leave() :
    pub fn aggregate(mut self) -> Stream<D> {
        if self.partitions > 1 {
            self.ch.set_channel_kind(ChannelKind::Aggregate);
            self.partitions = 1;
        }
        self
    }

    pub fn repartition<F>(mut self, route: F) -> Stream<D>
    where
        F: Fn(&D) -> FnResult<u64> + Send + 'static,
    {
        self.partitions = self.builder.worker_id.total_peers() as usize;
        self.ch
            .set_channel_kind(ChannelKind::Shuffle(box_route!(route)));
        self
    }

    pub fn broadcast(mut self) -> Stream<D> {
        self.partitions = self.builder.worker_id.total_peers() as usize;
        self.ch.set_channel_kind(ChannelKind::Broadcast);
        self
    }
}

impl<D: Data> Stream<D> {
    pub fn copied(self) -> Result<(Stream<D>, Stream<D>), BuildJobError> {
        if self.ch.is_pipeline() {
            let copy = Stream {
                upstream: self.upstream.copy_data(),
                ch: self.ch.clone(),
                builder: self.builder.clone(),
                partitions: self.partitions,
            };
            Ok((self, copy))
        } else {
            // stream.repartition(..).copied()
            // stream.aggregate().copied()
            // stream.broadcast().copied()
            let shuffled = self.forward("shuffle_clone")?;
            let copy = Stream {
                upstream: shuffled.upstream.copy_data(),
                ch: shuffled.ch.clone(),
                builder: shuffled.builder.clone(),
                partitions: shuffled.partitions,
            };
            Ok((shuffled, copy))
        }
    }

    pub(crate) fn sync_state(mut self) -> Stream<D> {
        if self.ch.is_pipeline() {
            let target = self.builder.worker_id.index;
            self.ch
                .set_channel_kind(ChannelKind::BatchShuffle(BatchRoute::AllToOne(target)));
        }
        self
    }

    pub fn transform<F, O, T>(mut self, name: &str, op_builder: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let dfb = self.builder.clone();
        let partitions = self.partitions;
        let op = self.add_operator(name, op_builder)?;
        let port = op.new_output::<O>();
        let ch = Channel::bind(&port);

        Ok(Stream { upstream: port, ch, builder: dfb, partitions })
    }

    pub fn transform_notify<F, O, T>(
        mut self, name: &str, op_builder: F,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        T: NotifiableOperator,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let dfb = self.builder.clone();
        let partitions = self.partitions;
        let op = self.add_notify_operator(name, op_builder)?;
        let port = op.new_output::<O>();
        let ch = Channel::bind(&port);

        Ok(Stream { upstream: port, ch, builder: dfb, partitions })
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
            self.builder.add_edge(edge);
            let dfb = self.builder.clone();
            let partitions = std::cmp::max(self.partitions, other.partitions);
            let port = op.new_output::<O>();

            let ch = Channel::bind(&port);
            Ok(Stream { upstream: port, ch, builder: dfb, partitions })
        }
    }

    pub fn union_transform_notify<R, O, F, T>(
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
            self.builder.add_edge(edge);
            let dfb = self.builder.clone();
            let partitions = std::cmp::max(self.partitions, other.partitions);
            let output = op.new_output::<O>();
            let ch = Channel::bind(&output);
            Ok(Stream { ch, upstream: output, builder: dfb, partitions })
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
        let dfb = self.builder.clone();
        let partitions = self.partitions;
        let ch = Channel::bind(&left);
        let left = Stream { upstream: left, ch, builder: dfb.clone(), partitions };
        let ch = Channel::bind(&right);
        let right = Stream { upstream: right, ch, builder: dfb, partitions };
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
        let dfb = self.builder.clone();
        let partitions = self.partitions;
        let ch = Channel::bind(&left);
        let left = Stream { upstream: left, ch, builder: dfb.clone(), partitions };
        let ch = Channel::bind(&right);
        let right = Stream { upstream: right, ch, builder: dfb, partitions };
        Ok((left, right))
    }

    pub fn sink_by<F, T>(mut self, name: &str, op_builder: F) -> Result<(), BuildJobError>
    where
        T: OperatorCore,
        F: FnOnce(&OperatorInfo) -> T,
    {
        let op_ref = self.add_operator(name, op_builder)?;
        self.builder.add_sink(op_ref.get_index());
        Ok(())
    }

    pub fn feedback_to(mut self, op_index: usize) -> Result<(), BuildJobError> {
        if self.get_scope_level() == 0 {
            Err("can't create feedback stream on root scope;")?;
        }
        let r = self.ch.add_delta(ScopeDelta::ToSibling(1));
        assert!(r.is_none());
        let mut op = self.builder.get_operator(op_index);
        let edge = self.connect(&mut op)?;
        self.builder.add_edge(edge);
        Ok(())
    }

    pub fn enter(self) -> Result<Self, BuildJobError> {
        let mut synced = self.sync_state();
        if synced
            .ch
            .add_delta(ScopeDelta::ToChild(1))
            .is_some()
        {
            let mut adapter = synced.forward("enter_adapter")?;
            adapter.ch.add_delta(ScopeDelta::ToChild(1));
            Ok(adapter)
        } else {
            Ok(synced)
        }
    }

    pub fn leave(mut self) -> Result<Self, BuildJobError> {
        if !self.ch.is_pipeline()
            || self
                .ch
                .add_delta(ScopeDelta::ToParent(1))
                .is_some()
        {
            self.forward("leave_adapter")?.leave()
        } else {
            Ok(self)
        }
    }

    pub fn add_operator<O, F>(&mut self, name: &str, builder: F) -> Result<OperatorRef, BuildJobError>
    where
        O: OperatorCore,
        F: FnOnce(&OperatorInfo) -> O,
    {
        let mut op = self
            .builder
            .add_operator(name, self.get_scope_level(), builder);
        let edge = self.connect(&mut op)?;
        self.builder.add_edge(edge);
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
            .builder
            .add_notify_operator(name, self.get_scope_level(), builder);
        let edge = self.connect(&mut op)?;
        self.builder.add_edge(edge);
        Ok(op)
    }

    fn connect(&mut self, op: &OperatorRef) -> Result<Edge, BuildJobError> {
        let target = op.next_input_port();
        let ch = std::mem::replace(&mut self.ch, Channel::default());
        let channel = ch.connect_to(target, &self.builder)?;
        let (push, pull, notify) = channel.take();
        let ch_info = push.ch_info;
        self.upstream.set_push(push);
        op.add_input(ch_info, pull, notify, &self.builder.event_emitter);
        let edge = Edge::new(ch_info);
        Ok(edge)
    }

    fn forward(self, name: &str) -> Result<Stream<D>, BuildJobError> {
        self.unary(name, |_info| {
            |input, output| {
                input.for_each_batch(|batch| {
                    output.push_batch_mut(batch)?;
                    Ok(())
                })
            }
        })
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

    pub fn map<T, F>(mut self, f: F) -> Result<SingleItem<T>, BuildJobError>
    where
        T: Debug + Send + Sync + 'static,
        F: Fn(D) -> FnResult<T> + Send + 'static,
    {
        self.inner.set_upstream_batch_size(1);
        self.inner.set_upstream_batch_capacity(1);
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
