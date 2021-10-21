//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::marker::PhantomData;
use crate::{Data, ChannelId, WorkerId};
use crate::channel::{Push, Pull, DataSet, Stash};
use crate::channel::counted::{CountedPush, CountedPull, TagAwarePull};
use crate::allocate::{ ThreadPush, ThreadPull};
use crate::stream::DataflowBuilder;
use crate::channel::shuffle::{ExchangePusher, BroadcastPusher};

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum CommType {
    /// local one to one;
    Pipeline,
    /// different messages will be send to different target, one message only has one receiver;
    Shuffle,
    /// each message will be cloned and send to all;
    Broadcast,
    /// remote one to one, each message is send to a specific receiver which may either local or remote;
    Aggregate
}

pub trait Communicate<D: Data> {
    type Pusher: Push<DataSet<D>> + 'static;
    type Puller: Pull<DataSet<D>> + Stash + 'static;

    fn is_local(&self) -> bool { false }

    fn connect<B: DataflowBuilder>(self, builder: &B, id: ChannelId) -> (Self::Pusher, Self::Puller);

    fn get_comm_type(&self) -> CommType;
}

pub struct Pipeline;

impl<D: Data> Communicate<D> for Pipeline {
    type Pusher = CountedPush<D, ThreadPush<DataSet<D>>>;
    type Puller = CountedPull<D, TagAwarePull<D, ThreadPull<DataSet<D>>>>;

    fn is_local(&self) -> bool {
        true
    }

    fn connect<B: DataflowBuilder>(self, builder: &B, ch_id: ChannelId) -> (Self::Pusher, Self::Puller) {
        let worker = builder.worker_id();
        let events_buf = builder.get_event_buffer();
        let (push, pull) = builder.pipeline(ch_id);
        let push = CountedPush::new(ch_id, worker, worker, push, events_buf);
        let pull = TagAwarePull::new(pull);
        let pull = CountedPull::new(ch_id, worker, pull, events_buf);
        (push, pull)
    }

    fn get_comm_type(&self) -> CommType {
        CommType::Pipeline
    }
}

pub struct Exchange<D, F: FnMut(&D) -> u64 + 'static> {
    pub hash_func: F,
    phantom: PhantomData<D>,
}

impl<D, F: FnMut(&D) -> u64> Exchange<D, F> {
    /// Allocates a new `Exchange` pact from a distribution function.
    pub fn new(func: F) -> Exchange<D, F> {
        Exchange {
            hash_func: func,
            phantom: PhantomData,
        }
    }
}

impl<D: Data, F: FnMut(&D) -> u64 + Send + 'static> Communicate<D> for Exchange<D, F> {
    type Pusher = ExchangePusher<D, CountedPush<D, Box<dyn Push<DataSet<D>>>>, F>;
    type Puller = CountedPull<D, TagAwarePull<D, Box<dyn Pull<DataSet<D>>>>>;

    fn connect<B: DataflowBuilder>(self, builder: &B, ch_id: ChannelId) -> (Self::Pusher, Self::Puller) {
        let source = builder.worker_id();
        let (pushes, pull) = builder.allocate_channel(ch_id);
        let events_buf = builder.get_event_buffer();
        let pushes = pushes.into_iter().enumerate()
            .map(|(target, push)| {
                CountedPush::new(ch_id, source, WorkerId(source.0, target), push, events_buf)
            }).collect::<Vec<_>>();
        let push =
            ExchangePusher::new(pushes, self.hash_func, builder.batch_size());

        let pull = TagAwarePull::new(pull);
        let pull = CountedPull::new(ch_id, source, pull, events_buf);
        (push, pull)
    }

    fn get_comm_type(&self) -> CommType {
        CommType::Shuffle
    }
}

pub struct Broadcast<D> {
    ph: PhantomData<D>
}

impl<D> Broadcast<D> {
    pub fn new() -> Self {
        Broadcast {
            ph: PhantomData
        }
    }
}

impl<D: Data> Communicate<D> for Broadcast<D> {
    type Pusher = BroadcastPusher<D, CountedPush<D, Box<dyn Push<DataSet<D>>>>>;
    type Puller = CountedPull<D, TagAwarePull<D, Box<dyn Pull<DataSet<D>>>>>;

    fn connect<B: DataflowBuilder>(self, builder: &B, ch_id: ChannelId) -> (Self::Pusher, Self::Puller) {
        let (pushes, pull) = builder.allocate_channel(ch_id);
        let events_buf = builder.get_event_buffer();
        let worker = builder.worker_id();
        let pushes = pushes.into_iter().enumerate()
            .map(|(target, push)| {
                CountedPush::new(ch_id, worker, WorkerId(worker.0, target), push, events_buf)
            }).collect::<Vec<_>>();
        let push = BroadcastPusher::new(pushes);
        let pull = TagAwarePull::new(pull);
        let pull = CountedPull::new(ch_id, worker, pull, events_buf);
        (push, pull)
    }

    fn get_comm_type(&self) -> CommType {
        CommType::Broadcast
    }
}

pub struct Aggregate<D> {
    target: usize,
    ph: PhantomData<D>
}

impl<D> Aggregate<D> {
    pub fn new(target: usize) -> Self {
        Aggregate {
            target,
            ph: PhantomData
        }
    }
}

impl<D: Data> Communicate<D> for Aggregate<D> {
    type Pusher = CountedPush<D, Box<dyn Push<DataSet<D>>>>;
    type Puller = CountedPull<D, TagAwarePull<D, Box<dyn Pull<DataSet<D>>>>>;

    fn connect<B: DataflowBuilder>(self, builder: &B, ch_id: ChannelId) -> (Self::Pusher, Self::Puller) {
        let (mut pushes, pull) = builder.allocate_channel(ch_id);
        let events_buf = builder.get_event_buffer();
        let worker = builder.worker_id();
        let push = pushes.swap_remove(self.target);
        let push = CountedPush::new(ch_id, worker, WorkerId(worker.0, self.target), push, events_buf);
        let pull = TagAwarePull::new(pull);
        let pull = CountedPull::new(ch_id, worker, pull, events_buf);
        (push, pull)
    }

    fn get_comm_type(&self) -> CommType {
        CommType::Aggregate
    }
}
