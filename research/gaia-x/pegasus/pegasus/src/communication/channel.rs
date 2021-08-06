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

use crate::api::function::RouteFunction;
use crate::channel_id::{ChannelId, ChannelInfo};
use crate::communication::decorator::aggregate::AggregateBatchPush;
use crate::communication::decorator::broadcast::BroadcastBatchPush;
use crate::communication::decorator::buffered::BufferedPush;
use crate::communication::decorator::exchange::ExchangeByScopePush;
use crate::communication::decorator::{
    evented::ControlPush, exchange::ExchangeMiniBatchPush, DataPush, LocalMiniBatchPush,
};
use crate::data::{Data, DataSet};
use crate::data_plane::{GeneralPull, GeneralPush};
use crate::dataflow::DataflowBuilder;
use crate::errors::BuildJobError;
use crate::graph::Port;
use pegasus_common::buffer::Batch;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub enum ChannelKind<T: Data> {
    Pipeline,
    Shuffle(Box<dyn RouteFunction<T>>),
    ShuffleScope,
    Broadcast,
    Aggregate(u32),
}

impl<T: Data> ChannelKind<T> {
    pub fn is_local(&self) -> bool {
        match self {
            ChannelKind::Pipeline => true,
            _ => false,
        }
    }
}

pub struct Channel<T: Data> {
    kind: ChannelKind<T>,
    allow_cancel: bool,
    batch_size: usize,
}

pub(crate) struct MaterializedChannel<T: Data> {
    pub ch_info: ChannelInfo,
    push: DataPush<T>,
    pull: GeneralPull<DataSet<T>>,
    notify: Option<GeneralPush<DataSet<T>>>,
}

impl<T: Data> MaterializedChannel<T> {
    pub fn take(
        self,
    ) -> (ChannelInfo, DataPush<T>, GeneralPull<DataSet<T>>, Option<GeneralPush<DataSet<T>>>) {
        (self.ch_info, self.push, self.pull, self.notify)
    }
}

impl<T: Data> Channel<T> {
    pub fn new(kind: ChannelKind<T>) -> Self {
        Channel { kind, allow_cancel: true, batch_size: u32::MAX as usize }
    }

    pub fn forbid_cancel(&mut self) {
        self.allow_cancel = false;
    }

    pub fn set_batch_size(&mut self, batch_size: usize) {
        if batch_size > 0 {
            self.batch_size = batch_size;
        }
    }

    pub(crate) fn materialize(
        self, source: Port, target: Port, scope_level: usize, has_cycles: Arc<AtomicBool>,
        dfb: &DataflowBuilder,
    ) -> Result<MaterializedChannel<T>, BuildJobError> {
        let index = dfb.next_channel_index();
        let id = ChannelId { job_seq: dfb.config.job_id as u64, index };
        let b = std::cmp::min(dfb.config.batch_size as usize, self.batch_size);
        let batch_size = Batch::<T>::with_capacity(b).capacity();
        if index > 1 {
            debug_worker!(
                "channel[{}] : config batch size = {}, runtime batch size = {}, scope_level = {}",
                index,
                b,
                batch_size,
                scope_level
            );
        }
        let capacity = dfb.config.output_capacity as usize;
        let scope_cap = dfb.config.scope_capacity as usize;

        if dfb.worker_id.total_peers() == 1 {
            return Self::build_pipeline(source, target, scope_level, id, batch_size, capacity, scope_cap);
        }

        match self.kind {
            ChannelKind::Pipeline => {
                Self::build_pipeline(source, target, scope_level, id, batch_size, capacity, scope_cap)
            }
            ChannelKind::Shuffle(r) => {
                let (mut raw, pull) = super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let worker_index = crate::worker_id::get_current_worker().index as usize;
                let notify = raw.swap_remove(worker_index);
                let ch_info = ChannelInfo::new(id, scope_level, raw.len(), raw.len(), source, target);
                let pushes =
                    decorate(scope_level, batch_size, capacity, scope_cap, has_cycles, ch_info, raw, &dfb);
                let push = ExchangeMiniBatchPush::new(ch_info, pushes, r);
                Ok(MaterializedChannel {
                    ch_info,
                    push: DataPush::Shuffle(push),
                    pull: pull.into(),
                    notify: Some(notify),
                })
            }
            ChannelKind::Broadcast => {
                let (mut raw, pull) = super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let worker_index = crate::worker_id::get_current_worker().index as usize;
                let notify = raw.swap_remove(worker_index);
                let ch_info = ChannelInfo::new(id, scope_level, raw.len(), raw.len(), source, target);
                let push = BroadcastBatchPush::new(ch_info, has_cycles, raw, &dfb.event_emitter);
                let push = BufferedPush::new(scope_level, batch_size, scope_cap, capacity, push);
                Ok(MaterializedChannel {
                    ch_info,
                    push: DataPush::Broadcast(push),
                    pull: pull.into(),
                    notify: Some(notify),
                })
            }
            ChannelKind::Aggregate(worker) => {
                let (mut raw, pull) = super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let worker_index = crate::worker_id::get_current_worker().index as usize;
                let notify = raw.swap_remove(worker_index);
                let ch_info = ChannelInfo::new(id, scope_level, raw.len(), 1, source, target);
                let source = dfb.worker_id.index;
                let push = AggregateBatchPush::new(
                    ch_info,
                    source,
                    worker,
                    has_cycles,
                    raw,
                    dfb.event_emitter.clone(),
                );
                let push = BufferedPush::new(scope_level, batch_size, scope_cap, capacity, push);
                Ok(MaterializedChannel {
                    ch_info,
                    push: DataPush::Aggregate(push),
                    pull: pull.into(),
                    notify: Some(notify),
                })
            }
            ChannelKind::ShuffleScope => {
                let (mut raw, pull) = super::build_channel::<DataSet<T>>(index, &dfb.config)?.take();
                let worker_index = crate::worker_id::get_current_worker().index as usize;
                let notify = raw.swap_remove(worker_index);
                let ch_info = ChannelInfo::new(id, scope_level, raw.len(), raw.len(), source, target);
                let push = ExchangeByScopePush::new(ch_info, has_cycles, raw, &dfb.event_emitter);
                let push = BufferedPush::new(scope_level, batch_size, scope_cap, capacity, push);
                Ok(MaterializedChannel {
                    ch_info,
                    push: DataPush::ScopeShuffle(push),
                    pull: pull.into(),
                    notify: Some(notify),
                })
            }
        }
    }

    fn build_pipeline(
        source: Port, target: Port, scope_level: usize, id: ChannelId, batch_size: usize, capacity: usize,
        scope_cap: usize,
    ) -> Result<MaterializedChannel<T>, BuildJobError> {
        let (tx, rx) = crate::data_plane::pipeline::<DataSet<T>>(id);
        let ch_info = ChannelInfo::new(id, scope_level, 1, 1, source, target);
        let push = LocalMiniBatchPush::new(ch_info, tx);
        let push = BufferedPush::new(scope_level, batch_size, scope_cap, capacity, push);

        Ok(MaterializedChannel { ch_info, push: DataPush::Pipeline(push), pull: rx.into(), notify: None })
    }
}

#[inline]
fn decorate<T: Data>(
    scope_level: usize, batch_size: usize, capacity: usize, scope_cap: usize, has_cycles: Arc<AtomicBool>,
    ch_info: ChannelInfo, raw: Vec<GeneralPush<DataSet<T>>>, dfb: &DataflowBuilder,
) -> Vec<BufferedPush<T, ControlPush<T>>> {
    let mut pushes = Vec::with_capacity(raw.len());
    let source = dfb.worker_id.index;
    for (idx, p) in raw.into_iter().enumerate() {
        let has_cycles = has_cycles.clone();
        let push = ControlPush::new(ch_info, source, idx as u32, has_cycles, p, dfb.event_emitter.clone());
        let push = BufferedPush::new(scope_level, batch_size, scope_cap, capacity, push);
        pushes.push(push);
    }
    pushes
}

pub struct Pipeline;

impl<T: Data> From<Pipeline> for Channel<T> {
    fn from(_: Pipeline) -> Self {
        Channel::new(ChannelKind::Pipeline)
    }
}

impl<T: Data, R: RouteFunction<T>> From<Box<R>> for Channel<T> {
    fn from(route: Box<R>) -> Self {
        let kind = ChannelKind::Shuffle(route as Box<dyn RouteFunction<T>>);
        Channel::new(kind)
    }
}

impl<T: Data> From<Box<dyn RouteFunction<T>>> for Channel<T> {
    fn from(route: Box<dyn RouteFunction<T>>) -> Self {
        Channel::new(ChannelKind::Shuffle(route))
    }
}

pub struct Broadcast;

impl<T: Data> From<Broadcast> for Channel<T> {
    fn from(_: Broadcast) -> Self {
        Channel::new(ChannelKind::Broadcast)
    }
}

pub struct Aggregate(pub u32);

impl<T: Data> From<Aggregate> for Channel<T> {
    fn from(a: Aggregate) -> Self {
        let kind = ChannelKind::Aggregate(a.0);
        Channel::new(kind)
    }
}
