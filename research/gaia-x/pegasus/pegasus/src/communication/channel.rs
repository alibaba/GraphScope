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
use crate::api::scope::{MergedScopeDelta, ScopeDelta};
use crate::channel_id::{ChannelId, ChannelInfo};
use crate::communication::buffer::ScopeBufferPool;
use crate::communication::cancel::{CancelHandle, SingleConsCancel};
use crate::communication::decorator::aggregate::AggregateBatchPush;
use crate::communication::decorator::broadcast::BroadcastBatchPush;
use crate::communication::decorator::evented::EventEmitPush;
use crate::communication::decorator::exchange::{ExchangeByScopePush, ExchangeMicroBatchPush};
use crate::communication::decorator::{LocalMicroBatchPush, MicroBatchPush};
use crate::communication::output::{ChannelPush, OutputBuilderImpl};
use crate::data::MicroBatch;
use crate::data_plane::{GeneralPull, GeneralPush};
use crate::dataflow::DataflowBuilder;
use crate::event::emitter::EventEmitter;
use crate::graph::Port;
use crate::BuildJobError;
use crate::Data;

pub enum ChannelKind<T: Data> {
    Pipeline(bool),
    Shuffle(Box<dyn RouteFunction<T>>),
    ShuffleScope,
    Broadcast,
    Aggregate(u32),
}

impl<T: Data> ChannelKind<T> {
    pub fn is_pipeline(&self) -> bool {
        match self {
            ChannelKind::Pipeline(_local) => true,
            _ => false,
        }
    }
}

pub struct Channel<T: Data> {
    /// the output port of an operator this channel bind to;
    source: Port,
    /// the hint of size of each batch this channel expect to deliver;
    batch_size: usize,
    /// the hint of size indicates how much batches of a scope can be delivered at same time;
    batch_capacity: u32,
    /// the hit of size indicates how much scopes can be delivered at same time;
    scope_capacity: u32,
    /// describe changes of data's scope(before vs after) through this channel;
    scope_delta: MergedScopeDelta,
    ///
    kind: ChannelKind<T>,
}

impl<T: Data> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel {
            batch_size: self.batch_size,
            batch_capacity: self.batch_capacity,
            scope_capacity: self.scope_capacity,
            source: self.source,
            scope_delta: self.scope_delta.clone(),
            kind: ChannelKind::Pipeline(false),
        }
    }
}

impl<T: Data> Default for Channel<T> {
    fn default() -> Self {
        Channel {
            batch_size: 1024,
            batch_capacity: 64,
            scope_capacity: 64,
            source: Port::new(0, 0),
            scope_delta: MergedScopeDelta::new(0),
            kind: ChannelKind::Pipeline(false),
        }
    }
}

pub(crate) struct MaterializedChannel<T: Data> {
    push: ChannelPush<T>,
    pull: GeneralPull<MicroBatch<T>>,
    notify: Option<GeneralPush<MicroBatch<T>>>,
}

impl<T: Data> MaterializedChannel<T> {
    pub fn take(self) -> (ChannelPush<T>, GeneralPull<MicroBatch<T>>, Option<GeneralPush<MicroBatch<T>>>) {
        (self.push, self.pull, self.notify)
    }
}

impl<T: Data> Channel<T> {
    pub fn bind(port: &OutputBuilderImpl<T>) -> Self {
        let scope_level = port.get_scope_level();
        Channel {
            batch_size: port.get_batch_size(),
            batch_capacity: port.get_batch_capacity(),
            scope_capacity: port.get_scope_capacity(),
            source: port.get_port(),
            scope_delta: MergedScopeDelta::new(scope_level as usize),
            kind: ChannelKind::Pipeline(false),
        }
    }

    pub fn set_batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.batch_size = batch_size;
        self
    }

    pub fn get_batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn set_batch_capacity(&mut self, capacity: u32) -> &mut Self {
        self.batch_capacity = capacity;
        self
    }

    pub fn get_batch_capacity(&self) -> u32 {
        self.batch_capacity
    }

    pub fn set_scope_capacity(&mut self, capacity: u32) -> &mut Self {
        self.scope_capacity = capacity;
        self
    }

    pub fn get_scope_capacity(&self) -> u32 {
        self.scope_capacity
    }

    pub fn set_channel_kind(&mut self, kind: ChannelKind<T>) -> &mut Self {
        self.kind = kind;
        self
    }

    pub fn add_delta(&mut self, delta: ScopeDelta) -> Option<ScopeDelta> {
        self.scope_delta.add_delta(delta)
    }

    pub fn get_scope_level(&self) -> u32 {
        self.scope_delta.output_scope_level() as u32
    }

    pub fn is_pipeline(&self) -> bool {
        self.kind.is_pipeline()
    }
}

impl<T: Data> Channel<T> {
    fn build_pipeline(
        self, target: Port, id: ChannelId, event_emit: EventEmitter, sync: bool,
    ) -> MaterializedChannel<T> {
        let (tx, rx) = crate::data_plane::pipeline::<MicroBatch<T>>(id);
        let scope_level = self.get_scope_level();
        let ch_info = ChannelInfo::new(id, scope_level, 1, 1, self.source, target);
        let mut notify = None;
        let push = if sync {
            notify = Some(GeneralPush::IntraThread(tx.clone()));
            let mut push = LocalMicroBatchPush::new(ch_info, tx, event_emit);
            push.sync_global_state();
            MicroBatchPush::Local(push)
        } else {
            let push = LocalMicroBatchPush::new(ch_info, tx, event_emit);
            MicroBatchPush::Local(push)
        };
        let worker = crate::worker_id::get_current_worker().index;
        let ch = CancelHandle::SC(SingleConsCancel::new(worker));
        let push = ChannelPush::new(ch_info, self.scope_delta, push, ch);
        MaterializedChannel { push, pull: rx.into(), notify }
    }

    fn build_remote(
        &self, scope_level: u32, target: Port, id: ChannelId, dfb: &DataflowBuilder,
    ) -> Result<
        (ChannelInfo, Vec<EventEmitPush<T>>, GeneralPull<MicroBatch<T>>, GeneralPush<MicroBatch<T>>),
        BuildJobError,
    > {
        let (mut raw, pull) = crate::communication::build_channel::<MicroBatch<T>>(id, &dfb.config)?.take();
        let worker_index = crate::worker_id::get_current_worker().index as usize;
        let notify = raw.swap_remove(worker_index);
        let ch_info = ChannelInfo::new(id, scope_level, raw.len(), raw.len(), self.source, target);
        let mut pushes = Vec::with_capacity(raw.len());
        let source = dfb.worker_id.index;
        for (idx, p) in raw.into_iter().enumerate() {
            let push = EventEmitPush::new(ch_info, source, idx as u32, p, dfb.event_emitter.clone());
            pushes.push(push);
        }
        Ok((ch_info, pushes, pull, notify))
    }

    pub(crate) fn connect_to(
        mut self, target: Port, dfb: &DataflowBuilder,
    ) -> Result<MaterializedChannel<T>, BuildJobError> {
        let index = dfb.next_channel_index();
        let id = ChannelId { job_seq: dfb.config.job_id as u64, index };
        let batch_size = self.batch_size;
        let scope_level = self.get_scope_level();
        let scope_capacity = self.scope_capacity as usize;
        let batch_capacity = self.batch_capacity as usize;

        if index > 1 {
            trace_worker!(
                "channel[{}] : config batch size = {}, runtime batch size = {}, scope_level = {}",
                index,
                self.batch_size,
                batch_size,
                self.get_scope_level()
            );
        }

        if dfb.worker_id.total_peers() == 1 {
            let event_emit = dfb.event_emitter.clone();
            return Ok(self.build_pipeline(target, id, event_emit, false));
        }

        let kind = std::mem::replace(&mut self.kind, ChannelKind::Pipeline(false));
        match kind {
            ChannelKind::Pipeline(sync) => {
                let event_emit = dfb.event_emitter.clone();
                Ok(self.build_pipeline(target, id, event_emit, sync))
            }
            ChannelKind::Shuffle(r) => {
                let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                let mut buffers = Vec::with_capacity(pushes.len());
                for _ in 0..pushes.len() {
                    let b = ScopeBufferPool::new(batch_size, batch_capacity, scope_capacity, scope_level);
                    buffers.push(b);
                }
                let push = ExchangeMicroBatchPush::new(info, r, buffers, pushes);
                let ch = push.get_cancel_handle();
                let push = ChannelPush::new(info, self.scope_delta, MicroBatchPush::Exchange(push), ch);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
            ChannelKind::Broadcast => {
                let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                let push = BroadcastBatchPush::new(info, pushes);
                let ch = push.get_cancel_handle();
                let push = ChannelPush::new(info, self.scope_delta, MicroBatchPush::Broadcast(push), ch);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
            ChannelKind::Aggregate(worker) => {
                let (mut ch_info, pushes, pull, notify) =
                    self.build_remote(scope_level, target, id, dfb)?;
                ch_info.target_peers = 1;
                let push = AggregateBatchPush::new(worker, ch_info, pushes);
                let cancel = CancelHandle::SC(SingleConsCancel::new(worker));
                let push =
                    ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::Global(push), cancel);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
            ChannelKind::ShuffleScope => {
                let (ch_info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                let push = ExchangeByScopePush::new(ch_info, pushes);
                let ch = push.get_cancel_handle();
                let push =
                    ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::ScopeGlobal(push), ch);
                Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
            }
        }
    }
}
