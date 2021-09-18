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
use crate::communication::output::{ChannelPush, OutputBuilderImpl};
use crate::data::MicroBatch;
use crate::data_plane::{GeneralPull, GeneralPush};
use crate::graph::Port;
use crate::Data;

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
    batch_size: usize,
    batch_capacity: u32,
    scope_capacity: u32,
    inbound_scope_level: u32,
    source: Port,
    scope_delta: MergedScopeDelta,
    kind: ChannelKind<T>,
}

impl<T: Data> Clone for Channel<T> {
    fn clone(&self) -> Self {
        Channel {
            batch_size: self.batch_size,
            batch_capacity: self.batch_capacity,
            scope_capacity: self.scope_capacity,
            inbound_scope_level: self.inbound_scope_level,
            source: self.source,
            scope_delta: self.scope_delta.clone(),
            kind: ChannelKind::Pipeline,
        }
    }
}

impl<T: Data> Default for Channel<T> {
    fn default() -> Self {
        Channel {
            batch_size: 1024,
            batch_capacity: 64,
            scope_capacity: 64,
            inbound_scope_level: 0,
            source: Port::new(0, 0),
            scope_delta: MergedScopeDelta::new(0),
            kind: ChannelKind::Pipeline,
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
    pub fn mount_to(port: &OutputBuilderImpl<T>) -> Self {
        let scope_level = port.get_scope_level();
        Channel {
            batch_size: port.get_batch_size(),
            batch_capacity: port.get_batch_capacity(),
            inbound_scope_level: scope_level,
            scope_capacity: port.get_scope_capacity(),
            source: port.get_port(),
            scope_delta: MergedScopeDelta::new(scope_level as usize),
            kind: ChannelKind::Pipeline,
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

    pub fn add_delta(&mut self, delta: ScopeDelta) {
        self.scope_delta.add_delta(delta);
    }

    pub fn get_scope_level(&self) -> u32 {
        self.scope_delta.output_scope_level() as u32
    }

    pub fn is_local(&self) -> bool {
        self.kind.is_local()
    }
}

#[cfg(not(feature = "rob"))]
mod rob {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use pegasus_common::buffer::Batch;

    use super::*;
    use crate::channel_id::{ChannelId, ChannelInfo};
    use crate::communication::buffer::BufferedPush;
    use crate::communication::cancel::{CancelHandle, SingleConsCancel};
    use crate::communication::decorator::aggregate::AggregateBatchPush;
    use crate::communication::decorator::broadcast::BroadcastBatchPush;
    use crate::communication::decorator::exchange::ExchangeByScopePush;
    use crate::communication::decorator::{
        evented::EventEmitPush, exchange::ExchangeMicroBatchPush, LocalMiniBatchPush, MicroBatchPush,
    };
    use crate::communication::output::ChannelPush;
    use crate::data::{Data, MicroBatch};
    use crate::data_plane::{GeneralPull, GeneralPush};
    use crate::dataflow::DataflowBuilder;
    use crate::errors::BuildJobError;
    use crate::graph::Port;

    impl<T: Data> Channel<T> {
        fn build_pipeline(self, target: Port, id: ChannelId) -> MaterializedChannel<T> {
            let (tx, rx) = crate::data_plane::pipeline::<MicroBatch<T>>(id);
            let scope_level = self.get_scope_level();
            let ch_info = ChannelInfo::new(id, scope_level, 1, 1, self.source, target);
            let push = LocalMiniBatchPush::new(ch_info, tx);
            let push = BufferedPush::new(
                scope_level,
                self.batch_size,
                self.scope_capacity as usize,
                self.batch_capacity as usize,
                push,
            );
            let worker = crate::worker_id::get_current_worker().index;
            let ch = CancelHandle::SC(SingleConsCancel::new(worker));
            let push = ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::Pipeline(push), ch);
            MaterializedChannel { push, pull: rx.into(), notify: None }
        }

        fn build_remote(
            &self, scope_level: u32, target: Port, id: ChannelId, cyclic: &Arc<AtomicBool>,
            dfb: &DataflowBuilder,
        ) -> Result<
            (ChannelInfo, Vec<EventEmitPush<T>>, GeneralPull<MicroBatch<T>>, GeneralPush<MicroBatch<T>>),
            BuildJobError,
        > {
            let (mut raw, pull) =
                crate::communication::build_channel::<MicroBatch<T>>(id, &dfb.config)?.take();
            let worker_index = crate::worker_id::get_current_worker().index as usize;
            let notify = raw.swap_remove(worker_index);
            let ch_info = ChannelInfo::new(id, scope_level, raw.len(), raw.len(), self.source, target);
            let mut pushes = Vec::with_capacity(raw.len());
            let source = dfb.worker_id.index;
            for (idx, p) in raw.into_iter().enumerate() {
                let has_cycles = cyclic.clone();
                let push = EventEmitPush::new(
                    ch_info,
                    source,
                    idx as u32,
                    has_cycles,
                    p,
                    dfb.event_emitter.clone(),
                );
                pushes.push(push);
            }
            Ok((ch_info, pushes, pull, notify))
        }

        pub(crate) fn connect_to(
            mut self, target: Port, cyclic: Arc<AtomicBool>, dfb: &DataflowBuilder,
        ) -> Result<MaterializedChannel<T>, BuildJobError> {
            let index = dfb.next_channel_index();
            let id = ChannelId { job_seq: dfb.config.job_id as u64, index };
            let batch_size = Batch::<T>::with_capacity(self.batch_size).capacity();
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
                return Ok(self.build_pipeline(target, id));
            }

            let kind = std::mem::replace(&mut self.kind, ChannelKind::Pipeline);

            match kind {
                ChannelKind::Pipeline => Ok(self.build_pipeline(target, id)),
                ChannelKind::Shuffle(r) => {
                    let (ch_info, pushes, pull, notify) =
                        self.build_remote(scope_level, target, id, &cyclic, dfb)?;
                    let mut buffered = Vec::with_capacity(pushes.len());
                    for p in pushes {
                        buffered.push(BufferedPush::new(
                            scope_level,
                            batch_size,
                            scope_capacity,
                            batch_capacity,
                            p,
                        ));
                    }
                    let push = ExchangeMicroBatchPush::new(ch_info, buffered, r);
                    let cancel = push.get_cancel_handle();
                    let push =
                        ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::Shuffle(push), cancel);
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
                ChannelKind::Broadcast => {
                    let (ch_info, pushes, pull, notify) =
                        self.build_remote(scope_level, target, id, &cyclic, dfb)?;
                    let push = BroadcastBatchPush::new(ch_info, pushes);
                    let cancel = push.get_cancel_handle();
                    let push =
                        BufferedPush::new(scope_level, batch_size, scope_capacity, batch_capacity, push);
                    let push = ChannelPush::new(
                        ch_info,
                        self.scope_delta,
                        MicroBatchPush::Broadcast(push),
                        cancel,
                    );

                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
                ChannelKind::Aggregate(worker) => {
                    let (mut ch_info, pushes, pull, notify) =
                        self.build_remote(scope_level, target, id, &cyclic, dfb)?;
                    ch_info.target_peers = 1;
                    let push = AggregateBatchPush::new(worker, ch_info, pushes, &cyclic);
                    let push =
                        BufferedPush::new(scope_level, batch_size, scope_capacity, batch_capacity, push);
                    let cancel = CancelHandle::SC(SingleConsCancel::new(worker));
                    let push = ChannelPush::new(
                        ch_info,
                        self.scope_delta,
                        MicroBatchPush::Aggregate(push),
                        cancel,
                    );
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
                ChannelKind::ShuffleScope => {
                    // Note: set cyclic to true because no-cyclic optimizes are not need in aggregation;
                    let (ch_info, pushes, pull, notify) =
                        self.build_remote(scope_level, target, id, &Arc::new(AtomicBool::new(true)), dfb)?;
                    let push = ExchangeByScopePush::new(ch_info, &cyclic, pushes);
                    let cancel = push.get_cancel_handle();
                    let push =
                        BufferedPush::new(scope_level, batch_size, scope_capacity, batch_capacity, push);
                    let push = ChannelPush::new(
                        ch_info,
                        self.scope_delta,
                        MicroBatchPush::ScopeShuffle(push),
                        cancel,
                    );
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
            }
        }
    }
}

#[cfg(feature = "rob")]
mod rob {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use super::*;
    use crate::channel_id::{ChannelId, ChannelInfo};
    use crate::communication::buffer::ScopeBufferPool;
    use crate::communication::cancel::{CancelHandle, SingleConsCancel};
    use crate::communication::decorator::aggregate::AggregateBatchPush;
    use crate::communication::decorator::broadcast::BroadcastBatchPush;
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::decorator::exchange::{ExchangeByScopePush, ExchangeMicroBatchPush};
    use crate::communication::decorator::{LocalMicroBatchPush, MicroBatchPush};
    use crate::dataflow::DataflowBuilder;
    use crate::BuildJobError;

    impl<T: Data> Channel<T> {
        fn build_pipeline(self, target: Port, id: ChannelId) -> MaterializedChannel<T> {
            let (tx, rx) = crate::data_plane::pipeline::<MicroBatch<T>>(id);
            let scope_level = self.get_scope_level();
            let ch_info = ChannelInfo::new(id, scope_level, 1, 1, self.source, target);
            let push = LocalMicroBatchPush::new(ch_info, tx);
            let worker = crate::worker_id::get_current_worker().index;
            let ch = CancelHandle::SC(SingleConsCancel::new(worker));
            let push = ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::Local(push), ch);
            MaterializedChannel { push, pull: rx.into(), notify: None }
        }

        fn build_remote(
            &self, scope_level: u32, target: Port, id: ChannelId, dfb: &DataflowBuilder,
        ) -> Result<
            (ChannelInfo, Vec<EventEmitPush<T>>, GeneralPull<MicroBatch<T>>, GeneralPush<MicroBatch<T>>),
            BuildJobError,
        > {
            let (mut raw, pull) =
                crate::communication::build_channel::<MicroBatch<T>>(id, &dfb.config)?.take();
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
            mut self, target: Port, cyclic: Arc<AtomicBool>, dfb: &DataflowBuilder,
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
                return Ok(self.build_pipeline(target, id));
            }

            let kind = std::mem::replace(&mut self.kind, ChannelKind::Pipeline);

            match kind {
                ChannelKind::Pipeline => Ok(self.build_pipeline(target, id)),
                ChannelKind::Shuffle(r) => {
                    let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                    let mut buffers = Vec::with_capacity(pushes.len());
                    for _ in 0..pushes.len() {
                        let b =
                            ScopeBufferPool::new(batch_size, batch_capacity, scope_capacity, scope_level);
                        buffers.push(b);
                    }
                    let push = ExchangeMicroBatchPush::new(info, cyclic, r, buffers, pushes);
                    let ch = push.get_cancel_handle();
                    let push = ChannelPush::new(info, self.scope_delta, MicroBatchPush::Exchange(push), ch);
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
                ChannelKind::Broadcast => {
                    let (info, pushes, pull, notify) = self.build_remote(scope_level, target, id, dfb)?;
                    let push = BroadcastBatchPush::new(info, pushes);
                    let ch = push.get_cancel_handle();
                    let push =
                        ChannelPush::new(info, self.scope_delta, MicroBatchPush::Broadcast(push), ch);
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
                ChannelKind::Aggregate(worker) => {
                    let (mut ch_info, pushes, pull, notify) =
                        self.build_remote(scope_level, target, id, dfb)?;
                    ch_info.target_peers = 1;
                    let push = AggregateBatchPush::new(worker, ch_info, pushes, &cyclic);
                    let cancel = CancelHandle::SC(SingleConsCancel::new(worker));
                    let push =
                        ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::Global(push), cancel);
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
                ChannelKind::ShuffleScope => {
                    let (ch_info, pushes, pull, notify) =
                        self.build_remote(scope_level, target, id, dfb)?;
                    let push = ExchangeByScopePush::new(ch_info, &cyclic, pushes);
                    let ch = push.get_cancel_handle();
                    let push =
                        ChannelPush::new(ch_info, self.scope_delta, MicroBatchPush::ScopeGlobal(push), ch);
                    Ok(MaterializedChannel { push, pull: pull.into(), notify: Some(notify) })
                }
            }
        }
    }
}
