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

use crate::channel_id::ChannelInfo;
use crate::communication::decorator::aggregate::AggregateBatchPush;
use crate::communication::decorator::broadcast::BroadcastBatchPush;
use crate::communication::decorator::exchange::{ExchangeByScopePush, ExchangeMicroBatchPush};
use crate::communication::IOResult;
use crate::data::EndOfScope;
use crate::data::MicroBatch;
use crate::data_plane::intra_thread::ThreadPush;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::event::emitter::EventEmitter;
use crate::event::{Event, EventKind};
use crate::graph::Port;
use crate::progress::EndSignal;
use crate::progress::Weight;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
pub mod aggregate;
pub mod broadcast;
pub mod evented;
pub mod exchange;

pub trait ScopeStreamPush<T: Data> {
    fn port(&self) -> Port;

    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<()>;

    fn push_last(&mut self, msg: T, end: EndOfScope) -> IOResult<()>;

    fn try_push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        for x in iter {
            self.push(tag, x)?;
        }
        Ok(())
    }

    fn notify_end(&mut self, end: EndOfScope) -> IOResult<()>;

    fn flush(&mut self) -> IOResult<()>;

    fn close(&mut self) -> IOResult<()>;
}

pub trait ScopeStreamBuffer {
    fn scope_size(&self) -> usize;

    fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize>;

    fn flush_scope(&mut self, tag: &Tag) -> IOResult<()>;
}

pub trait BlockPush {
    // try to unblock data on scope, return true if the data is unblocked;
    fn try_unblock(&mut self, tag: &Tag) -> IOResult<bool>;

    fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()>;
}

#[allow(dead_code)]
pub struct LocalMicroBatchPush<T: Data> {
    pub ch_info: ChannelInfo,
    pub worker_index: u32,
    inner: ThreadPush<MicroBatch<T>>,
    event_emit: EventEmitter,
    global_state: bool,
    push_counts: TidyTagMap<(usize, usize)>,
}

#[allow(dead_code)]
impl<T: Data> LocalMicroBatchPush<T> {
    pub fn new(ch_info: ChannelInfo, push: ThreadPush<MicroBatch<T>>, event_emit: EventEmitter) -> Self {
        let push_counts = TidyTagMap::new(ch_info.scope_level);
        let worker_index = crate::worker_id::get_current_worker().index;
        LocalMicroBatchPush {
            ch_info,
            worker_index,
            inner: push,
            event_emit,
            global_state: false,
            push_counts,
        }
    }

    pub fn sync_global_state(&mut self) {
        self.global_state = true;
    }
}

impl<T: Data> Push<MicroBatch<T>> for LocalMicroBatchPush<T> {
    fn push(&mut self, mut batch: MicroBatch<T>) -> IOResult<()> {
        if let Some(mut end) = batch.take_end() {
            let mut c = self
                .push_counts
                .remove(&batch.tag)
                .unwrap_or((0, 0));
            c.1 += batch.len();
            if batch.is_empty() {
                trace_worker!(
                    "output[{:?}] push end of {:?} to channel[{}] to self, total pushed {};",
                    self.ch_info.source_port,
                    batch.tag,
                    self.ch_info.index(),
                    c.1
                )
            } else {
                trace_worker!(
                    "output[{:?}] push last batch(len={}) of {:?} to channel[{}] to self, total pushed {};",
                    self.ch_info.source_port,
                    batch.len(),
                    batch.tag,
                    self.ch_info.id.index,
                    c.1
                );
            }
            end.total_send = c.1 as u64;

            if self.global_state {
                let worker = self.worker_index;
                let port = self.ch_info.target_port;

                if end.source.value() == 1 {
                    let event = Event::new(
                        worker,
                        port,
                        EventKind::End(EndSignal::new(end.clone(), Weight::partial_current())),
                    );
                    for i in 0..self.event_emit.peers() as u32 {
                        if i != worker {
                            self.event_emit.send(i, event.clone())?;
                        }
                    }
                    batch.set_end(end);
                } else {
                    if !batch.is_empty() {
                        self.inner.push(batch)?;
                    }
                    let event = Event::new(
                        worker,
                        port,
                        EventKind::End(EndSignal::new(end, Weight::partial_current())),
                    );
                    return self.event_emit.broadcast(event);
                }
            } else {
                batch.set_end(end);
            }
        } else {
            let c = self.push_counts.get_mut_or_insert(&batch.tag);
            c.0 += batch.len();
            c.1 += batch.len();
        }

        self.inner.push(batch)
    }

    fn flush(&mut self) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let port = self.ch_info.source_port;
            let index = self.ch_info.index();
            for (t, (a, _)) in self.push_counts.iter_mut() {
                if *a > 0 {
                    trace_worker!(
                        "output[{:?}] flush {} data of {:?} to channel[{}] to self;",
                        port,
                        *a,
                        t,
                        index
                    );
                    *a = 0;
                }
            }
        }
        self.inner.flush()?;
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        self.flush()?;
        self.inner.close()
    }
}

#[allow(dead_code)]
pub(crate) enum MicroBatchPush<T: Data> {
    Local(LocalMicroBatchPush<T>),
    Exchange(ExchangeMicroBatchPush<T>),
    Broadcast(BroadcastBatchPush<T>),
    Global(AggregateBatchPush<T>),
    ScopeGlobal(ExchangeByScopePush<T>),
}

impl<T: Data> Push<MicroBatch<T>> for MicroBatchPush<T> {
    fn push(&mut self, batch: MicroBatch<T>) -> Result<(), IOError> {
        match self {
            MicroBatchPush::Local(p) => p.push(batch),
            MicroBatchPush::Exchange(p) => p.push(batch),
            MicroBatchPush::Broadcast(p) => p.push(batch),
            MicroBatchPush::Global(p) => p.push(batch),
            MicroBatchPush::ScopeGlobal(p) => p.push(batch),
        }
    }

    fn flush(&mut self) -> Result<(), IOError> {
        match self {
            MicroBatchPush::Local(p) => p.flush(),
            MicroBatchPush::Exchange(p) => p.flush(),
            MicroBatchPush::Broadcast(p) => p.flush(),
            MicroBatchPush::Global(p) => p.flush(),
            MicroBatchPush::ScopeGlobal(p) => p.flush(),
        }
    }

    fn close(&mut self) -> Result<(), IOError> {
        match self {
            MicroBatchPush::Local(p) => p.close(),
            MicroBatchPush::Exchange(p) => p.close(),
            MicroBatchPush::Broadcast(p) => p.close(),
            MicroBatchPush::Global(p) => p.close(),
            MicroBatchPush::ScopeGlobal(p) => p.close(),
        }
    }
}

impl<T: Data> BlockPush for MicroBatchPush<T> {
    fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError> {
        match self {
            MicroBatchPush::Exchange(p) => p.try_unblock(tag),
            _ => Ok(true),
        }
    }

    fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()> {
        match self {
            MicroBatchPush::Exchange(p) => p.clean_block_of(tag),
            _ => Ok(()),
        }
    }
}
