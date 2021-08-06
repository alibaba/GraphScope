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
use crate::communication::decorator::buffered::BufferedPush;
use crate::communication::decorator::exchange::{ExchangeByScopePush, ExchangeMiniBatchPush};
use crate::communication::IOResult;
use crate::data::{Data, DataSet, MicroBatch};
use crate::data_plane::intra_thread::ThreadPush;
use crate::data_plane::Push;
use crate::graph::Port;
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::Tag;
use pegasus_common::buffer::Batch;

pub mod aggregate;
pub mod broadcast;
pub mod buffered;
pub mod evented;
pub mod exchange;

pub trait ScopeStreamPush<T: Data> {
    fn port(&self) -> Port;

    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<()>;

    fn push_last(&mut self, msg: T, end: EndSignal) -> IOResult<()>;

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        for x in iter {
            self.push(tag, x)?;
        }
        Ok(())
    }

    fn notify_end(&mut self, end: EndSignal) -> IOResult<()>;

    fn flush(&mut self) -> IOResult<()>;

    fn close(&mut self) -> IOResult<()>;
}

pub struct LocalMiniBatchPush<T: Data> {
    pub ch_info: ChannelInfo,
    worker: u32,
    inner: ThreadPush<DataSet<T>>,
    push_counts: TidyTagMap<(usize, usize)>,
}

impl<T: Data> LocalMiniBatchPush<T> {
    pub fn new(ch_info: ChannelInfo, push: ThreadPush<DataSet<T>>) -> Self {
        let worker = crate::worker_id::get_current_worker().index;
        let push_counts = TidyTagMap::new(ch_info.scope_level);
        LocalMiniBatchPush { ch_info, worker, inner: push, push_counts }
    }
}

impl<T: Data> ScopeStreamPush<DataSet<T>> for LocalMiniBatchPush<T> {
    fn port(&self) -> Port {
        self.ch_info.source_port
    }

    fn push(&mut self, _: &Tag, msg: DataSet<T>) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let c = self.push_counts.get_mut_or_insert(&msg.tag);
            c.0 += msg.len();
        }
        self.inner.push(msg)
    }

    fn push_last(&mut self, mut msg: DataSet<T>, end: EndSignal) -> IOResult<()> {
        assert_eq!(msg.tag, end.tag);
        if log_enabled!(log::Level::Trace) {
            let c = self
                .push_counts
                .remove(&end.tag)
                .unwrap_or((0, 0));
            let size = c.0 + c.1 + msg.len();
            if size > 0 {
                trace_worker!(
                    "output[{:?}]: push last data of {:?}, total pushed {} to self;",
                    self.port(),
                    msg.tag,
                    size
                );
            }
        }
        let (tag, w, u) = end.take();
        assert!(u.is_none(), "tag={:?}, w={:?}, u={:?}, port {:?}", tag, w, u, self.port());
        let new_end = EndSignal::new(tag, w);
        msg.set_last(new_end);
        self.inner.push(msg)
    }

    fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            if end.tag.len() == self.push_counts.scope_level {
                let c = self
                    .push_counts
                    .remove(&end.tag)
                    .unwrap_or((0, 0));
                let size = c.0 + c.1;
                trace_worker!(
                    "output[{:?}]: notify end of {:?}, total pushed {} to self;",
                    self.port(),
                    end.tag,
                    size
                );
            } else {
                trace_worker!("output[{:?}] notify end of {:?}", self.port(), end.tag);
            }
        }
        let seq = end.seq;
        let (tag, w, u) = end.take();
        assert!(u.is_none(), "end is {:?}, {:?}", w, u);
        let mut new_end = EndSignal::new(tag.clone(), w);
        new_end.seq = seq;

        let mut data = DataSet::new(tag, self.worker, seq, Batch::new());
        data.set_last(new_end);
        self.inner.push(data)
    }

    fn flush(&mut self) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let port = self.port();
            trace_worker!("output[{:?}] flush;", port);
            for (a, b) in self.push_counts.iter_mut() {
                let cnt = b.0;
                if cnt > 0 {
                    b.1 += cnt;
                    b.0 = 0;
                    trace_worker!("output[{:?}] flush {} data of {:?} to self;", port, cnt, a);
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

pub trait ScopeStreamBuffer {
    fn scope_size(&self) -> usize;

    fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize>;

    fn flush_scope(&mut self, tag: &Tag) -> IOResult<()>;
}

pub enum DataPush<T: Data> {
    Pipeline(BufferedPush<T, LocalMiniBatchPush<T>>),
    Shuffle(ExchangeMiniBatchPush<T>),
    ScopeShuffle(BufferedPush<T, ExchangeByScopePush<T>>),
    Broadcast(BufferedPush<T, BroadcastBatchPush<T>>),
    Aggregate(BufferedPush<T, AggregateBatchPush<T>>),
}

impl<T: Data> DataPush<T> {
    pub fn forward(&mut self, mut data: DataSet<T>) -> IOResult<()> {
        match self {
            DataPush::Pipeline(p) => p.forward_buffer(data),
            DataPush::Shuffle(p) => {
                let tag = data.tag.clone();
                for d in data.drain() {
                    p.push(&tag, d)?;
                }
                Ok(())
            }
            DataPush::ScopeShuffle(p) => p.forward_buffer(data),
            DataPush::Broadcast(p) => p.forward_buffer(data),
            DataPush::Aggregate(p) => p.forward_buffer(data),
        }
    }
}

impl<T: Data> ScopeStreamBuffer for DataPush<T> {
    fn scope_size(&self) -> usize {
        match self {
            DataPush::Pipeline(p) => p.scope_size(),
            DataPush::Shuffle(p) => p.scope_size(),
            DataPush::Aggregate(p) => p.scope_size(),
            DataPush::Broadcast(p) => p.scope_size(),
            DataPush::ScopeShuffle(p) => p.scope_size(),
        }
    }

    #[inline]
    fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
        match self {
            DataPush::Pipeline(p) => p.ensure_capacity(tag),
            DataPush::Shuffle(p) => p.ensure_capacity(tag),
            DataPush::Aggregate(p) => p.ensure_capacity(tag),
            DataPush::Broadcast(p) => p.ensure_capacity(tag),
            DataPush::ScopeShuffle(p) => p.ensure_capacity(tag),
        }
    }

    fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
        match self {
            DataPush::Pipeline(p) => p.flush_scope(tag),
            DataPush::Shuffle(p) => p.flush_scope(tag),
            DataPush::Aggregate(p) => p.flush_scope(tag),
            DataPush::Broadcast(p) => p.flush_scope(tag),
            DataPush::ScopeShuffle(p) => p.flush_scope(tag),
        }
    }
}

impl<T: Data> ScopeStreamPush<T> for DataPush<T> {
    fn port(&self) -> Port {
        match self {
            DataPush::Pipeline(p) => p.port(),
            DataPush::Shuffle(p) => p.port(),
            DataPush::Aggregate(p) => p.port(),
            DataPush::Broadcast(p) => p.port(),
            DataPush::ScopeShuffle(p) => p.port(),
        }
    }

    #[inline]
    fn push(&mut self, tag: &Tag, msg: T) -> IOResult<()> {
        match self {
            DataPush::Pipeline(p) => p.push(tag, msg),
            DataPush::Shuffle(p) => p.push(tag, msg),
            DataPush::Aggregate(p) => p.push(tag, msg),
            DataPush::Broadcast(p) => p.push(tag, msg),
            DataPush::ScopeShuffle(p) => p.push(tag, msg),
        }
    }

    #[inline]
    fn push_last(&mut self, msg: T, end: EndSignal) -> IOResult<()> {
        trace_worker!("out port {:?} push end of {:?}", self.port(), end.tag);
        match self {
            DataPush::Pipeline(p) => p.push_last(msg, end),
            DataPush::Shuffle(p) => p.push_last(msg, end),
            DataPush::Aggregate(p) => p.push_last(msg, end),
            DataPush::Broadcast(p) => p.push_last(msg, end),
            DataPush::ScopeShuffle(p) => p.push_last(msg, end),
        }
    }

    fn push_iter<I: Iterator<Item = T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        match self {
            DataPush::Pipeline(p) => p.push_iter(tag, iter),
            DataPush::Shuffle(p) => p.push_iter(tag, iter),
            DataPush::Aggregate(p) => p.push_iter(tag, iter),
            DataPush::Broadcast(p) => p.push_iter(tag, iter),
            DataPush::ScopeShuffle(p) => p.push_iter(tag, iter),
        }
    }

    #[inline]
    fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
        trace_worker!("out port {:?} notify end of {:?}", self.port(), end.tag);
        match self {
            DataPush::Pipeline(p) => p.notify_end(end),
            DataPush::Shuffle(p) => p.notify_end(end),
            DataPush::Aggregate(p) => p.notify_end(end),
            DataPush::Broadcast(p) => p.notify_end(end),
            DataPush::ScopeShuffle(p) => p.notify_end(end),
        }
    }

    #[inline]
    fn flush(&mut self) -> IOResult<()> {
        match self {
            DataPush::Pipeline(p) => p.flush(),
            DataPush::Shuffle(p) => p.flush(),
            DataPush::Aggregate(p) => p.flush(),
            DataPush::Broadcast(p) => p.flush(),
            DataPush::ScopeShuffle(p) => p.flush(),
        }
    }

    #[inline]
    fn close(&mut self) -> IOResult<()> {
        match self {
            DataPush::Pipeline(p) => p.close(),
            DataPush::Shuffle(p) => p.close(),
            DataPush::Aggregate(p) => p.close(),
            DataPush::Broadcast(p) => p.close(),
            DataPush::ScopeShuffle(p) => p.close(),
        }
    }
}

////////////////////////////////////////////////
#[allow(dead_code)]
pub struct LocalMicroBatchPush<T: Data> {
    pub ch_info: ChannelInfo,
    inner: ThreadPush<MicroBatch<T>>,
    push_counts: TidyTagMap<(usize, usize)>,
}

#[allow(dead_code)]
impl<T: Data> LocalMicroBatchPush<T> {
    pub fn new(ch_info: ChannelInfo, push: ThreadPush<MicroBatch<T>>) -> Self {
        let push_counts = TidyTagMap::new(ch_info.scope_level);
        LocalMicroBatchPush { ch_info, inner: push, push_counts }
    }
}

impl<T: Data> Push<MicroBatch<T>> for LocalMicroBatchPush<T> {
    fn push(&mut self, msg: MicroBatch<T>) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let c = self.push_counts.get_mut_or_insert(&msg.tag);
            c.0 += msg.len();
        }
        self.inner.push(msg)
    }

    fn flush(&mut self) -> IOResult<()> {
        if log_enabled!(log::Level::Trace) {
            let port = self.ch_info.source_port;
            for (a, b) in self.push_counts.iter_mut() {
                let cnt = b.0;
                if cnt > 0 {
                    b.1 += cnt;
                    b.0 = 0;
                    trace_worker!("output[{:?}] flush {} data of {:?} to self;", port, cnt, a);
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
