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




use crate::{Data, Tag};
use crate::graph::Port;
use crate::communication::IOResult;
use crate::progress::EndSignal;

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

pub trait ScopeStreamBuffer {
    fn scope_size(&self) -> usize;

    fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize>;

    fn flush_scope(&mut self, tag: &Tag) -> IOResult<()>;
}

pub use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use super::*;

    use crate::channel_id::ChannelInfo;
    use crate::communication::decorator::aggregate::AggregateBatchPush;
    use crate::communication::decorator::broadcast::BroadcastBatchPush;
    use crate::communication::decorator::buffered::BufferedPush;
    use crate::communication::decorator::exchange::{ExchangeByScopePush, ExchangeMicroBatchPush};
    use crate::communication::IOResult;
    use crate::data::{Data, MicroBatch};
    use crate::data_plane::intra_thread::ThreadPush;
    use crate::data_plane::Push;
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::tag::tools::map::TidyTagMap;
    use crate::Tag;
    use pegasus_common::buffer::Batch;


    pub struct LocalMiniBatchPush<T: Data> {
        pub ch_info: ChannelInfo,
        worker: u32,
        inner: ThreadPush<MicroBatch<T>>,
        push_counts: TidyTagMap<(usize, usize)>,
    }

    impl<T: Data> LocalMiniBatchPush<T> {
        pub fn new(ch_info: ChannelInfo, push: ThreadPush<MicroBatch<T>>) -> Self {
            let worker = crate::worker_id::get_current_worker().index;
            let push_counts = TidyTagMap::new(ch_info.scope_level);
            LocalMiniBatchPush { ch_info, worker, inner: push, push_counts }
        }
    }

    impl<T: Data> ScopeStreamPush<MicroBatch<T>> for LocalMiniBatchPush<T> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, _: &Tag, msg: MicroBatch<T>) -> IOResult<()> {
            if log_enabled!(log::Level::Trace) {
                let c = self.push_counts.get_mut_or_insert(&msg.tag);
                c.0 += msg.len();
            }
            self.inner.push(msg)
        }

        fn push_last(&mut self, mut msg: MicroBatch<T>, end: EndSignal) -> IOResult<()> {
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
            msg.set_end(new_end);
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

            let mut data = MicroBatch::new(tag, self.worker, seq, Batch::new());
            data.set_end(new_end);
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

    pub enum MicroBatchPush<T: Data> {
        Pipeline(BufferedPush<T, LocalMiniBatchPush<T>>),
        Shuffle(ExchangeMicroBatchPush<T>),
        ScopeShuffle(BufferedPush<T, ExchangeByScopePush<T>>),
        Broadcast(BufferedPush<T, BroadcastBatchPush<T>>),
        Aggregate(BufferedPush<T, AggregateBatchPush<T>>),
    }

    impl<T: Data> MicroBatchPush<T> {
        pub fn forward(&mut self, mut data: MicroBatch<T>) -> IOResult<()> {
            match self {
                MicroBatchPush::Pipeline(p) => p.forward_buffer(data),
                MicroBatchPush::Shuffle(p) => {
                    let tag = data.tag.clone();
                    for d in data.drain() {
                        p.push(&tag, d)?;
                    }
                    Ok(())
                }
                MicroBatchPush::ScopeShuffle(p) => p.forward_buffer(data),
                MicroBatchPush::Broadcast(p) => p.forward_buffer(data),
                MicroBatchPush::Aggregate(p) => p.forward_buffer(data),
            }
        }
    }

    impl<T: Data> ScopeStreamBuffer for MicroBatchPush<T> {
        fn scope_size(&self) -> usize {
            match self {
                MicroBatchPush::Pipeline(p) => p.scope_size(),
                MicroBatchPush::Shuffle(p) => p.scope_size(),
                MicroBatchPush::Aggregate(p) => p.scope_size(),
                MicroBatchPush::Broadcast(p) => p.scope_size(),
                MicroBatchPush::ScopeShuffle(p) => p.scope_size(),
            }
        }

        #[inline]
        fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
            match self {
                MicroBatchPush::Pipeline(p) => p.ensure_capacity(tag),
                MicroBatchPush::Shuffle(p) => p.ensure_capacity(tag),
                MicroBatchPush::Aggregate(p) => p.ensure_capacity(tag),
                MicroBatchPush::Broadcast(p) => p.ensure_capacity(tag),
                MicroBatchPush::ScopeShuffle(p) => p.ensure_capacity(tag),
            }
        }

        fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
            match self {
                MicroBatchPush::Pipeline(p) => p.flush_scope(tag),
                MicroBatchPush::Shuffle(p) => p.flush_scope(tag),
                MicroBatchPush::Aggregate(p) => p.flush_scope(tag),
                MicroBatchPush::Broadcast(p) => p.flush_scope(tag),
                MicroBatchPush::ScopeShuffle(p) => p.flush_scope(tag),
            }
        }
    }

    impl<T: Data> ScopeStreamPush<T> for MicroBatchPush<T> {
        fn port(&self) -> Port {
            match self {
                MicroBatchPush::Pipeline(p) => p.port(),
                MicroBatchPush::Shuffle(p) => p.port(),
                MicroBatchPush::Aggregate(p) => p.port(),
                MicroBatchPush::Broadcast(p) => p.port(),
                MicroBatchPush::ScopeShuffle(p) => p.port(),
            }
        }

        #[inline]
        fn push(&mut self, tag: &Tag, msg: T) -> IOResult<()> {
            match self {
                MicroBatchPush::Pipeline(p) => p.push(tag, msg),
                MicroBatchPush::Shuffle(p) => p.push(tag, msg),
                MicroBatchPush::Aggregate(p) => p.push(tag, msg),
                MicroBatchPush::Broadcast(p) => p.push(tag, msg),
                MicroBatchPush::ScopeShuffle(p) => p.push(tag, msg),
            }
        }

        #[inline]
        fn push_last(&mut self, msg: T, end: EndSignal) -> IOResult<()> {
            trace_worker!("out port {:?} push end of {:?}", self.port(), end.tag);
            match self {
                MicroBatchPush::Pipeline(p) => p.push_last(msg, end),
                MicroBatchPush::Shuffle(p) => p.push_last(msg, end),
                MicroBatchPush::Aggregate(p) => p.push_last(msg, end),
                MicroBatchPush::Broadcast(p) => p.push_last(msg, end),
                MicroBatchPush::ScopeShuffle(p) => p.push_last(msg, end),
            }
        }

        fn push_iter<I: Iterator<Item=T>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            match self {
                MicroBatchPush::Pipeline(p) => p.push_iter(tag, iter),
                MicroBatchPush::Shuffle(p) => p.push_iter(tag, iter),
                MicroBatchPush::Aggregate(p) => p.push_iter(tag, iter),
                MicroBatchPush::Broadcast(p) => p.push_iter(tag, iter),
                MicroBatchPush::ScopeShuffle(p) => p.push_iter(tag, iter),
            }
        }

        #[inline]
        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            trace_worker!("out port {:?} notify end of {:?}", self.port(), end.tag);
            match self {
                MicroBatchPush::Pipeline(p) => p.notify_end(end),
                MicroBatchPush::Shuffle(p) => p.notify_end(end),
                MicroBatchPush::Aggregate(p) => p.notify_end(end),
                MicroBatchPush::Broadcast(p) => p.notify_end(end),
                MicroBatchPush::ScopeShuffle(p) => p.notify_end(end),
            }
        }

        #[inline]
        fn flush(&mut self) -> IOResult<()> {
            match self {
                MicroBatchPush::Pipeline(p) => p.flush(),
                MicroBatchPush::Shuffle(p) => p.flush(),
                MicroBatchPush::Aggregate(p) => p.flush(),
                MicroBatchPush::Broadcast(p) => p.flush(),
                MicroBatchPush::ScopeShuffle(p) => p.flush(),
            }
        }

        #[inline]
        fn close(&mut self) -> IOResult<()> {
            match self {
                MicroBatchPush::Pipeline(p) => p.close(),
                MicroBatchPush::Shuffle(p) => p.close(),
                MicroBatchPush::Aggregate(p) => p.close(),
                MicroBatchPush::Broadcast(p) => p.close(),
                MicroBatchPush::ScopeShuffle(p) => p.close(),
            }
        }
    }
}
////////////////////////////////////////////////

#[cfg(feature = "rob")]
mod rob {
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

    #[allow(dead_code)]
    pub(crate) enum MicroBatchPush<T: Data> {
        Local(LocalMicroBatchPush<T>),
        Exchange(ExchangeMicroBatchPush<T>),
    }

    impl<T: Data> Push<MicroBatch<T>> for MicroBatchPush<T> {
        fn push(&mut self, _msg: MicroBatch<T>) -> Result<(), IOError> {
            todo!()
        }

        fn flush(&mut self) -> Result<(), IOError> {
            todo!()
        }

        fn close(&mut self) -> Result<(), IOError> {
            todo!()
        }
    }

    pub trait BlockPush {
        // try to unblock data on scope, return true if the data is unblocked;
        fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError>;
    }

    impl<T: Data> BlockPush for MicroBatchPush<T> {
        fn try_unblock(&mut self, _tag: &Tag) -> Result<bool, IOError> {
            todo!()
        }
    }
}
