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

use std::cell::RefCell;
use std::rc::Rc;

use ahash::AHashSet;
pub(crate) use rob::*;

use crate::api::scope::MergedScopeDelta;
use crate::communication::cancel::{CancelHandle, CancelListener};
use crate::tag::tools::map::TidyTagMap;
use crate::Tag;

struct ChannelCancel {
    scope_level: u32,
    inner: CancelHandle,
    delta: MergedScopeDelta,
    /// used to check cancel before evolve and push;
    before_enter: TidyTagMap<()>,
    parent: AHashSet<Tag>,
}

impl CancelListener for ChannelCancel {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        let tag = self.inner.cancel(tag, to)?;
        let level = tag.len() as u32;

        if self.delta.scope_level_delta < 0 {
            // channel send data to parent scope;
            // cancel from parent scope;
            assert!(level < self.before_enter.scope_level);
            if *crate::config::ENABLE_CANCEL_CHILD {
                self.parent.insert(tag.clone());
                return Some(tag.clone());
            } else {
                None
            }
        } else {
            // scope_level delta > 0;
            if level == self.scope_level {
                let before_enter = self.delta.evolve_back(&tag);
                self.before_enter
                    .insert(before_enter.clone(), ());
                Some(before_enter)
            } else if level < self.scope_level {
                // cancel from parent scope;
                if *crate::config::ENABLE_CANCEL_CHILD {
                    self.parent.insert(tag.clone());
                    Some(tag.clone())
                } else {
                    None
                }
            } else {
                warn_worker!(
                    "unexpected cancel of scope {:?} expect scope level {};",
                    tag,
                    self.scope_level
                );
                // ignore:
                None
            }
        }
    }
}

impl ChannelCancel {
    fn is_canceled(&self, tag: &Tag) -> bool {
        let level = tag.len() as u32;
        if level == self.before_enter.scope_level {
            if !self.before_enter.is_empty() && self.before_enter.contains_key(tag) {
                return true;
            }

            if *crate::config::ENABLE_CANCEL_CHILD && !self.parent.is_empty() {
                let p = tag.to_parent_uncheck();
                self.check_parent(p)
            } else {
                false
            }
        } else if level < self.before_enter.scope_level {
            self.check_parent(tag.clone())
        } else {
            false
        }
    }

    fn check_parent(&self, mut p: Tag) -> bool {
        loop {
            if self.parent.contains(&p) {
                return true;
            }
            if p.is_root() {
                break;
            } else {
                p = p.to_parent_uncheck();
            }
        }

        false
    }
}

#[derive(Clone)]
pub(crate) struct ChannelCancelPtr {
    inner: Rc<RefCell<ChannelCancel>>,
}

impl CancelListener for ChannelCancelPtr {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        self.inner.borrow_mut().cancel(tag, to)
    }
}

impl ChannelCancelPtr {
    fn new(scope_level: u32, delta: MergedScopeDelta, ch: CancelHandle) -> Self {
        let level_before = delta.origin_scope_level as u32;
        let inner = ChannelCancel {
            scope_level,
            inner: ch,
            delta,
            before_enter: TidyTagMap::new(level_before),
            parent: AHashSet::new(),
        };
        ChannelCancelPtr { inner: Rc::new(RefCell::new(inner)) }
    }

    fn is_canceled(&self, tag: &Tag) -> bool {
        self.inner.borrow().is_canceled(tag)
    }
}

unsafe impl Send for ChannelCancelPtr {}

#[cfg(not(feature = "rob"))]
mod rob {
    use nohash_hasher::IntSet;
    use pegasus_common::buffer::{Batch, BatchPool, MemBatchPool, MemBufAlloc};
    use smallvec::SmallVec;

    use super::*;
    use crate::api::scope::MergedScopeDelta;
    use crate::channel_id::ChannelInfo;
    use crate::communication::cancel::CancelHandle;
    use crate::communication::decorator::{MicroBatchPush, ScopeStreamBuffer, ScopeStreamPush};
    use crate::data::MicroBatch;
    use crate::errors::{IOError, IOResult};
    use crate::graph::Port;
    use crate::progress::EndSignal;
    use crate::{Data, Tag};

    struct Buffer<D> {
        pin: Tag,
        buf: Vec<D>,
        cap: usize,
    }

    impl<D> Buffer<D> {
        pub fn new() -> Self {
            Buffer { pin: Tag::Root, buf: vec![], cap: 0 }
        }

        #[inline]
        fn is_full(&self) -> bool {
            self.buf.len() == self.cap
        }
    }

    pub(crate) struct ChannelPush<D: Data> {
        /// information about the channel;
        pub ch_info: ChannelInfo,
        pub(crate) delta: MergedScopeDelta,
        /// the push side of underlying channel;
        push: MicroBatchPush<D>,
        /// describe the scope change through this push;
        /// buffer for small push;
        buffer: Option<Buffer<D>>,
        cancel_handle: ChannelCancelPtr,
    }

    impl<D: Data> ChannelPush<D> {
        pub(crate) fn new(
            ch_info: ChannelInfo, delta: MergedScopeDelta, push: MicroBatchPush<D>, cancel: CancelHandle,
        ) -> Self {
            let cancel_handle = ChannelCancelPtr::new(ch_info.scope_level, delta.clone(), cancel);
            ChannelPush { ch_info, push, delta, buffer: None, cancel_handle }
        }

        pub(crate) fn get_cancel_handle(&self) -> ChannelCancelPtr {
            self.cancel_handle.clone()
        }

        #[inline]
        fn is_canceled(&self, tag: &Tag) -> bool {
            self.cancel_handle.is_canceled(tag)
        }

        fn push_without_evolve(&mut self, tag: Tag, msg: D) -> IOResult<()> {
            if let Some(b) = self.buffer.as_mut() {
                if &b.pin == &tag {
                    if b.cap > 0 {
                        b.buf.push(msg);
                        if b.is_full() {
                            let mut buf = std::mem::replace(&mut b.buf, vec![]);
                            {
                                let mut iter = buf.drain(..);
                                // as capacity is ensured in advance, there should no block errors;
                                self.push.try_push_iter(&tag, &mut iter)?;
                            }
                            assert!(buf.is_empty());
                            b.buf = buf;
                            // ensure next capacity for following push; return block error if
                            // no space;
                            b.cap = 0;
                            b.cap = self.push.ensure_capacity(&tag)?;
                        }
                        return Ok(());
                    }
                } else if !b.buf.is_empty() {
                    let mut buf = std::mem::replace(&mut b.buf, vec![]);
                    // as capacity is ensured in advance, there should no block errors;
                    self.push
                        .try_push_iter(&b.pin, &mut buf.drain(..))?;
                    b.buf = buf;
                }
            }

            match self.push.ensure_capacity(&tag) {
                Ok(cap) => {
                    assert!(cap > 0);
                    if cap == 1 {
                        self.push.push(&tag, msg)?;
                    } else {
                        let buf = self.buffer.get_or_insert_with(Buffer::new);
                        buf.pin = tag;
                        buf.cap = cap;
                        buf.buf.push(msg);
                    }
                }
                Err(_) => {
                    // push the message and return block hint;
                    self.push.push(&tag, msg)?;
                }
            }
            Ok(())
        }
    }

    impl<D: Data> ScopeStreamBuffer for ChannelPush<D> {
        fn scope_size(&self) -> usize {
            self.push.scope_size()
        }

        #[inline]
        fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
            let tag = self.delta.evolve(tag);
            self.push.ensure_capacity(&tag)
        }

        #[inline]
        fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
            let tag = self.delta.evolve(tag);
            self.push.flush_scope(&tag)
        }
    }

    impl<D: Data> ScopeStreamPush<D> for ChannelPush<D> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
            assert_eq!(tag.len(), self.delta.origin_scope_level);
            if self.cancel_handle.is_canceled(&tag) {
                return Ok(());
            }
            let tag = self.delta.evolve(tag);
            self.push_without_evolve(tag, msg)
        }

        fn push_last(&mut self, msg: D, mut end: EndSignal) -> IOResult<()> {
            assert_eq!(end.tag.len(), self.delta.origin_scope_level);
            let tag = self.delta.evolve(&end.tag);
            let old_tag = std::mem::replace(&mut end.tag, tag.clone());
            if self.delta.scope_level_delta >= 0 {
                if let Some(b) = self.buffer.as_mut() {
                    if b.pin == tag {
                        if !b.buf.is_empty() {
                            let mut buf = std::mem::replace(&mut b.buf, vec![]);
                            self.push
                                .try_push_iter(&tag, &mut buf.drain(..))?;
                            b.buf = buf;
                        }
                    }
                }
                if self.delta.scope_level_delta == 0 {
                    self.push.push_last(msg, end)
                } else {
                    // to child;
                    self.push.push_last(msg, end.clone())?;
                    end.tag = old_tag;
                    self.push.notify_end(end)
                }
            } else {
                // leave :
                self.push_without_evolve(tag, msg)
            }
        }

        fn try_push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            assert_eq!(tag.len(), self.delta.origin_scope_level);
            if self.cancel_handle.is_canceled(tag) {
                return Ok(());
            }
            let tag = self.delta.evolve(tag);
            if let Some(b) = self.buffer.as_mut() {
                if b.pin == tag {
                    if !b.buf.is_empty() {
                        let mut buf = std::mem::replace(&mut b.buf, vec![]);
                        self.push
                            .try_push_iter(&tag, &mut buf.drain(..))?;
                        assert!(buf.is_empty());
                        b.buf = buf;
                        b.cap = 0;
                        b.cap = self.push.ensure_capacity(&tag)?;
                    }
                }
            }
            self.push.try_push_iter(&tag, iter)
        }

        #[inline]
        fn notify_end(&mut self, mut end: EndSignal) -> IOResult<()> {
            if end.tag.len() == self.delta.origin_scope_level {
                let tag = self.delta.evolve(&end.tag);
                let old_tag = end.tag.clone();
                end.tag = tag.clone();
                if let Some(b) = self.buffer.as_mut() {
                    if b.pin == tag {
                        if !b.buf.is_empty() {
                            let mut buf = std::mem::replace(&mut b.buf, vec![]);
                            if buf.len() == 1 {
                                let msg = buf.pop().unwrap();
                                b.buf = buf;
                                b.cap = 0;
                                if self.delta.scope_level_delta > 0 {
                                    self.push.push_last(msg, end.clone())?;
                                    end.tag = old_tag;
                                    self.push.notify_end(end)?;
                                } else {
                                    self.push.push_last(msg, end)?;
                                }
                                return Ok(());
                            } else {
                                self.push
                                    .try_push_iter(&end.tag, &mut buf.drain(..))?;
                                b.buf = buf;
                                b.cap = 0;
                            }
                        }
                    }
                }

                if self.delta.scope_level_delta > 0 {
                    // to child:
                    self.push.notify_end(end.clone())?;
                    end.tag = old_tag;
                    self.push.notify_end(end)
                } else if self.delta.scope_level_delta == 0 {
                    self.push.notify_end(end)
                } else {
                    // leave to parent:
                    if log_enabled!(log::Level::Trace) {
                        trace_worker!("ignore end of child scope {:?};", old_tag);
                    }
                    Ok(())
                }
            } else {
                // end of parent scope;
                assert!(end.tag.len() < self.delta.origin_scope_level);
                self.flush()?;
                self.push.notify_end(end)
            }
        }

        #[inline]
        fn flush(&mut self) -> IOResult<()> {
            if let Some(b) = self.buffer.as_mut() {
                if !b.buf.is_empty() {
                    let mut buf = std::mem::replace(&mut b.buf, vec![]);
                    self.push
                        .try_push_iter(&b.pin, &mut buf.drain(..))?;
                    assert!(buf.is_empty());
                    b.buf = buf;
                    b.cap = 0;
                }
            }
            self.push.flush()
        }

        fn close(&mut self) -> IOResult<()> {
            self.push.close()?;
            Ok(())
        }
    }

    pub(crate) struct Tee<D: Data> {
        pub port: Port,
        main_push: ChannelPush<D>,
        pushes: SmallVec<[ChannelPush<D>; 2]>,
        buffer_pool: MemBatchPool<D>,
    }

    impl<D: Data> Tee<D> {
        pub fn new(port: Port, _scope_level: u32, push: ChannelPush<D>) -> Self {
            Tee {
                port,
                main_push: push,
                pushes: SmallVec::new(),
                buffer_pool: BatchPool::new(1023, 128, MemBufAlloc::new()),
            }
        }

        pub fn add_push(&mut self, push: ChannelPush<D>) {
            self.pushes.push(push);
        }

        pub fn forward(&mut self, data: MicroBatch<D>) -> IOResult<()> {
            if self.pushes.len() == 0 {
                self.main_push.push.push_batch(data)
            } else {
                for i in 0..self.pushes.len() {
                    self.pushes[i].push.push_batch(data.clone())?;
                }
                self.main_push.push.push_batch(data)
            }
        }
    }

    impl<D: Data> ScopeStreamBuffer for Tee<D> {
        fn scope_size(&self) -> usize {
            self.main_push.scope_size()
        }

        fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
            self.main_push.ensure_capacity(tag)
        }

        fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
            self.main_push.flush_scope(tag)?;

            for p in self.pushes.iter_mut() {
                p.flush_scope(tag)?;
            }
            Ok(())
        }
    }

    impl<D: Data> ScopeStreamPush<D> for Tee<D> {
        fn port(&self) -> Port {
            self.port
        }

        fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
            let len = self.pushes.len();
            if len == 0 {
                self.main_push.push(tag, msg)
            } else {
                // to do flow control in tee:
                if let Err(e) = self.main_push.push(tag, msg.clone()) {
                    if e.is_would_block() || e.is_interrupted() {
                        // although error returned, the message was pushed;
                        if len == 1 {
                            self.pushes[0].push(tag, msg)?;
                        } else {
                            for p in &mut self.pushes[1..len - 1] {
                                p.push(tag, msg.clone())?;
                            }
                            self.pushes[0].push(tag, msg)?;
                        }
                    }
                    Err(e)
                } else {
                    // if first push send message successfully, other pushes won't get error caused by flow control;
                    if len == 1 {
                        self.pushes[0].push(tag, msg)
                    } else {
                        for p in &mut self.pushes[1..len - 1] {
                            p.push(tag, msg.clone())?;
                        }
                        self.pushes[0].push(tag, msg)
                    }
                }
            }
        }

        fn push_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
            let len = self.pushes.len();

            if len == 0 {
                self.main_push.push_last(msg, end)
            } else {
                if let Err(e) = self
                    .main_push
                    .push_last(msg.clone(), end.clone())
                {
                    if e.is_would_block() || e.is_interrupted() {
                        // although error returned, the message was pushed;
                        if len == 1 {
                            self.pushes[0].push_last(msg, end)?;
                        } else {
                            for p in &mut self.pushes[1..len - 1] {
                                p.push_last(msg.clone(), end.clone())?;
                            }
                            self.pushes[0].push_last(msg, end)?;
                        }
                    }
                    Err(e)
                } else {
                    // if first push send message successfully, other pushes won't get error caused by flow control;
                    if len == 1 {
                        self.pushes[0].push_last(msg, end)
                    } else {
                        for p in &mut self.pushes[1..len - 1] {
                            p.push_last(msg.clone(), end.clone())?;
                        }
                        self.pushes[0].push_last(msg, end)
                    }
                }
            }
        }

        fn try_push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            let len = self.pushes.len();
            if len == 0 {
                self.main_push.try_push_iter(tag, iter)
            } else {
                // if no cancel has been triggered, the 'stat' won't be allocated;
                let mut stat = IntSet::default();
                for (i, p) in self.pushes.iter().enumerate() {
                    if p.is_canceled(tag) {
                        stat.insert(i);
                    }
                }

                if self.main_push.is_canceled(tag) {
                    if stat.len() == self.pushes.len() {
                        // all canceled;
                        Ok(())
                    } else if self.pushes.len() == 1 {
                        self.pushes[0].try_push_iter(tag, iter)
                    } else {
                        // corner case;
                        if self.pushes.len() - stat.len() == 1 {
                            for (i, p) in self.pushes.iter_mut().enumerate() {
                                if !stat.contains(&i) {
                                    return p.try_push_iter(tag, iter);
                                }
                            }
                            Ok(())
                        } else {
                            let mut queue = vec![];
                            for i in 0..self.pushes.len() {
                                if !stat.contains(&i) {
                                    let b = self
                                        .buffer_pool
                                        .fetch()
                                        .unwrap_or(Batch::with_capacity(1024));
                                    queue.push((i, b));
                                }
                            }
                            assert!(queue.len() > 1);
                            let first = queue.swap_remove(queue.len() - 1);
                            let mut cor = Iter::new(iter, queue.as_mut_slice());
                            let mut has_block = false;
                            if let Err(err) = self.pushes[first.0].try_push_iter(tag, &mut cor) {
                                if err.is_would_block() || err.is_interrupted() {
                                    has_block = true;
                                } else {
                                    return Err(err);
                                }
                            }

                            for (i, mut b) in queue.drain(..) {
                                if let Err(err) = self.pushes[i].try_push_iter(tag, &mut b) {
                                    return if err.is_would_block() || err.is_interrupted() {
                                        error_worker!("can't handle block of tee push;");
                                        Err(IOError::cannot_block())
                                    } else {
                                        Err(err)
                                    };
                                }
                            }
                            if has_block {
                                would_block!("main push block in tee;")
                            } else {
                                Ok(())
                            }
                        }
                    }
                } else {
                    if stat.len() == self.pushes.len() {
                        self.main_push.try_push_iter(tag, iter)
                    } else {
                        let mut queue = vec![];
                        for i in 0..self.pushes.len() {
                            if !stat.contains(&i) {
                                let b = self
                                    .buffer_pool
                                    .fetch()
                                    .unwrap_or(Batch::with_capacity(1024));
                                queue.push((i, b));
                            }
                        }
                        let mut cor = Iter::new(iter, queue.as_mut_slice());
                        let mut has_block = false;
                        if let Err(err) = self.main_push.try_push_iter(tag, &mut cor) {
                            if err.is_would_block() || err.is_interrupted() {
                                has_block = true;
                            } else {
                                return Err(err);
                            }
                        }

                        for (i, mut b) in queue.drain(..) {
                            if let Err(err) = self.pushes[i].try_push_iter(tag, &mut b) {
                                if err.is_would_block() || err.is_interrupted() {
                                    if !b.is_empty() {
                                        error_worker!(
                                            "can't handle block of tee push {} on port {:?};",
                                            i,
                                            self.port
                                        );
                                        return Err(IOError::cannot_block());
                                    }
                                } else {
                                    return Err(err);
                                }
                            }
                        }
                        if has_block {
                            would_block!("main push block in tee;")
                        } else {
                            Ok(())
                        }
                    }
                }
            }
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            if self.pushes.len() == 0 {
                self.main_push.notify_end(end)
            } else {
                self.main_push.notify_end(end.clone())?;

                if self.pushes.len() > 1 {
                    for p in &mut self.pushes[1..] {
                        p.notify_end(end.clone())?;
                    }
                }
                if self.pushes.len() > 0 {
                    self.pushes[0].notify_end(end)?;
                }
                Ok(())
            }
        }

        fn flush(&mut self) -> IOResult<()> {
            if self.pushes.len() == 0 {
                self.main_push.flush()
            } else {
                let mut error = vec![];
                if let Err(err) = self.main_push.flush() {
                    error.push(err);
                }
                for p in self.pushes.iter_mut() {
                    if let Err(err) = p.flush() {
                        error.push(err);
                    }
                }
                if !error.is_empty() {
                    Err(error.remove(0))
                } else {
                    Ok(())
                }
            }
        }

        fn close(&mut self) -> IOResult<()> {
            if self.pushes.len() == 0 {
                self.main_push.close()
            } else {
                let mut error = vec![];
                if let Err(e) = self.main_push.close() {
                    error.push(e);
                }
                for p in self.pushes.iter_mut() {
                    if let Err(err) = p.close() {
                        error.push(err);
                    }
                }
                if !error.is_empty() {
                    Err(error.remove(0))
                } else {
                    Ok(())
                }
            }
        }
    }

    struct Iter<'a, D, I> {
        iter: &'a mut I,
        buf: &'a mut [(usize, Batch<D>)],
    }

    impl<'a, D, I> Iter<'a, D, I> {
        pub fn new(iter: &'a mut I, buf: &'a mut [(usize, Batch<D>)]) -> Self {
            Iter { iter, buf }
        }
    }

    impl<'a, D, I> Iterator for Iter<'a, D, I>
    where
        D: Clone,
        I: Iterator<Item = D>,
    {
        type Item = D;

        fn next(&mut self) -> Option<Self::Item> {
            if let Some(next) = self.iter.next() {
                for b in self.buf.iter_mut() {
                    b.1.push(next.clone());
                }
                Some(next)
            } else {
                None
            }
        }
    }
}
///////////////////////////////////////////////////////////

#[cfg(feature = "rob")]
mod rob {
    use pegasus_common::buffer::ReadBuffer;

    use crate::api::scope::MergedScopeDelta;
    use crate::channel_id::ChannelInfo;
    use crate::communication::cancel::CancelHandle;
    use crate::communication::decorator::{BlockPush, MicroBatchPush};
    use crate::communication::output::tee::ChannelCancelPtr;
    use crate::communication::IOResult;
    use crate::data::MicroBatch;
    use crate::data_plane::Push;
    use crate::errors::IOError;
    use crate::graph::Port;
    use crate::{Data, Tag};

    #[allow(dead_code)]
    pub(crate) struct ChannelPush<D: Data> {
        pub ch_info: ChannelInfo,
        pub src: u32,
        pub(crate) delta: MergedScopeDelta,
        push: MicroBatchPush<D>,
        cancel_handle: ChannelCancelPtr,
    }

    impl<D: Data> ChannelPush<D> {
        pub(crate) fn new(
            ch_info: ChannelInfo, delta: MergedScopeDelta, push: MicroBatchPush<D>, ch: CancelHandle,
        ) -> Self {
            let src = crate::worker_id::get_current_worker().index;
            let cancel_handle = ChannelCancelPtr::new(ch_info.scope_level, delta.clone(), ch);
            ChannelPush { ch_info, src, delta, push, cancel_handle }
        }

        pub(crate) fn get_cancel_handle(&self) -> ChannelCancelPtr {
            self.cancel_handle.clone()
        }

        #[inline]
        fn is_canceled(&self, tag: &Tag) -> bool {
            self.cancel_handle.is_canceled(tag)
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for ChannelPush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            if self.is_canceled(&batch.tag) {
                if batch.is_last() {
                    batch.clear();
                } else {
                    return Ok(());
                }
            }

            if batch.tag.len() == self.delta.origin_scope_level {
                let tag = self.delta.evolve(&batch.tag);
                if let Some(end) = batch.take_end() {
                    if self.delta.scope_level_delta > 0 {
                        // enter
                        let end_cp = end.clone();
                        batch.set_end(end);
                        batch.set_tag(tag);
                        self.push.push(batch)?;
                        let mut p = MicroBatch::new(end_cp.tag.clone(), self.src, ReadBuffer::new());
                        p.set_end(end_cp);
                        self.push.push(p)
                    } else if self.delta.scope_level_delta == 0 {
                        batch.set_end(end);
                        batch.set_tag(tag);
                        self.push.push(batch)
                    } else {
                        // leave:
                        batch.set_tag(tag);
                        self.push.push(batch)
                    }
                } else if !batch.is_empty() {
                    batch.set_tag(tag);
                    self.push.push(batch)
                } else {
                    //ignore;
                    Ok(())
                }
            } else if batch.tag.len() < self.delta.origin_scope_level {
                assert!(batch.is_empty(), "batch from parent is not empty;");
                assert!(batch.is_last(), "batch from parent is not last;");
                self.push.push(batch)
            } else {
                unreachable!("unrecognized batch from child scope {:?}", batch.tag);
            }
        }

        fn flush(&mut self) -> Result<(), IOError> {
            trace_worker!(
                "output[{:?}] flush channel [{}]",
                self.ch_info.source_port,
                self.ch_info.index()
            );
            self.push.flush()
        }

        fn close(&mut self) -> Result<(), IOError> {
            self.push.close()
        }
    }

    impl<D: Data> BlockPush for ChannelPush<D> {
        fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError> {
            self.push.try_unblock(tag)
        }

        fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()> {
            self.push.clean_block_of(tag)
        }
    }

    #[allow(dead_code)]
    pub(crate) struct Tee<D: Data> {
        port: Port,
        scope_level: u32,
        main_push: ChannelPush<D>,
        other_pushes: Vec<ChannelPush<D>>,
    }

    impl<D: Data> Tee<D> {
        pub fn new(port: Port, scope_level: u32, push: ChannelPush<D>) -> Self {
            Tee { port, scope_level, main_push: push, other_pushes: Vec::new() }
        }

        pub fn add_push(&mut self, push: ChannelPush<D>) {
            self.other_pushes.push(push);
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for Tee<D> {
        fn push(&mut self, mut msg: MicroBatch<D>) -> Result<(), IOError> {
            let mut would_block = false;
            if !self.other_pushes.is_empty() {
                for tx in self.other_pushes.iter_mut() {
                    let msg_cp = msg.share();
                    if let Err(err) = tx.push(msg_cp) {
                        if err.is_would_block() {
                            trace_worker!(
                                "tee[{:?}] other push blocked on push batch of {:?} ;",
                                self.port,
                                msg.tag
                            );
                            would_block = true;
                        } else {
                            return Err(err);
                        }
                    }
                }
            }
            match self.main_push.push(msg) {
                Ok(_) => {
                    if would_block {
                        would_block!("underlying channel push blocked")
                    } else {
                        Ok(())
                    }
                }
                Err(err) => {
                    if err.is_would_block() {
                        trace_worker!("tee[{:?}] main push blocked on push batch;", self.port,);
                    }
                    Err(err)
                }
            }
        }

        fn flush(&mut self) -> Result<(), IOError> {
            self.main_push.flush()?;
            for o in self.other_pushes.iter_mut() {
                o.flush()?;
            }
            Ok(())
        }

        fn close(&mut self) -> Result<(), IOError> {
            self.main_push.close()?;
            for o in self.other_pushes.iter_mut() {
                o.close()?;
            }
            Ok(())
        }
    }

    impl<D: Data> BlockPush for Tee<D> {
        fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError> {
            let mut would_block = self.main_push.try_unblock(tag)?;
            for o in self.other_pushes.iter_mut() {
                would_block |= o.try_unblock(tag)?;
            }
            Ok(would_block)
        }

        fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()> {
            self.main_push.clean_block_of(tag)?;
            for o in self.other_pushes.iter_mut() {
                o.clean_block_of(tag)?;
            }
            Ok(())
        }
    }
}
