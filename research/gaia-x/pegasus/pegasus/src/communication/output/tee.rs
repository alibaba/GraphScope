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

use crate::api::scope::MergedScopeDelta;
use crate::channel_id::ChannelInfo;
use crate::communication::decorator::{DataPush, ScopeStreamBuffer, ScopeStreamPush};
use crate::data::DataSet;
use crate::errors::{IOError, IOResult};
use crate::graph::Port;
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
use pegasus_common::buffer::{Batch, BatchPool, MemBatchPool, MemoryAlloc};
use smallvec::SmallVec;

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
    push: DataPush<D>,
    /// describe the scope change through this push;
    /// buffer for small push;
    buffer: Option<Buffer<D>>,
}

impl<D: Data> ChannelPush<D> {
    pub(crate) fn new(ch_info: ChannelInfo, delta: MergedScopeDelta, push: DataPush<D>) -> Self {
        ChannelPush { ch_info, push, delta, buffer: None }
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
                            self.push.push_iter(&tag, &mut iter)?;
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
                    .push_iter(&b.pin, &mut buf.drain(..))?;
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
                        self.push.push_iter(&tag, &mut buf.drain(..))?;
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

    fn push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        assert_eq!(tag.len(), self.delta.origin_scope_level);
        let tag = self.delta.evolve(tag);
        if let Some(b) = self.buffer.as_mut() {
            if b.pin == tag {
                if !b.buf.is_empty() {
                    let mut buf = std::mem::replace(&mut b.buf, vec![]);
                    self.push.push_iter(&tag, &mut buf.drain(..))?;
                    assert!(buf.is_empty());
                    b.buf = buf;
                    b.cap = 0;
                    b.cap = self.push.ensure_capacity(&tag)?;
                }
            }
        }
        self.push.push_iter(&tag, iter)
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
                                .push_iter(&end.tag, &mut buf.drain(..))?;
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
                    .push_iter(&b.pin, &mut buf.drain(..))?;
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
    buffers: TidyTagMap<SmallVec<[Batch<D>; 2]>>,
    buffer_pool: MemBatchPool<D>,
}

impl<D: Data> Tee<D> {
    pub fn new(port: Port, scope_level: usize, push: ChannelPush<D>) -> Self {
        Tee {
            port,
            main_push: push,
            pushes: SmallVec::new(),
            buffers: TidyTagMap::new(scope_level),
            buffer_pool: BatchPool::new(1023, 128, MemoryAlloc::new()),
        }
    }

    pub fn add_push(&mut self, push: ChannelPush<D>) {
        self.pushes.push(push);
    }

    pub fn forward(&mut self, data: DataSet<D>) -> IOResult<()> {
        if self.pushes.len() == 0 {
            self.main_push.push.forward(data)
        } else {
            for i in 0..self.pushes.len() {
                self.pushes[i].push.forward(data.clone())?;
            }
            self.main_push.push.forward(data)
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

    fn push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        let len = self.pushes.len();
        if len == 0 {
            self.main_push.push_iter(tag, iter)
        } else {
            let mut buffer_map = std::mem::replace(&mut self.buffers, Default::default());
            let buffers = buffer_map.get_mut_or_insert(tag);
            if buffers.is_empty() {
                let b = self
                    .buffer_pool
                    .fetch()
                    .unwrap_or(Batch::with_capacity(1023));
                buffers.push(b);
            }
            let mut cor = Iter::new(iter, buffers);
            let mut error = None;
            if let Err(e) = self.main_push.push_iter(tag, &mut cor) {
                if !e.is_interrupted() && !e.is_would_block() {
                    return Err(e);
                } else {
                    error = Some(e);
                }
            }

            let mut errors = Vec::new();
            for (i, buf) in buffers.iter_mut().enumerate() {
                if !buf.is_empty() {
                    if let Err(e) = self.pushes[i].push_iter(tag, buf) {
                        if e.is_interrupted() {
                            errors.push(0);
                        } else if e.is_would_block() {
                            errors.push(1);
                        } else {
                            return Err(e);
                        }
                    }
                }
            }

            if errors.is_empty() {
                assert!(buffers.iter().all(|b| b.is_empty()));
                buffers.clear();
            } else {
                if buffers.iter().all(|b| b.is_empty()) {
                    buffers.clear();
                }
            }
            self.buffers = buffer_map;
            if let Some(err) = error {
                if err.is_interrupted() {
                    return Err(err);
                } else {
                    if errors.iter().any(|x| *x == 0) {
                        return interrupt!("tee");
                    } else {
                        return Err(err);
                    }
                }
            } else if !errors.is_empty() {
                if errors.iter().any(|x| *x == 0) {
                    return interrupt!("tee");
                } else {
                    return would_block!("tee");
                }
            } else {
                Ok(())
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
    buf: &'a mut [Batch<D>],
}

impl<'a, D, I> Iter<'a, D, I> {
    pub fn new(iter: &'a mut I, buf: &'a mut [Batch<D>]) -> Self {
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
                b.push(next.clone());
            }
            Some(next)
        } else {
            None
        }
    }
}
