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

use std::cell::RefMut;
use std::collections::VecDeque;

use pegasus_common::buffer::ReadBuffer;

use crate::communication::buffer::ScopeBufferPool;
use crate::communication::decorator::{BlockPush, ScopeStreamPush};
use crate::communication::output::builder::OutputMeta;
use crate::communication::output::tee::Tee;
use crate::communication::output::BlockScope;
use crate::communication::IOResult;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::EndOfScope;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

enum BlockEntry<D: Data> {
    Single(D),
    LastSingle(D, EndOfScope),
    DynIter(Option<D>, Box<dyn Iterator<Item = D> + Send + 'static>),
}

pub struct OutputHandle<D: Data> {
    pub port: Port,
    pub scope_level: u32,
    pub src: u32,
    tee: Tee<D>,
    buf_pool: ScopeBufferPool<D>,
    block_entries: TidyTagMap<BlockEntry<D>>,
    blocks: VecDeque<BlockScope>,
    seq_emit: TidyTagMap<u64>,
    is_closed: bool,
    current_skips: TidyTagMap<()>,
    parent_skips: TidyTagMap<()>,
}

impl<D: Data> OutputHandle<D> {
    pub(crate) fn new(meta: OutputMeta, output: Tee<D>) -> Self {
        let batch_capacity = meta.batch_capacity as usize;
        let scope_level = meta.scope_level;
        debug_worker!(
            "init output[{:?}] with batch_size={}, batch_capacity={}",
            meta.port,
            meta.batch_size,
            batch_capacity
        );
        let buf_pool = ScopeBufferPool::new(meta.batch_size, batch_capacity, scope_level);
        let src = crate::worker_id::get_current_worker().index;
        let parent_level = if scope_level == 0 { 0 } else { scope_level - 1 };
        OutputHandle {
            port: meta.port,
            scope_level,
            src,
            tee: output,
            buf_pool,
            block_entries: TidyTagMap::new(scope_level),
            blocks: VecDeque::new(),
            seq_emit: TidyTagMap::new(scope_level),
            is_closed: false,
            current_skips: TidyTagMap::new(scope_level),
            parent_skips: TidyTagMap::new(parent_level),
        }
    }

    pub fn push_iter<I: Iterator<Item = D> + Send + 'static>(
        &mut self, tag: &Tag, mut iter: I,
    ) -> IOResult<()> {
        if self.is_skipped(tag) {
            return Ok(());
        }

        match self.try_push_iter_inner(tag, &mut iter) {
            Ok(None) => Ok(()),
            Ok(Some(item)) => {
                trace_worker!("output[{:?}] blocked on push iterator of {:?} ;", self.port, tag);
                self.block_entries
                    .insert(tag.clone(), BlockEntry::DynIter(Some(item), Box::new(iter)));
                self.blocks
                    .push_back(BlockScope::new(tag.clone()));
                would_block!("push iterator")
            }
            Err(e) => {
                if e.is_would_block() {
                    trace_worker!("output[{:?}] blocked on push iterator of {:?} ;", self.port, tag);
                    self.block_entries
                        .insert(tag.clone(), BlockEntry::DynIter(None, Box::new(iter)));
                    self.blocks
                        .push_back(BlockScope::new(tag.clone()));
                    would_block!("push iterator")
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn push_batch(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
        if self.is_skipped(&batch.tag) {
            if batch.is_last() {
                batch.take_data();
            } else {
                return Ok(());
            }
        }

        self.send_batch(batch)
    }

    pub fn push_batch_mut(&mut self, batch: &mut MicroBatch<D>) -> IOResult<()> {
        if self.is_skipped(&batch.tag) {
            if batch.is_last() {
                batch.take_data();
            } else {
                return Ok(());
            }
        }
        let mut new_batch = MicroBatch::new(batch.tag.clone(), self.src, batch.take_data());
        if let Some(end) = batch.take_end() {
            new_batch.set_end(end);
        }
        self.send_batch(new_batch)
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub(crate) fn pin(&mut self, tag: &Tag) {
        self.buf_pool.pin(tag);
    }

    pub(crate) fn try_unblock(&mut self) -> IOResult<()> {
        let len = self.blocks.len();
        if len > 0 {
            for _ in 0..len {
                if let Some(bk) = self.blocks.pop_front() {
                    trace_worker!("output[{:?}] try to unblock data of {:?};", self.port, bk.tag());
                    if self.tee.try_unblock(bk.tag())? {
                        if let Some(iter) = self.block_entries.remove(bk.tag()) {
                            match iter {
                                BlockEntry::Single(x) => {
                                    if let Err(e) = self.push(bk.tag(), x) {
                                        if e.is_would_block() {
                                            let b = self.blocks.pop_back().expect("can't be none");
                                            assert_eq!(b.tag, bk.tag);
                                            self.blocks.push_back(bk);
                                        } else {
                                            return Err(e);
                                        }
                                    }
                                }
                                BlockEntry::LastSingle(x, e) => {
                                    self.push_last(x, e)?;
                                }
                                BlockEntry::DynIter(head, iter) => {
                                    if let Some(x) = head {
                                        if let Err(e) = self.push(bk.tag(), x) {
                                            if e.is_would_block() {
                                                if let Some(blocked) = self.block_entries.remove(bk.tag()) {
                                                    match blocked {
                                                        BlockEntry::Single(x) => {
                                                            self.block_entries.insert(
                                                                bk.tag().clone(),
                                                                BlockEntry::DynIter(Some(x), iter),
                                                            );
                                                        }
                                                        _ => unreachable!("unexpected blocking;"),
                                                    }
                                                } else {
                                                    self.block_entries.insert(
                                                        bk.tag().clone(),
                                                        BlockEntry::DynIter(None, iter),
                                                    );
                                                }
                                                self.blocks.push_back(bk);
                                            }
                                            return Err(e);
                                        }
                                    }

                                    self.push_box_iter(bk, iter)?;
                                }
                            }
                            // trace_worker!("output[{:?}]: data in scope {:?} unblocked", self.port, tag);
                        } else {
                            // trace_worker!("output[{:?}]: data in scope {:?} unblocked", self.port, tag);
                        }
                    } else {
                        self.blocks.push_back(bk);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn get_blocks(&self) -> &VecDeque<BlockScope> {
        &self.blocks
    }

    pub(crate) fn cancel(&mut self, tag: &Tag) -> IOResult<()> {
        let level = tag.len() as u32;
        if level < self.scope_level {
            //assert_eq!(level + 1, self.scope_level);
            self.parent_skips.insert(tag.clone(), ());
            // skip from parent scope;
            if *crate::config::ENABLE_CANCEL_CHILD {
                let block_len = self.blocks.len();
                for _ in 0..block_len {
                    if let Some(b) = self.blocks.pop_front() {
                        if tag.is_parent_of(b.tag()) {
                            if let Some(b) = self.block_entries.remove(b.tag()) {
                                match b {
                                    BlockEntry::LastSingle(_, end) => {
                                        self.notify_end(end)?;
                                    }
                                    _ => (),
                                }
                            }
                        } else {
                            self.blocks.push_back(b);
                        }
                    }
                }
                self.buf_pool.skip_buf(tag);
                self.tee.clean_block_of(tag)?;
            }
        } else if level == self.scope_level {
            // skip current scope level;
            self.current_skips.insert(tag.clone(), ());
            self.buf_pool.skip_buf(tag);
            let block_len = self.blocks.len();
            for _ in 0..block_len {
                if let Some(b) = self.blocks.pop_front() {
                    if tag == b.tag() {
                        trace_worker!(
                            "output[{:?}] clean blocking data of {:?} as canceled;",
                            self.port,
                            b.tag()
                        );
                        if let Some(b) = self.block_entries.remove(b.tag()) {
                            match b {
                                BlockEntry::LastSingle(_, end) => {
                                    self.notify_end(end)?;
                                }
                                _ => (),
                            }
                        }
                    } else {
                        self.blocks.push_back(b);
                    }
                }
            }
            self.tee.clean_block_of(tag)?;
        } else {
            // skip from child scopes;
            // ignore;
        }
        Ok(())
    }

    #[inline]
    fn is_skipped(&self, tag: &Tag) -> bool {
        let level = tag.len() as u32;
        if level == self.scope_level {
            (!self.current_skips.is_empty() && self.current_skips.contains_key(tag))
                || (level >= 1
                    && !self.parent_skips.is_empty()
                    && self
                        .parent_skips
                        .contains_key(&tag.to_parent_uncheck()))
        } else if level < self.scope_level {
            *crate::config::ENABLE_CANCEL_CHILD
                && !self.parent_skips.is_empty()
                && self.parent_skips.contains_key(tag)
        } else {
            false
        }
    }

    #[inline]
    fn push_box_iter(
        &mut self, bks: BlockScope, mut iter: Box<dyn Iterator<Item = D> + Send + 'static>,
    ) -> IOResult<()> {
        match self.try_push_iter_inner(bks.tag(), &mut iter) {
            Ok(None) => Ok(()),
            Ok(Some(item)) => {
                self.block_entries
                    .insert(bks.tag().clone(), BlockEntry::DynIter(Some(item), iter));
                self.blocks.push_back(bks);
                would_block!("unblock push iter")
            }
            Err(e) => {
                if e.is_would_block() {
                    self.block_entries
                        .insert(bks.tag().clone(), BlockEntry::DynIter(None, iter));
                    self.blocks.push_back(bks);
                    would_block!("unblock push iter")
                } else {
                    Err(e)
                }
            }
        }
    }

    #[inline]
    fn send_batch(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
        assert_eq!(batch.tag().len(), self.scope_level as usize);
        if batch.is_empty() {
            if batch.is_last() {
                let seq = self.seq_emit.remove(&batch.tag).unwrap_or(0);
                batch.set_seq(seq);
            } else {
                return Ok(());
            }
        } else {
            let seq = if batch.is_last() {
                self.seq_emit.remove(&batch.tag).unwrap_or(0)
            } else {
                let seq = self.seq_emit.get_mut_or_insert(&batch.tag);
                *seq += 1;
                *seq - 1
            };
            batch.set_seq(seq);
        }

        let tag = batch.tag().clone();
        if !batch.is_empty() {
            trace_worker!(
                "output[{:?}] send {}th batch(len={}) of {:?} ;",
                self.port,
                batch.get_seq(),
                batch.len(),
                tag
            );
        }
        match self.tee.push(batch) {
            Err(e) => {
                if e.is_would_block() {
                    trace_worker!("output[{:?}] been blocked when sending batch of {:?};", self.port, tag);
                    self.blocks.push_back(BlockScope::new(tag));
                }
                Err(e)
            }
            _ => Ok(()),
        }
    }

    fn flush_buffer(&mut self, buffers: &mut ScopeBufferPool<D>) -> IOResult<()> {
        for (tag, buf) in buffers.buffers() {
            let batch = MicroBatch::new(tag, self.src, buf.into_read_only());
            self.send_batch(batch)?;
        }
        self.tee.flush()
    }

    fn clean_lost_end_child(&mut self, tag: &Tag, buf_pool: &mut ScopeBufferPool<D>) -> IOResult<()> {
        trace_worker!("clean buffer child of {:?}", tag);
        for (tag, buf) in buf_pool.child_buffers_of(tag, true) {
            if !buf.is_empty() {
                // 正常情况不应进入这段逻辑，除非用户算子hold了end 信号， 通常这类情况需要用户定义on_notify算子；
                warn_worker!("output[{:?}] the end signal of scope {:?} lost;", self.port, tag);
                let batch = MicroBatch::new(tag, self.src, buf.into_read_only());
                if let Err(err) = self.send_batch(batch) {
                    return if err.is_would_block() { Err(IOError::cannot_block()) } else { Err(err) };
                }
            }
        }
        Ok(())
    }

    fn try_push_iter_inner<I: Iterator<Item = D>>(
        &mut self, tag: &Tag, iter: &mut I,
    ) -> IOResult<Option<D>> {
        //self.buf_pool.pin(tag);
        loop {
            match self.buf_pool.push_iter(tag, iter) {
                Ok(Some(buf)) => {
                    let batch = MicroBatch::new(tag.clone(), self.src, buf);
                    self.send_batch(batch)?;
                }
                Ok(None) => {
                    // all data in iter should be send;
                    assert!(iter.next().is_none());
                    break;
                }
                Err(e) => return if let Some(item) = e.0 { Ok(Some(item)) } else { would_block!("") },
            }
        }
        Ok(None)
    }
}

pub struct OutputSession<'a, D: Data> {
    pub tag: Tag,
    pub skip: bool,
    output: RefMut<'a, OutputHandle<D>>,
}

impl<'a, D: Data> OutputSession<'a, D> {
    pub(crate) fn new(tag: Tag, mut output: RefMut<'a, OutputHandle<D>>) -> Self {
        if output.is_skipped(&tag) {
            OutputSession { tag, skip: true, output }
        } else {
            //trace_worker!("output[{:?}] try to pin {:?} to create output session; ", output.port, tag);
            output.buf_pool.pin(&tag);
            OutputSession { tag, skip: false, output }
        }
    }

    pub fn give(&mut self, msg: D) -> IOResult<()> {
        if self.skip {
            Ok(())
        } else {
            self.output.push(&self.tag, msg)
        }
    }

    pub fn give_last(&mut self, msg: D, end: EndOfScope) -> IOResult<()> {
        if self.skip {
            self.output.notify_end(end)
        } else {
            assert_eq!(self.tag, end.tag);
            self.output.push_last(msg, end)
        }
    }

    pub fn give_iterator<I>(&mut self, iter: I) -> IOResult<()>
    where
        I: Iterator<Item = D> + Send + 'static,
    {
        if self.skip {
            Ok(())
        } else {
            self.output.push_iter(&self.tag, iter)
        }
    }

    pub fn notify_end(&mut self, end: EndOfScope) -> IOResult<()> {
        assert_eq!(self.tag, end.tag);
        self.output.notify_end(end)
    }

    pub fn flush(&mut self) -> IOResult<()> {
        self.output.flush()
    }
}

/// don't check skip as it would be checked in [`OutputSession`] ;
impl<D: Data> ScopeStreamPush<D> for OutputHandle<D> {
    fn port(&self) -> Port {
        self.port
    }

    fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
        match self.buf_pool.push(tag, msg) {
            Ok(Some(buf)) => {
                let batch = MicroBatch::new(tag.clone(), self.src, buf);
                self.send_batch(batch)
            }
            Err(e) => {
                trace_worker!("output[{:?}] block on pushing data of {:?};", self.port, tag);
                if let Some(item) = e.0 {
                    self.block_entries
                        .insert(tag.clone(), BlockEntry::Single(item));
                }
                self.blocks
                    .push_back(BlockScope::new(tag.clone()));
                would_block!("no buffer available")
            }
            Ok(_) => Ok(()),
        }
    }

    fn push_last(&mut self, msg: D, end: EndOfScope) -> IOResult<()> {
        match self.buf_pool.push_last(&end.tag, msg) {
            Ok(Some(buf)) => {
                let mut batch = MicroBatch::new(end.tag.clone(), self.src, buf);
                batch.set_end(end);
                self.send_batch(batch)
            }
            Ok(None) => Ok(()),
            Err(e) => {
                trace_worker!("output[{:?}]: block on pushing last data of {:?};", self.port, &end.tag);
                if let Some(item) = e.0 {
                    self.blocks
                        .push_back(BlockScope::new(end.tag.clone()));
                    self.block_entries
                        .insert(end.tag.clone(), BlockEntry::LastSingle(item, end));
                } else {
                    unreachable!("data may lost;")
                }
                would_block!("no buffer available")
            }
        }
    }

    fn try_push_iter<I: Iterator<Item = D>>(&mut self, _tag: &Tag, _iter: &mut I) -> IOResult<()> {
        //self.buf_pool.pin(tag);
        unimplemented!("use push iter;");
    }

    fn notify_end(&mut self, end: EndOfScope) -> IOResult<()> {
        let level = end.tag.len() as u32;
        if level == self.scope_level {
            let mut batch = if let Some(buf) = self.buf_pool.take_last_buf(&end.tag) {
                MicroBatch::new(end.tag.clone(), self.src, buf.into_read_only())
            } else {
                MicroBatch::new(end.tag.clone(), self.src, ReadBuffer::new())
            };
            trace_worker!(
                "output[{:?}] send end of scope{:?} peers: {:?}",
                self.port,
                batch.tag,
                end.peers()
            );
            batch.set_end(end);
            self.send_batch(batch)
        } else if level < self.scope_level {
            trace_worker!(
                "output[{:?}] send end of scope{:?} peers: {:?}",
                self.port,
                end.tag,
                end.peers()
            );
            let mut buf_pool = std::mem::replace(&mut self.buf_pool, Default::default());
            let result = self.clean_lost_end_child(&end.tag, &mut buf_pool);
            self.buf_pool = buf_pool;
            if let Err(err) = result {
                return Err(err);
            }
            let mut batch = MicroBatch::new(end.tag.clone(), self.src, ReadBuffer::new());
            batch.set_end(end);
            self.tee.push(batch)
        } else {
            warn_worker!("end signal from child scope {:?};", end.tag);
            // ignore;
            Ok(())
        }
    }

    fn flush(&mut self) -> IOResult<()> {
        trace_worker!("output[{:?}] force flush all buffers;", self.port);
        let mut buffers = std::mem::replace(&mut self.buf_pool, Default::default());
        let result = self.flush_buffer(&mut buffers);
        self.buf_pool = buffers;
        result
    }

    fn close(&mut self) -> IOResult<()> {
        self.flush()?;
        self.is_closed = true;
        trace_worker!("output[{:?}] closing ...;", self.port);
        self.tee.close()
    }
}
