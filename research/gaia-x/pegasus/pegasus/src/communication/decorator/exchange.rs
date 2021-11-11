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

use std::collections::VecDeque;

use pegasus_common::buffer::ReadBuffer;

use crate::api::function::{FnResult, RouteFunction};
use crate::channel_id::ChannelInfo;
use crate::communication::buffer::ScopeBufferPool;
use crate::communication::cancel::{CancelHandle, DynSingleConsCancelPtr, MultiConsCancelPtr};
use crate::communication::decorator::evented::EventEmitPush;
use crate::communication::decorator::BlockPush;
use crate::communication::{IOResult, Magic};
use crate::data::{EndOfScope, MicroBatch};
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::{EndSignal, Weight};
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

#[allow(dead_code)]
struct Exchange<D> {
    magic: Magic,
    targets: u64,
    router: Box<dyn RouteFunction<D>>,
}

#[allow(dead_code)]
impl<D: Data> Exchange<D> {
    fn new(len: usize, router: Box<dyn RouteFunction<D>>) -> Self {
        let magic =
            if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
        Exchange { magic, targets: len as u64, router }
    }

    #[inline]
    fn route(&self, item: &D) -> FnResult<u64> {
        let par_key = self.router.route(item)?;
        Ok(self.magic.exec(par_key))
    }
}

enum BlockEntry<D> {
    Single((usize, D)),
    Batch(MicroBatch<D>),
}

pub(crate) struct ExchangeMicroBatchPush<D: Data> {
    pub src: u32,
    pub port: Port,
    index: u32,
    scope_level: u32,
    buffers: Vec<ScopeBufferPool<D>>,
    pushes: Vec<EventEmitPush<D>>,
    route: Exchange<D>,
    blocks: TidyTagMap<VecDeque<BlockEntry<D>>>,
    cancel_handle: MultiConsCancelPtr,
}

impl<D: Data> ExchangeMicroBatchPush<D> {
    pub(crate) fn new(
        info: ChannelInfo, router: Box<dyn RouteFunction<D>>, buffers: Vec<ScopeBufferPool<D>>,
        pushes: Vec<EventEmitPush<D>>,
    ) -> Self {
        let src = crate::worker_id::get_current_worker().index;
        let len = pushes.len();
        let cancel_handle = MultiConsCancelPtr::new(info.scope_level, len);
        ExchangeMicroBatchPush {
            src,
            port: info.source_port,
            index: info.index(),
            scope_level: info.scope_level,
            buffers,
            pushes,
            route: Exchange::new(len, router),
            blocks: TidyTagMap::new(info.scope_level),
            cancel_handle,
        }
    }

    pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
        CancelHandle::MC(self.cancel_handle.clone())
    }

    pub(crate) fn skip(&mut self, tag: &Tag) -> IOResult<()> {
        // clean buffer first;
        for buf in self.buffers.iter_mut() {
            buf.skip_buf(tag);
        }

        let level = tag.len() as u32;
        if level == self.blocks.scope_level {
            if let Some(mut b) = self.blocks.remove(tag) {
                while let Some(b) = b.pop_front() {
                    match b {
                        BlockEntry::Single(_) => {
                            // ignore
                        }
                        BlockEntry::Batch(mut batch) => {
                            batch.clear();
                            if batch.is_last() {
                                self.push(batch)?;
                            }
                        }
                    }
                }
            }
        } else if level < self.blocks.scope_level {
            let mut blocks = std::mem::replace(&mut self.blocks, Default::default());
            for (k, v) in blocks.iter_mut() {
                if tag.is_parent_of(&*k) {
                    while let Some(b) = v.pop_front() {
                        match b {
                            BlockEntry::Single(_) => {
                                // ignore
                            }
                            BlockEntry::Batch(mut batch) => {
                                batch.clear();
                                if batch.is_last() {
                                    if let Err(err) = self.push(batch) {
                                        self.blocks = blocks;
                                        return Err(err);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // ignore
        }
        Ok(())
    }

    fn flush_last_buffer(&mut self, tag: &Tag) -> IOResult<()> {
        if let Some(block) = self.blocks.remove(tag) {
            assert!(block.is_empty(), "has block outstanding");
        }

        for index in 0..self.pushes.len() {
            if let Some(last) = self.buffers[index].take_buf(tag, true) {
                if !last.is_empty() {
                    self.push_to(index, tag.clone(), last.into_read_only())?;
                }
            }
            self.pushes[index].flush()?;
        }
        Ok(())
    }

    fn flush_inner(&mut self, buffers: &mut Vec<ScopeBufferPool<D>>) -> IOResult<()> {
        for i in 0..self.pushes.len() {
            let iter = buffers[i].buffers();
            for (tag, buf) in iter {
                if buf.len() > 0 {
                    self.push_to(i, tag, buf.into_read_only())?;
                }
            }
            self.pushes[i].flush()?;
        }
        Ok(())
    }

    fn update_end(&mut self, target: Option<usize>, end: &EndOfScope) -> Vec<EndSignal> {
        let mut push_stat = vec![];
        for (index, p) in self.pushes.iter().enumerate() {
            let mut pushes = p.get_push_count(&end.tag).unwrap_or(0);
            if let Some(target) = target {
                if target == index {
                    pushes += 1;
                }
            }
            push_stat.push(pushes);
        }

        let weight = if end.tag.is_root() {
            Weight::all()
        } else {
            let mut weight = Weight::partial_empty();
            for (i, p) in push_stat.iter().enumerate() {
                if *p > 0 {
                    weight.add_source(i as u32);
                }
            }

            trace_worker!(
                "output[{:?}]: try to update eos weight to {:?} of scope {:?}",
                self.port,
                weight,
                end.tag
            );
            weight
        };

        let mut result = vec![];
        for p in push_stat.into_iter() {
            let mut end = end.clone();
            end.total_send = p as u64;
            result.push(EndSignal::new(end, weight.clone()));
        }

        result
    }

    fn push_to(&mut self, index: usize, tag: Tag, buf: ReadBuffer<D>) -> IOResult<()> {
        // if batch.is_last() {
        //     let mut cnt = self.push_counts[index].remove(&batch.tag).unwrap_or(0);
        //     cnt += batch.len();
        //     trace_worker!("total pushed {} records of scope {:?} on port {:?} to worker {}", cnt, batch.tag, self.port, index);
        // } else {
        //     let count = self.push_counts[index].get_mut_or_insert(&batch.tag);
        //     *count = batch.len();
        // };
        if !self.cancel_handle.is_canceled(&tag, index) {
            let batch = MicroBatch::new(tag, self.src, buf);
            self.pushes[index].push(batch)
        } else {
            Ok(())
        }
    }

    fn push_inner(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
        let mut has_block = false;
        let tag = batch.tag().clone();
        for item in batch.drain() {
            let target = self.route.route(&item)? as usize;
            match self.buffers[target].push(&tag, item) {
                Ok(Some(buf)) => {
                    self.push_to(target, tag.clone(), buf)?;
                }
                Err(e) => {
                    if !self.cancel_handle.is_canceled(&tag, target) {
                        has_block = true;
                        if let Some(x) = e.0 {
                            self.blocks
                                .get_mut_or_insert(&tag)
                                .push_back(BlockEntry::Single((target, x)));
                            trace_worker!("output[{:?}] blocked when push data of {:?} ;", self.port, tag);
                            break;
                        }
                    }
                }
                _ => (),
            }
        }

        if has_block {
            if !batch.is_empty() {
                trace_worker!(
                    "output[{:?}] blocking on push batch(len={}) of {:?} ;",
                    self.port,
                    batch.len(),
                    batch.tag
                );
                self.blocks
                    .get_mut_or_insert(&batch.tag)
                    .push_back(BlockEntry::Batch(batch));
            }
            would_block!("no buffer available in exchange;")
        } else {
            if let Some(end) = batch.take_end() {
                self.flush_last_buffer(&batch.tag)?;
                let result = self.update_end(None, &end);
                if end.source.value() == 1 {
                    assert!(self.scope_level > 0);
                    for (i, e) in result.into_iter().enumerate() {
                        let end = e.into_end();
                        if end.total_send > 0 {
                            let batch = MicroBatch::last(self.src, end);
                            self.pushes[i].push(batch)?;
                        }
                    }
                } else {
                    for (i, e) in result.into_iter().enumerate() {
                        self.pushes[i].notify_end(e)?;
                    }
                }
            }
            Ok(())
        }
    }
}

impl<D: Data> Push<MicroBatch<D>> for ExchangeMicroBatchPush<D> {
    fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
        if !self.blocks.is_empty() {
            if let Some(b) = self.blocks.get_mut(&batch.tag) {
                warn_worker!(
                    "output[{:?}] block pushing batch of {:?} to channel[{}] ;",
                    self.port,
                    batch.tag,
                    self.index
                );
                b.push_back(BlockEntry::Batch(batch));
                return would_block!("no buffer available in exchange;");
            }
        }

        let level = batch.tag.len() as u32;

        if self.pushes.len() == 1 {
            if level == self.scope_level && self.cancel_handle.is_canceled(&batch.tag, 0) {
                batch.clear();
                if !batch.is_last() {
                    return Ok(());
                }
            }
            return self.pushes[0].push(batch);
        }

        let len = batch.len();

        if len == 0 {
            if let Some(mut end) = batch.take_end() {
                if level == self.scope_level {
                    // this is an end of scope of current level;
                    if end.source.value() == 1 {
                        assert!(self.scope_level > 0);
                        if batch.get_seq() == 0 {
                            // an empty scope without any data;
                            trace_worker!("output[{:?}] empty scope {:?};", self.port, batch.tag());
                            let tag_cur = end.tag.current_uncheck() as u64;
                            let owner = self.route.magic.exec(tag_cur) as usize;
                            end.total_send = 0;
                            end.source = Weight::single(self.src);
                            self.pushes[owner].push(MicroBatch::last(self.src, end))?;
                        } else {
                            // source = 1, batch.seq > 0
                            self.flush_last_buffer(batch.tag())?;
                            let mut total_pushed = 0;
                            for (i, sig) in self
                                .update_end(None, &end)
                                .into_iter()
                                .enumerate()
                            {
                                let end = sig.into_end();
                                if end.total_send > 0 {
                                    total_pushed += end.total_send;
                                    let last = MicroBatch::last(self.src, end);
                                    self.pushes[i].push(last)?;
                                }
                            }
                            if total_pushed == 0 {
                                let tag_cur = end.tag.current_uncheck() as u64;
                                let owner = self.route.magic.exec(tag_cur) as usize;
                                end.total_send = 0;
                                end.source = Weight::single(self.src);
                                self.pushes[owner].push(MicroBatch::last(self.src, end))?;
                            }
                        }
                    } else {
                        if batch.get_seq() == 0 {
                            for p in self.pushes.iter_mut() {
                                let children =
                                    if end.tag.is_root() { Weight::all() } else { Weight::partial_empty() };
                                let end = EndSignal::new(end.clone(), children);
                                p.notify_end(end)?;
                            }
                        } else {
                            self.flush_last_buffer(batch.tag())?;
                            let sigs = self.update_end(None, &end);
                            for (i, s) in sigs.into_iter().enumerate() {
                                self.pushes[i].notify_end(s)?;
                            }
                        }
                    }
                } else {
                    // this is an end of parent scope;
                    if end.source.value() == 1 {
                        end.source = Weight::all();
                        batch.set_end(end);
                        for p in self.pushes.iter_mut() {
                            p.push(batch.clone())?;
                        }
                    } else {
                        for p in self.pushes.iter_mut() {
                            let sig = EndSignal::new(end.clone(), Weight::all());
                            p.notify_end(sig)?;
                        }
                    }
                }
            } else {
                warn_worker!("output[{:?}]: empty batch of {:?};", self.port, batch.tag);
            }
            Ok(())
        } else if len == 1 {
            // only one data, not need re-batching;
            assert_eq!(level, self.scope_level);
            if let Some(mut end) = batch.take_end() {
                let x = batch
                    .get(0)
                    .expect("expect at least one entry as len = 1");
                let target = self.route.route(x)? as usize;

                if batch.get_seq() == 0 {
                    // only one data scope;
                    if end.source.value() == 1 {
                        assert!(level > 0);
                        end.source = Weight::single(target as u32);
                        batch.set_end(end);
                        self.pushes[target].push(batch)?;
                    } else {
                        // multi source;
                        self.pushes[target].push(batch)?;
                        let weight =
                            if end.tag.is_root() { Weight::all() } else { Weight::single(target as u32) };
                        let end = EndSignal::new(end, weight);
                        for i in 1..self.pushes.len() {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                        self.pushes[0].notify_end(end)?;
                    }
                } else {
                    // flush previous buffered data;
                    self.flush_last_buffer(&batch.tag)?;
                    let result = self.update_end(Some(target), &end);
                    if end.source.value() == 1 {
                        assert!(self.scope_level > 0);
                        for (i, e) in result.into_iter().enumerate() {
                            let mut empty = MicroBatch::new(end.tag.clone(), self.src, ReadBuffer::new());
                            if i == target {
                                let mut batch = std::mem::replace(&mut batch, empty);
                                batch.set_end(e.into_end());
                                self.pushes[i].push(batch)?;
                            } else {
                                let end = e.into_end();
                                if end.total_send > 0 {
                                    empty.set_end(end);
                                    self.pushes[i].push(empty)?;
                                }
                            }
                        }
                    } else {
                        for (i, e) in result.into_iter().enumerate() {
                            self.pushes[i].notify_end(e)?;
                        }
                    }
                }
            } else {
                let item = batch
                    .next()
                    .expect("expect at least one entry as len = 1");
                let target = self.route.route(&item)? as usize;
                if !self
                    .cancel_handle
                    .is_canceled(&batch.tag, target)
                {
                    match self.buffers[target].push(&batch.tag, item) {
                        Ok(Some(buf)) => self.push_to(target, batch.tag().clone(), buf)?,
                        Ok(None) => (),
                        Err(e) => {
                            if let Some(x) = e.0 {
                                self.blocks
                                    .get_mut_or_insert(&batch.tag)
                                    .push_back(BlockEntry::Single((target, x)));
                                trace_worker!(
                                    "output[{:?}] blocked when push data of {:?} ;",
                                    self.port,
                                    batch.tag,
                                );
                            }
                            would_block!("no buffer available in exchange;")?;
                        }
                    }
                } else {
                    // ignore is canceled;
                }
            }
            Ok(())
        } else {
            assert_eq!(level, self.scope_level);
            let tag = batch.tag().clone();
            for (target, p) in self.buffers.iter_mut().enumerate() {
                if self.cancel_handle.is_canceled(&tag, target) {
                    p.skip_buf(&tag);
                } else {
                    p.pin(&tag);
                }
            }
            self.push_inner(batch)
        }
    }

    fn flush(&mut self) -> Result<(), IOError> {
        let mut buffers = std::mem::replace(&mut self.buffers, vec![]);
        let result = self.flush_inner(&mut buffers);
        self.buffers = buffers;
        result
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.flush()?;
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}

impl<D: Data> BlockPush for ExchangeMicroBatchPush<D> {
    fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError> {
        if let Some(mut blocks) = self.blocks.remove(tag) {
            while let Some(block) = blocks.pop_front() {
                match block {
                    BlockEntry::Single((t, d)) => {
                        match self.buffers[t].push(tag, d) {
                            Ok(Some(buf)) => {
                                if let Err(e) = self.push_to(t, tag.clone(), buf) {
                                    self.blocks.insert(tag.clone(), blocks);
                                    return Err(e);
                                }
                            }
                            Ok(None) => {
                                // continue unblock;
                            }
                            Err(err) => {
                                if let Some(d) = err.0 {
                                    blocks.push_front(BlockEntry::Single((t, d)));
                                }
                                self.blocks.insert(tag.clone(), blocks);
                                return Ok(false);
                            }
                        }
                    }
                    BlockEntry::Batch(b) => {
                        if let Err(err) = self.push_inner(b) {
                            return if err.is_would_block() {
                                if !blocks.is_empty() {
                                    let b = self
                                        .blocks
                                        .get_mut(tag)
                                        .expect("expect has block;");
                                    while let Some(x) = blocks.pop_front() {
                                        b.push_front(x);
                                    }
                                }
                                Ok(false)
                            } else {
                                self.blocks.insert(tag.clone(), blocks);
                                Err(err)
                            };
                        }
                    }
                }
            }
        }
        Ok(true)
    }

    fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()> {
        self.skip(tag)
    }
}

pub struct ExchangeByScopePush<D: Data> {
    pub ch_info: ChannelInfo,
    pushes: Vec<EventEmitPush<D>>,
    magic: Magic,
    cancel_handle: DynSingleConsCancelPtr,
}

impl<D: Data> ExchangeByScopePush<D> {
    pub fn new(ch_info: ChannelInfo, pushes: Vec<EventEmitPush<D>>) -> Self {
        assert!(ch_info.scope_level > 0);
        let len = pushes.len();
        let magic = Magic::new(len);
        let cancel_handle = DynSingleConsCancelPtr::new(ch_info.scope_level, len);
        ExchangeByScopePush { ch_info, pushes, magic, cancel_handle }
    }

    pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
        CancelHandle::DSC(self.cancel_handle.clone())
    }
}

impl<D: Data> Push<MicroBatch<D>> for ExchangeByScopePush<D> {
    fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
        let level = batch.tag.len() as u32;
        if level == self.ch_info.scope_level {
            let cur = batch.tag.current_uncheck() as u64;
            let target = self.magic.exec(cur) as usize;
            if self
                .cancel_handle
                .is_canceled(batch.tag(), target)
            {
                batch.clear();
                if !batch.is_last() {
                    return Ok(());
                }
            }

            if let Some(mut end) = batch.take_end() {
                if end.source.value() == 1 {
                    end.source = Weight::single(target as u32);
                    batch.set_end(end);
                    self.pushes[target].push(batch)?;
                } else {
                    if !batch.is_empty() {
                        self.pushes[target].push(batch)?;
                    }
                    // only notify the target;
                    let end = EndSignal::new(end, Weight::single(target as u32));
                    self.pushes[target].notify_end(end)?;
                }
            } else {
                if !batch.is_empty() {
                    self.pushes[target].push(batch)?;
                } else {
                    // ignore empty batch;
                }
            }
        } else {
            assert!(batch.is_empty());
            if let Some(mut end) = batch.take_end() {
                if end.source.value() == 1 {
                    end.source = Weight::all();
                    batch.set_end(end);
                    for p in self.pushes.iter_mut() {
                        p.push(batch.clone())?;
                    }
                } else {
                    let end = EndSignal::new(end, Weight::all());
                    for p in self.pushes.iter_mut() {
                        p.notify_end(end.clone())?;
                    }
                }
            } else {
                // ignore empty batch;
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.close()?;
        }
        Ok(())
    }
}
