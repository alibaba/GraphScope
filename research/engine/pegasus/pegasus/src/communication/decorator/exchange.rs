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

use crate::api::function::{BatchRouteFunction, FnResult, RouteFunction};
use crate::channel_id::ChannelInfo;
use crate::communication::buffer::ScopeBufferPool;
use crate::communication::cancel::{CancelHandle, MultiConsCancelPtr, SingleConsCancel};
use crate::communication::channel::BatchRoute;
use crate::communication::decorator::evented::EventEmitPush;
use crate::communication::decorator::BlockPush;
use crate::communication::{IOResult, Magic};
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::{DynPeers, EndOfScope};
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

struct Exchange<D> {
    magic: Magic,
    router: Box<dyn RouteFunction<D>>,
}

impl<D: Data> Exchange<D> {
    fn new(len: usize, router: Box<dyn RouteFunction<D>>) -> Self {
        let magic =
            if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
        Exchange { magic, router }
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

pub(crate) struct ExchangeByDataPush<D: Data> {
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

impl<D: Data> ExchangeByDataPush<D> {
    pub(crate) fn new(
        info: ChannelInfo, router: Box<dyn RouteFunction<D>>, buffers: Vec<ScopeBufferPool<D>>,
        pushes: Vec<EventEmitPush<D>>,
    ) -> Self {
        let src = crate::worker_id::get_current_worker().index;
        let len = pushes.len();
        let cancel_handle = MultiConsCancelPtr::new(info.scope_level, len);
        ExchangeByDataPush {
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
            if let Some(last) = self.buffers[index].take_last_buf(tag) {
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

    fn update_end(
        &mut self, target: Option<usize>, end: &EndOfScope,
    ) -> impl Iterator<Item = (u64, u64, DynPeers)> {
        let mut push_stat = Vec::with_capacity(self.pushes.len());
        for (index, p) in self.pushes.iter().enumerate() {
            let mut pushes = p.get_push_count(&end.tag).unwrap_or(0) as u64;
            if let Some(target) = target {
                if target == index {
                    pushes += 1;
                }
            }
            push_stat.push(pushes);
        }

        let mut weight = DynPeers::empty();
        for (i, p) in push_stat.iter().enumerate() {
            if *p > 0 {
                weight.add_source(i as u32);
            }
        }

        trace_worker!(
            "output[{:?}]: try to update peers from {:?} to {:?} of scope {:?}",
            self.port,
            end.peers(),
            weight,
            end.tag
        );
        let global_total_send = push_stat.iter().sum::<u64>();
        push_stat
            .into_iter()
            .map(move |send| (send, global_total_send, weight.clone()))
    }

    fn push_to(&mut self, index: usize, tag: Tag, buf: ReadBuffer<D>) -> IOResult<()> {
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
                    has_block = true;
                    if let Some(x) = e.0 {
                        self.blocks
                            .get_mut_or_insert(&tag)
                            .push_back(BlockEntry::Single((target, x)));
                        trace_worker!("output[{:?}] blocked when push data of {:?} ;", self.port, tag);
                        break;
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
                assert!(end.peers_contains(self.src), "push illegal data without allow;");
                self.flush_last_buffer(&batch.tag)?;
                for (i, (t, g, children)) in self.update_end(None, &end).enumerate() {
                    let mut new_end = end.clone();
                    new_end.total_send = t;
                    new_end.global_total_send = g;
                    self.pushes[i].sync_end(new_end, children)?;
                }
            }
            Ok(())
        }
    }
}

impl<D: Data> Push<MicroBatch<D>> for ExchangeByDataPush<D> {
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
            if let Some(end) = batch.take_end() {
                if level == self.scope_level {
                    if end.peers().value() == 0 {
                        // TODO: seems unreachable;
                        assert_eq!(batch.get_seq(), 0);
                        // handle empty stream
                        let mut owner = 0;
                        if level > 0 {
                            owner = self
                                .route
                                .magic
                                .exec(end.tag.current_uncheck() as u64)
                                as u32;
                        };
                        if self.src == owner {
                            for p in self.pushes.iter_mut() {
                                let mut new_end = end.clone();
                                new_end.total_send = 0;
                                new_end.global_total_send = 0;
                                new_end.update_peers(DynPeers::single(self.src));
                                p.push_end(new_end, DynPeers::single(self.src))?;
                            }
                        } else {
                            trace_worker!(
                                "output[{:?}]: ignore end of scope {:?} as peers = {:?}",
                                self.port,
                                end.tag,
                                end.peers()
                            );
                        }
                        return Ok(());
                    }

                    if batch.get_seq() == 0 {
                        // it's the first batch need to be pushed on this port, and it's an empty batch;
                        if !end.peers_contains(self.src) {
                            trace_worker!(
                                "output[{:?}]: ignore end of scope {:?} as peers = {:?}",
                                self.port,
                                end.tag,
                                end.peers()
                            );
                            return Ok(());
                        }

                        for p in self.pushes.iter_mut() {
                            let mut new_end = end.clone();
                            new_end.total_send = 0;
                            new_end.global_total_send = 0;
                            p.sync_end(new_end, DynPeers::empty())?;
                        }
                    } else {
                        // not the first batch;
                        assert!(end.peers_contains(self.src), "pushed invalid data without allow;");
                        self.flush_last_buffer(batch.tag())?;
                        for (i, (t, g, children)) in self.update_end(None, &end).enumerate() {
                            let mut new_end = end.clone();
                            new_end.total_send = t;
                            new_end.global_total_send = g;
                            self.pushes[i].sync_end(new_end, children)?;
                        }
                    };
                } else {
                    // this is an end of parent scope;
                    for p in self.pushes.iter_mut() {
                        let mut new_end = end.clone();
                        new_end.total_send = 0;
                        new_end.global_total_send = 0;
                        p.sync_end(new_end, DynPeers::all())?;
                    }
                }
            } else {
                warn_worker!("output[{:?}]: ignore empty batch of {:?};", self.port, batch.tag);
            }
            Ok(())
        } else if len == 1 {
            // only one data, not need re-batching;
            assert_eq!(level, self.scope_level);
            if let Some(end) = batch.take_end() {
                assert!(end.peers_contains(self.src), "invalid data, can't push without allow;");
                let x = batch
                    .get(0)
                    .expect("expect at least one entry as len = 1");
                let target = self.route.route(x)? as usize;
                if batch.get_seq() == 0 {
                    // only one data scope;
                    if end.peers().value() == 1 {
                        for i in 0..self.pushes.len() {
                            if i != target {
                                let mut new_end = end.clone();
                                new_end.total_send = 0;
                                new_end.global_total_send = 1;
                                self.pushes[i].push_end(new_end, DynPeers::single(target as u32))?;
                            }
                        }
                        let mut new_end = end;
                        new_end.total_send = 1;
                        new_end.global_total_send = 1;
                        new_end.update_peers(DynPeers::single(target as u32));
                        batch.set_end(new_end);
                        self.pushes[target as usize].push(batch)?;
                    } else {
                        // multi source;
                        self.pushes[target].push(batch)?;
                        let children = DynPeers::single(target as u32);
                        for i in 0..self.pushes.len() {
                            let mut new_end = end.clone();
                            if i != target {
                                new_end.total_send = 0;
                            } else {
                                new_end.total_send = 1;
                            }
                            new_end.global_total_send = 1;
                            self.pushes[i].sync_end(end.clone(), children.clone())?;
                        }
                    }
                } else {
                    // flush previous buffered data;
                    self.flush_last_buffer(&batch.tag)?;
                    let result = self.update_end(Some(target), &end);
                    if end.peers().value() == 1 {
                        for (i, (t, g, children)) in result.enumerate() {
                            let mut new_end = end.clone();
                            new_end.total_send = t;
                            new_end.global_total_send = g;
                            if i == target {
                                let mut batch = std::mem::replace(&mut batch, MicroBatch::empty());
                                new_end.update_peers(children);
                                batch.set_end(new_end);
                                self.pushes[i].push(batch)?;
                            } else {
                                self.pushes[i].push_end(new_end, children)?;
                            }
                        }
                    } else {
                        self.pushes[target].push(batch)?;
                        for (i, (t, g, children)) in result.into_iter().enumerate() {
                            let mut new_end = end.clone();
                            new_end.total_send = t;
                            new_end.global_total_send = g;
                            self.pushes[i].sync_end(new_end, children)?;
                        }
                    }
                }
            } else {
                let item = batch
                    .next()
                    .expect("expect at least one entry as len = 1");
                let target = self.route.route(&item)? as usize;
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
            }
            Ok(())
        } else {
            assert_eq!(level, self.scope_level);
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

impl<D: Data> BlockPush for ExchangeByDataPush<D> {
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

pub struct ExchangeByBatchPush<D: Data> {
    pub ch_info: ChannelInfo,
    src: u32,
    pushes: Vec<EventEmitPush<D>>,
    magic: Magic,
    route: BatchRoute<D>,
    cancel_handle: CancelHandle,
}

impl<D: Data> ExchangeByBatchPush<D> {
    pub fn new(ch_info: ChannelInfo, route: BatchRoute<D>, pushes: Vec<EventEmitPush<D>>) -> Self {
        let len = pushes.len();
        let magic = Magic::new(len);
        let src = crate::worker_id::get_current_worker().index;
        let cancel_handle = match route {
            BatchRoute::AllToOne(t) => CancelHandle::SC(SingleConsCancel::new(t)),
            BatchRoute::Dyn(_) => CancelHandle::MC(MultiConsCancelPtr::new(ch_info.scope_level, len)),
        };
        ExchangeByBatchPush { ch_info, src, pushes, magic, route, cancel_handle }
    }

    pub(crate) fn update_cancel_handle(&mut self, cancel_handle: CancelHandle) {
        self.cancel_handle = cancel_handle;
    }

    pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
        self.cancel_handle.clone()
    }

    fn update_end(
        &mut self, target: Option<usize>, end: &EndOfScope,
    ) -> impl Iterator<Item = (u64, u64, DynPeers)> {
        let mut push_stat = Vec::with_capacity(self.pushes.len());
        for (index, p) in self.pushes.iter().enumerate() {
            let mut pushes = p.get_push_count(&end.tag).unwrap_or(0) as u64;
            if let Some(target) = target {
                if target == index {
                    pushes += 1;
                }
            }
            push_stat.push(pushes);
        }

        let mut weight = DynPeers::empty();
        for (i, p) in push_stat.iter().enumerate() {
            if *p > 0 {
                weight.add_source(i as u32);
            }
        }
        trace_worker!(
            "output[{:?}]: try to update peers from {:?} to {:?} of scope {:?}",
            self.ch_info.source_port,
            end.peers(),
            weight,
            end.tag
        );
        let global_total_send = push_stat.iter().sum::<u64>();
        push_stat
            .into_iter()
            .map(move |send| (send, global_total_send, weight.clone()))
    }

    fn handle_last(&mut self, seq: u64, end: EndOfScope) -> Result<(), IOError> {
        if !end.peers_contains(self.src) {
            if end.peers().value() == 0 {
                // TODO: seems unreachable
                let mut owner = 0;
                if end.tag.len() > 0 {
                    owner = end.tag.current_uncheck() % (self.pushes.len() as u32);
                }
                if owner == self.src {
                    for p in self.pushes.iter_mut() {
                        let mut new_end = end.clone();
                        new_end.total_send = 0;
                        new_end.global_total_send = 0;
                        new_end.update_peers(DynPeers::single(self.src));
                        p.push_end(new_end, DynPeers::single(self.src))?;
                    }
                } else {
                    trace_worker!(
                        "output[{:?}]: ignore end of scope {:?} as peers = {:?}",
                        self.ch_info.source_port,
                        end.tag,
                        end.peers()
                    )
                }
            } else {
                trace_worker!(
                    "output[{:?}]: ignore end of scope {:?} as peers = {:?}",
                    self.ch_info.source_port,
                    end.tag,
                    end.peers()
                )
            }
            return Ok(());
        }

        if seq == 0 {
            for p in self.pushes.iter_mut() {
                let mut new_end = end.clone();
                new_end.total_send = 0;
                new_end.global_total_send = 0;
                p.sync_end(new_end, DynPeers::empty())?;
            }
        } else {
            for (i, (t, g, c)) in self.update_end(None, &end).enumerate() {
                let mut new_end = end.clone();
                new_end.total_send = t;
                new_end.global_total_send = g;
                self.pushes[i].sync_end(new_end, c)?;
            }
        }
        Ok(())
    }

    fn handle_last_batch(
        &mut self, target: usize, mut end: EndOfScope, mut batch: MicroBatch<D>,
    ) -> Result<(), IOError> {
        assert!(end.peers_contains(self.src));
        if end.peers().value() == 1 {
            // if only one peers, it must be this worker;
            if batch.get_seq() == 0 {
                // if the first batch:
                //    total_send may be 0;
                let total_send = batch.len() as u64;
                for (i, p) in self.pushes.iter_mut().enumerate() {
                    let mut new_end = end.clone();
                    if i != target {
                        new_end.total_send = 0;
                        new_end.global_total_send = total_send;
                        p.push_end(new_end, DynPeers::single(target as u32))?;
                    }
                }

                end.update_peers(DynPeers::single(target as u32));
                end.total_send = total_send;
                end.global_total_send = total_send;
                batch.set_end(end);
                self.pushes[target].push(batch)?;
            } else {
                // not the first batch, need to check pushed before;
                let tmp = if batch.is_empty() { None } else { Some(target) };
                for (i, (t, g, c)) in self.update_end(tmp, &end).enumerate() {
                    let mut new_end = end.clone();
                    new_end.total_send = t;
                    new_end.global_total_send = g;
                    if i == target {
                        let mut batch = std::mem::replace(&mut batch, MicroBatch::empty());
                        new_end.update_peers(c);
                        batch.set_end(new_end);
                        self.pushes[i].push(batch)?;
                    } else {
                        self.pushes[i].push_end(new_end, c)?;
                    }
                }
            }
        } else {
            // not the only one peers of the scope;
            if batch.get_seq() == 0 {
                // if the first batch:
                if batch.is_empty() {
                    // first batch is empty as been canceled;
                    for p in self.pushes.iter_mut() {
                        let mut new_end = end.clone();
                        new_end.total_send = 0;
                        new_end.global_total_send = 0;
                        p.sync_end(new_end, DynPeers::empty())?;
                    }
                } else {
                    let len = batch.len() as u64;
                    self.pushes[target].push(batch)?;
                    for (i, p) in self.pushes.iter_mut().enumerate() {
                        let mut new_end = end.clone();
                        if i == target {
                            new_end.total_send = len;
                        } else {
                            new_end.total_send = 0;
                        }
                        new_end.global_total_send = len;
                        p.sync_end(new_end, DynPeers::single(target as u32))?;
                    }
                }
            } else {
                let tmp = if !batch.is_empty() {
                    self.pushes[target].push(batch)?;
                    Some(target)
                } else {
                    None
                };
                for (i, (t, g, c)) in self.update_end(tmp, &end).enumerate() {
                    let mut new_end = end.clone();
                    new_end.total_send = t;
                    new_end.global_total_send = g;
                    self.pushes[i].sync_end(new_end, c)?;
                }
            }
        }
        Ok(())
    }
}

impl<D: Data> Push<MicroBatch<D>> for ExchangeByBatchPush<D> {
    fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
        let level = batch.tag.len() as u32;
        if level == self.ch_info.scope_level {
            if batch.is_empty() {
                if let Some(end) = batch.take_end() {
                    self.handle_last(batch.get_seq(), end)?;
                } else {
                    //
                }
            } else {
                // batch is not empty, it means this worker must in peers of the scope;
                let r = self.route.route(&batch)?;
                let target = self.magic.exec(r) as usize;
                if self
                    .cancel_handle
                    .is_canceled(batch.tag(), target)
                {
                    batch.clear();
                }
                // after cancel, the batch may be empty;
                if let Some(end) = batch.take_end() {
                    self.handle_last_batch(target, end, batch)?;
                } else if !batch.is_empty() {
                    self.pushes[target].push(batch)?;
                } else {
                    // ignore
                }
            }
        } else {
            assert!(batch.is_empty());
            if let Some(end) = batch.take_end() {
                if end.contains_source(self.src) {
                    for p in self.pushes.iter_mut() {
                        let mut new_end = end.clone();
                        new_end.total_send = 0;
                        new_end.global_total_send = 0;
                        p.sync_end(new_end, DynPeers::all())?;
                    }
                }
            } else {
                // ignore empty batch;
                unreachable!();
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
