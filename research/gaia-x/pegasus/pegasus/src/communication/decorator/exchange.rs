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

pub use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use crate::api::function::RouteFunction;
    use crate::channel_id::ChannelInfo;
    use crate::communication::buffer::BufferedPush;
    use crate::communication::cancel::{CancelHandle, DynSingleConsCancelPtr, MultiConsCancelPtr};
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::decorator::{ScopeStreamBuffer, ScopeStreamPush};
    use crate::communication::Magic;
    use crate::data::MicroBatch;
    use crate::errors::{IOError, IOResult};
    use crate::graph::Port;
    use crate::progress::{EndSignal, Weight};
    use crate::{Data, Tag};

    pub struct ExchangeMicroBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        pushes: Vec<BufferedPush<D, EventEmitPush<D>>>,
        routing: Box<dyn RouteFunction<D>>,
        magic: Magic,
        cancel_handle: MultiConsCancelPtr,
    }

    impl<D: Data> ExchangeMicroBatchPush<D> {
        pub(crate) fn new(
            ch_info: ChannelInfo, pushes: Vec<BufferedPush<D, EventEmitPush<D>>>,
            routing: Box<dyn RouteFunction<D>>,
        ) -> Self {
            let len = pushes.len();
            let magic =
                if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
            let cancel_table = MultiConsCancelPtr::new(ch_info.scope_level, len);
            ExchangeMicroBatchPush { ch_info, pushes, routing, magic, cancel_handle: cancel_table }
        }

        pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
            CancelHandle::MC(self.cancel_handle.clone())
        }

        pub(crate) fn push_batch(&mut self, mut batch: MicroBatch<D>) -> IOResult<()> {
            let len = batch.len();
            if len > 1 {
                let tag = batch.tag.clone();
                let mut iter = batch.drain();
                if let Err(err) = self.try_push_iter(&tag, &mut iter) {
                    if err.is_would_block() {
                        Err(IOError::cannot_block())
                    } else {
                        Err(err)
                    }
                } else {
                    std::mem::drop(iter);
                    if let Some(mut end) = batch.take_end() {
                        self.update_weight(&mut end, None);
                        self.notify_end(end)?;
                    }
                    Ok(())
                }
            } else if len > 0 {
                assert_eq!(len, 1);
                let target = {
                    let mut x = batch.iter().expect("expect iter but none;");
                    let last = x.next().expect("expect at least one;");
                    self.magic.exec(self.routing.route(last)?) as usize
                };
                if let Some(mut end) = batch.take_end() {
                    if batch.seq > 0 {
                        self.update_weight(&mut end, Some(target));
                    }

                    for i in 0..self.pushes.len() {
                        if i != target {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                    }
                    batch.set_end(end);
                }
                self.pushes[target].forward_buffer(batch)
            } else {
                assert_eq!(len, 0);
                if let Some(mut end) = batch.take_end() {
                    if batch.seq > 0 {
                        self.update_weight(&mut end, None);
                    }
                    if self.pushes.len() > 1 {
                        for i in 1..self.pushes.len() {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                    }
                    self.pushes[0].notify_end(end)
                } else {
                    Ok(())
                }
            }
        }

        fn update_weight(&self, end: &mut EndSignal, target: Option<usize>) {
            // let total = crate::worker_id::get_current_worker_uncheck().peers;
            if end.source_weight.value() < self.pushes.len() {
                let mut weight = Weight::partial_empty();
                if let Some(t) = target {
                    weight.add_source(t as u32);
                }
                for (index, p) in self.pushes.iter().enumerate() {
                    if Some(index) != target && p.has_buffer(&end.tag) {
                        weight.add_source(index as u32);
                    }
                }
                if weight.value() > 1 {
                    trace_worker!("try to update eos weight to {:?} of scope {:?}", weight, end.tag);
                    end.update_to(weight);
                }
            }
        }
    }

    impl<D: Data> ScopeStreamBuffer for ExchangeMicroBatchPush<D> {
        fn scope_size(&self) -> usize {
            let len = self.pushes.len();
            if len == 0 {
                0
            } else {
                let mut size = !0u32 as usize;
                for i in 0..len {
                    size = std::cmp::min(self.pushes[i].scope_size(), size);
                }
                size
            }
        }

        fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
            for push in &mut self.pushes[1..] {
                push.ensure_capacity(tag)?;
            }
            self.pushes[0].ensure_capacity(tag)
        }

        fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
            for push in self.pushes.iter_mut() {
                push.flush_scope(tag)?;
            }
            Ok(())
        }
    }

    impl<D: Data> ScopeStreamPush<D> for ExchangeMicroBatchPush<D> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
            let len = self.pushes.len();
            if len == 1 {
                return self.pushes[0].push(tag, msg);
            }

            let target = self.magic.exec(self.routing.route(&msg)?) as usize;

            if !self.cancel_handle.is_canceled(tag, target) {
                self.pushes[target].push(tag, msg)
            } else {
                Ok(())
            }
        }

        fn push_last(&mut self, msg: D, mut end: EndSignal) -> IOResult<()> {
            let len = self.pushes.len();
            if len == 1 {
                return self.pushes[0].push_last(msg, end);
            }

            let target = self.magic.exec(self.routing.route(&msg)?) as usize;
            self.update_weight(&mut end, Some(target));

            for (i, p) in self.pushes.iter_mut().enumerate() {
                if i != target {
                    p.notify_end(end.clone())?;
                }
            }

            self.pushes[target].push_last(msg, end)
        }

        fn try_push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            if self.pushes.len() == 1 {
                if !self.cancel_handle.is_canceled(tag, 0) {
                    self.pushes[0].try_push_iter(tag, iter)
                } else {
                    trace_worker!(
                        "EARLY_START: output[{:?}] ch: {} cancel push data of {:?} to worker 0;",
                        self.ch_info.scope_level,
                        self.ch_info.index(),
                        tag
                    );
                    Ok(())
                }
            } else {
                match self.magic {
                    Magic::Modulo(x) => {
                        for next in iter {
                            let idx = (self.routing.route(&next)? % x) as usize;
                            if !self.cancel_handle.is_canceled(tag, idx) {
                                self.pushes[idx].push(tag, next)?;
                            }
                        }
                    }
                    Magic::And(x) => {
                        for next in iter {
                            let idx = (self.routing.route(&next)? & x) as usize;
                            if !self.cancel_handle.is_canceled(tag, idx) {
                                self.pushes[idx as usize].push(tag, next)?;
                            }
                        }
                    }
                }
                Ok(())
            }
        }

        fn notify_end(&mut self, mut end: EndSignal) -> IOResult<()> {
            self.update_weight(&mut end, None);
            for p in &mut self.pushes[1..] {
                p.notify_end(end.clone())?;
            }
            self.pushes[0].notify_end(end)
        }

        fn flush(&mut self) -> IOResult<()> {
            for push in self.pushes.iter_mut() {
                push.flush()?;
            }
            Ok(())
        }

        fn close(&mut self) -> IOResult<()> {
            self.flush()?;
            for p in self.pushes.iter_mut() {
                p.close()?;
            }
            Ok(())
        }
    }

    pub struct ExchangeByScopePush<D: Data> {
        pub ch_info: ChannelInfo,
        pushes: Vec<EventEmitPush<D>>,
        magic: Magic,
        has_cycles: Arc<AtomicBool>,
        cancel_handle: DynSingleConsCancelPtr,
    }

    impl<D: Data> ExchangeByScopePush<D> {
        pub fn new(ch_info: ChannelInfo, cyclic: &Arc<AtomicBool>, pushes: Vec<EventEmitPush<D>>) -> Self {
            assert!(ch_info.scope_level > 0);
            let len = pushes.len();
            let magic = Magic::new(len);
            let cancel_handle = DynSingleConsCancelPtr::new(ch_info.scope_level, len);
            ExchangeByScopePush { ch_info, pushes, magic, has_cycles: cyclic.clone(), cancel_handle }
        }

        pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
            CancelHandle::DSC(self.cancel_handle.clone())
        }
    }

    impl<D: Data> ScopeStreamPush<MicroBatch<D>> for ExchangeByScopePush<D> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, tag: &Tag, msg: MicroBatch<D>) -> IOResult<()> {
            let idx = tag.current_uncheck() as u64;
            let offset = self.magic.exec(idx) as usize;
            if self.cancel_handle.is_canceled(tag, offset) {
                return Ok(());
            }
            self.pushes[offset].push(tag, msg)
        }

        fn push_last(&mut self, msg: MicroBatch<D>, mut end: EndSignal) -> IOResult<()> {
            let idx = msg.tag.current_uncheck() as u64;
            let offset = self.magic.exec(idx) as usize;
            if self.cancel_handle.is_canceled(&msg.tag, offset) {
                return self.notify_end(end);
            }
            if self.has_cycles.load(Ordering::SeqCst) {
                for (i, p) in self.pushes.iter_mut().enumerate() {
                    if i != offset {
                        p.notify_end(end.clone())?;
                    }
                }
            } else {
                // end is only send to aggregate target,
                let w = crate::worker_id::get_current_worker().index;
                end.update_weight = Some(Weight::single(w));
            }
            self.pushes[offset].push_last(msg, end)
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            if end.tag.len() < self.ch_info.scope_level as usize || self.has_cycles.load(Ordering::SeqCst) {
                for p in self.pushes.iter_mut() {
                    p.notify_end(end.clone())?;
                }
            } else {
                let idx = end.tag.current_uncheck() as u64;
                let offset = self.magic.exec(idx) as usize;
                self.pushes[offset].notify_end(end)?;
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
}
////////////////////////////////////////////////////
#[cfg(feature = "rob")]
mod rob {
    use std::collections::VecDeque;
    use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

    use pegasus_common::buffer::ReadBuffer;

    use crate::api::function::{FnResult, RouteFunction};
    use crate::channel_id::ChannelInfo;
    use crate::communication::buffer::ScopeBufferPool;
    use crate::communication::cancel::{CancelHandle, DynSingleConsCancelPtr, MultiConsCancelPtr};
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::decorator::BlockPush;
    use crate::communication::{IOResult, Magic};
    use crate::data::MicroBatch;
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
        _cyclic: Arc<AtomicBool>,
        blocks: TidyTagMap<VecDeque<BlockEntry<D>>>,
        cancel_handle: MultiConsCancelPtr,
    }

    impl<D: Data> ExchangeMicroBatchPush<D> {
        pub(crate) fn new(
            info: ChannelInfo, _cyclic: Arc<AtomicBool>, router: Box<dyn RouteFunction<D>>,
            buffers: Vec<ScopeBufferPool<D>>, pushes: Vec<EventEmitPush<D>>,
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
                _cyclic,
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

        fn update_end_weight(&mut self, target: Option<usize>, end: &mut EndSignal) {
            let src_weight = end.source_weight.value();
            if src_weight < self.pushes.len() {
                let mut weight = Weight::partial_empty();
                if let Some(ref target) = target {
                    weight.add_source(*target as u32);
                }
                for (index, p) in self.pushes.iter().enumerate() {
                    if let Some(ref target) = target {
                        if *target == index {
                            continue;
                        }
                    }

                    if p.get_push_count(&end.tag).unwrap_or(0) > 0 {
                        weight.add_source(index as u32);
                    }
                }

                trace_worker!(
                        "output[{:?}]: try to update eos weight to {:?} of scope {:?}",
                        self.port,
                        weight,
                        end.tag
                    );
                end.update_to(weight);
            }
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
            let tag = batch.tag();
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
                                trace_worker!(
                                    "output[{:?}] blocked when push data of {:?} ;",
                                    self.port,
                                    tag
                                );
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
                if let Some(mut end) = batch.take_end() {
                    self.flush_last_buffer(&batch.tag)?;
                    self.update_end_weight(None, &mut end);
                    for i in 0..self.pushes.len() {
                        self.pushes[i].notify_end(end.clone())?;
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

            if self.pushes.len() == 1 {
                if !self.cancel_handle.is_canceled(&batch.tag, 0) {
                    self.pushes[0].push(batch)?;
                } else if let Some(end) = batch.take_end() {
                    self.pushes[0].notify_end(end)?;
                }
                return Ok(());
            }

            let len = batch.len();
            let level = batch.tag.len() as u32;
            if len == 0 {
                if let Some(mut end) = batch.take_end() {
                    if level == self.scope_level {
                        // this is an end of scope of current level;
                        self.flush_last_buffer(&end.tag)?;
                        self.update_end_weight(None, &mut end);
                        for i in 1..self.pushes.len() {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                        self.pushes[0].notify_end(end)
                    } else {
                        // this is an end of parent scope;
                        for i in 1..self.pushes.len() {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                        self.pushes[0].notify_end(end)
                    }
                } else {
                    warn_worker!("output[{:?}]: empty batch of {:?};", self.port, batch.tag);
                    Ok(())
                }
            } else if len == 1 {
                // only one data, not need re-batching;
                if let Some(mut end) = batch.take_end() {
                    let x = batch
                        .get(0)
                        .expect("expect at least one entry as len = 1");
                    let target = self.route.route(x)? as usize;

                    if batch.get_seq() == 0 {
                        // the first and last batch, with only one message;
                        // won't flush or update;
                        batch.set_end(end);
                        return self.pushes[target].push(batch);
                    } else {
                        // flush previous buffered data;
                        self.flush_last_buffer(&batch.tag)?;
                        // update after flush;
                        self.update_end_weight(Some(target), &mut end);
                    }

                    if end.source_weight.value() == 1 {
                        // will send end through data queue;
                        batch.set_end(end.clone());
                        self.pushes[target].push(batch)?;
                        for i in 0..self.pushes.len() {
                            if i != target {
                                self.pushes[i].notify_end(end.clone())?;
                            }
                        }
                        Ok(())
                    } else {
                        self.pushes[target].push(batch)?;
                        for i in 1..self.pushes.len() {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                        self.pushes[0].notify_end(end)
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
                            Ok(Some(buf)) => self.push_to(target, batch.tag(), buf),
                            Ok(None) => Ok(()),
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
                                would_block!("no buffer available in exchange;")
                            }
                        }
                    } else {
                        Ok(())
                    }
                }
            } else {
                let tag = batch.tag.clone();
                for p in self.buffers.iter_mut() {
                    p.pin(&tag);
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
        has_cycles: Arc<AtomicBool>,
        cancel_handle: DynSingleConsCancelPtr,
    }

    impl<D: Data> ExchangeByScopePush<D> {
        pub fn new(ch_info: ChannelInfo, cyclic: &Arc<AtomicBool>, pushes: Vec<EventEmitPush<D>>) -> Self {
            assert!(ch_info.scope_level > 0);
            let len = pushes.len();
            let magic = Magic::new(len);
            let cancel_handle = DynSingleConsCancelPtr::new(ch_info.scope_level, len);
            ExchangeByScopePush { ch_info, pushes, magic, has_cycles: cyclic.clone(), cancel_handle }
        }

        pub(crate) fn get_cancel_handle(&self) -> CancelHandle {
            CancelHandle::DSC(self.cancel_handle.clone())
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for ExchangeByScopePush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            let end = batch.take_end();
            if !batch.is_empty() {
                let cur = batch.tag.current_uncheck() as u64;
                let target = self.magic.exec(cur) as usize;
                if !self
                    .cancel_handle
                    .is_canceled(&batch.tag, target)
                {
                    self.pushes[target].push(batch)?;
                }
            }

            if let Some(mut end) = end {
                if end.tag.len() < self.ch_info.scope_level as usize
                    || self.has_cycles.load(Ordering::SeqCst)
                {
                    if self.pushes.len() > 1 {
                        for i in 1..self.pushes.len() {
                            self.pushes[i].notify_end(end.clone())?;
                        }
                    }
                    self.pushes[0].notify_end(end)
                } else {
                    let idx = end.tag.current_uncheck() as u64;
                    let offset = self.magic.exec(idx) as usize;
                    let w = crate::worker_id::get_current_worker().index;
                    end.update_weight = Some(Weight::single(w));
                    self.pushes[offset].notify_end(end)
                }
            } else {
                Ok(())
            }
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
}
