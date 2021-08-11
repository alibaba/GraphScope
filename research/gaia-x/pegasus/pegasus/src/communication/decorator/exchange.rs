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

#[derive(Copy, Clone)]
enum Magic {
    Modulo(u64),
    And(u64),
}

impl Magic {
    #[inline(always)]
    pub fn exec(&self, id: u64) -> u64 {
        match self {
            Magic::Modulo(x) => id % *x,
            Magic::And(x) => id & *x,
        }
    }
}

pub use rob::*;

#[cfg(not(feature = "rob"))]
mod rob {
    use super::*;
    use crate::api::function::RouteFunction;
    use crate::channel_id::ChannelInfo;
    use crate::communication::decorator::buffered::BufferedPush;
    use crate::communication::decorator::evented::EventEmitPush;
    use crate::communication::decorator::{ScopeStreamBuffer, ScopeStreamPush};
    use crate::data::MicroBatch;
    use crate::data_plane::GeneralPush;
    use crate::errors::IOResult;
    use crate::event::emitter::EventEmitter;
    use crate::graph::Port;
    use crate::progress::{EndSignal, Weight};
    use crate::{Data, Tag};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    pub struct ExchangeMicroBatchPush<D: Data> {
        pub ch_info: ChannelInfo,
        pushes: Vec<BufferedPush<D, EventEmitPush<D>>>,
        routing: Box<dyn RouteFunction<D>>,
        magic: Magic,
    }

    impl<D: Data> ExchangeMicroBatchPush<D> {
        pub fn new(
            ch_info: ChannelInfo, pushes: Vec<BufferedPush<D, EventEmitPush<D>>>,
            routing: Box<dyn RouteFunction<D>>,
        ) -> Self {
            let len = pushes.len();
            let magic =
                if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
            ExchangeMicroBatchPush { ch_info, pushes, routing, magic }
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
            self.pushes[target].push(tag, msg)
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

        fn push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
            if self.pushes.len() == 1 {
                self.pushes[0].push_iter(tag, iter)
            } else {
                match self.magic {
                    Magic::Modulo(x) => {
                        for next in iter {
                            let idx = self.routing.route(&next)? % x;
                            self.pushes[idx as usize].push(tag, next)?;
                        }
                    }
                    Magic::And(x) => {
                        for next in iter {
                            let idx = self.routing.route(&next)? & x;
                            self.pushes[idx as usize].push(tag, next)?;
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
    }

    impl<D: Data> ExchangeByScopePush<D> {
        pub fn new(
            ch_info: ChannelInfo, has_cycles: Arc<AtomicBool>, pushes: Vec<GeneralPush<MicroBatch<D>>>,
            event_emitter: &EventEmitter,
        ) -> Self {
            assert!(ch_info.scope_level > 0);
            let mut decorated = Vec::with_capacity(pushes.len());
            let source = crate::worker_id::get_current_worker().index;
            let len = pushes.len();
            for (i, p) in pushes.into_iter().enumerate() {
                let has_cycles = Arc::new(AtomicBool::new(true));
                let p = EventEmitPush::new(ch_info, source, i as u32, has_cycles, p, event_emitter.clone());
                decorated.push(p);
            }
            let magic =
                if len & (len - 1) == 0 { Magic::And(len as u64 - 1) } else { Magic::Modulo(len as u64) };
            ExchangeByScopePush { ch_info, pushes: decorated, magic, has_cycles }
        }
    }

    impl<D: Data> ScopeStreamPush<MicroBatch<D>> for ExchangeByScopePush<D> {
        fn port(&self) -> Port {
            self.ch_info.source_port
        }

        fn push(&mut self, tag: &Tag, msg: MicroBatch<D>) -> IOResult<()> {
            let idx = tag.current_uncheck() as u64;
            let offset = self.magic.exec(idx) as usize;
            self.pushes[offset].push(tag, msg)
        }

        fn push_last(&mut self, msg: MicroBatch<D>, mut end: EndSignal) -> IOResult<()> {
            let idx = end.tag.current_uncheck() as u64;
            let offset = self.magic.exec(idx) as usize;
            if self.has_cycles.load(Ordering::SeqCst) {
                for (i, p) in self.pushes.iter_mut().enumerate() {
                    if i != offset {
                        p.notify_end(end.clone())?;
                    }
                }
            } else {
                // end is only send to aggregate target,
                end.update_weight = Some(Weight::single());
            }
            self.pushes[offset].push_last(msg, end)
        }

        fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
            if end.tag.len() < self.ch_info.scope_level || self.has_cycles.load(Ordering::SeqCst) {
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
    use super::*;

    #[allow(dead_code)]
    struct Exchange<D> {
        magic: Magic,
        targets: u64,
        router: Box<dyn RouteFunction<D>>,
    }

    #[allow(dead_code)]
    impl<D: Data> Exchange<D> {
        #[inline]
        fn route(&self, item: &D) -> FnResult<u64> {
            let par_key = self.router.route(item)?;
            Ok(self.magic.exec(par_key))
        }
    }

    pub(crate) struct ExchangeMicroBatchPush<D: Data> {
        pub src: u32,
        buffers: Vec<ScopeBufferPool<D>>,
        pushes: Vec<EventEmitPush<D>>,
        route: Exchange<D>,
        blocks: Blocking<D>,
    }

    impl<D: Data> ExchangeMicroBatchPush<D> {
        fn end_scope(&mut self, index: usize, end: EndSignal) -> Result<(), IOError> {
            let tag = end.tag.clone();
            let mut last = self.buffers[index]
                .take_buf(&end.tag, true)
                .map(|buf| MicroBatch::new(tag.clone(), self.src, buf.into_read_only()))
                .unwrap_or_else(|| MicroBatch::new(tag, self.src, BufferReader::new()));
            last.set_end(end.clone());
            self.pushes[index].push(last)
        }
    }

    impl<D: Data> Push<MicroBatch<D>> for ExchangeMicroBatchPush<D> {
        fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
            if batch.is_empty() {
                if let Some(end) = batch.take_end() {
                    if cfg!(debug_assertions) {
                        for i in 0..self.pushes.len() {
                            assert!(
                                !self.blocks.has_blocks(i, &batch.tag),
                                "can't push more when still blocking"
                            )
                        }
                    }
                    for i in 0..self.pushes.len() {
                        self.end_scope(i, end.clone())?;
                    }
                } else {
                    warn_worker!("ignore empty micro batch of {:?}", batch.tag);
                }
                return Ok(());
            }

            let tag = batch.tag.clone();

            if batch.len() > 1 {
                for p in self.buffers.iter_mut() {
                    p.pin(&tag);
                }
            }

            let mut has_block = false;
            for item in batch.drain() {
                let target = self.route.route(&item)? as usize;
                match self.buffers[target].push(&tag, item) {
                    Ok(Some(buf)) => {
                        let b = MicroBatch::new(tag.clone(), self.src, buf);
                        self.pushes[target].push(b)?;
                    }
                    Err(e) => {
                        if let Some(x) = e.0 {
                            self.blocks.add_data(target, &tag, x);
                            has_block = true;
                        }
                    }
                    _ => (),
                }
            }

            if let Some(end) = batch.take_end() {
                for i in 0..self.pushes.len() {
                    if has_block && self.blocks.has_blocks(i, &tag) {
                        self.blocks.add_end(i, &tag, end.clone());
                    } else {
                        self.end_scope(i, end.clone())?;
                    }
                }
            }

            if has_block {
                would_block!("no buffer available")
            } else {
                Ok(())
            }
        }

        fn flush(&mut self) -> Result<(), IOError> {
            for i in 0..self.pushes.len() {
                for (tag, buf) in self.buffers[i].buffers() {
                    if buf.len() > 0 {
                        let batch = MicroBatch::new(tag, self.src, buf.into_read_only());
                        self.pushes[i].push(batch)?;
                    }
                }
            }
            Ok(())
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
            let mut unblocked = true;
            for i in 0..self.pushes.len() {
                if let Some((mut buf, end)) = self.blocks.take_block(i, tag) {
                    loop {
                        match self.buffers[i].push_iter(tag, &mut buf) {
                            Ok(Some(buf)) => {
                                let batch = MicroBatch::new(tag.clone(), self.src, buf);
                                self.pushes[i].push(batch)?;
                            }
                            Ok(None) => {
                                if let Some(end) = end {
                                    self.end_scope(i, end)?;
                                }
                                break;
                            }
                            Err(e) => {
                                if let Some(x) = e.0 {
                                    // TODO: may disrupt the order of data;
                                    buf.push(x);
                                }
                                self.blocks.waiting_queue[i].insert(tag.clone(), (buf, end));
                                unblocked = false;
                                break;
                            }
                        }
                    }
                }
            }
            Ok(unblocked)
        }
    }

    struct Blocking<D: Data> {
        buffer: BufferPool<D, MemBufAlloc<D>>,
        waiting_queue: Vec<TidyTagMap<(Buffer<D>, Option<EndSignal>)>>,
    }

    impl<D: Data> Blocking<D> {
        fn add_data(&mut self, index: usize, tag: &Tag, item: D) {
            if let Some(queue) = self.waiting_queue[index].get_mut(tag) {
                queue.0.push(item);
            } else {
                let mut buf = self
                    .buffer
                    .fetch()
                    .unwrap_or(Buffer::with_capacity(64));
                buf.push(item);
                self.waiting_queue[index].insert(tag.clone(), (buf, None));
            }
        }

        fn add_end(&mut self, index: usize, tag: &Tag, end: EndSignal) {
            if let Some(queue) = self.waiting_queue[index].get_mut(tag) {
                queue.1.replace(end);
            } else {
                let buf = self.buffer.fetch().unwrap_or(Buffer::new());
                self.waiting_queue[index].insert(tag.clone(), (buf, Some(end)));
            }
        }

        fn has_blocks(&self, index: usize, tag: &Tag) -> bool {
            self.waiting_queue[index].contains_key(tag)
        }

        fn take_block(&mut self, index: usize, tag: &Tag) -> Option<(Buffer<D>, Option<EndSignal>)> {
            self.waiting_queue[index].remove(tag)
        }
    }
}
