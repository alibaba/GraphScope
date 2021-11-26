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
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::AHashSet;
use pegasus_common::downcast::*;

use crate::channel_id::ChannelInfo;
use crate::communication::input::{InputProxy, InputSession};
use crate::data::MicroBatch;
use crate::data_plane::{GeneralPull, Pull};
use crate::errors::IOResult;
use crate::event::emitter::EventEmitter;
use crate::event::{Event, EventKind};
use crate::progress::EndOfScope;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

pub struct InputBlockGuard {
    pub tag: Tag,
    guard: Arc<AtomicUsize>,
}

impl Drop for InputBlockGuard {
    fn drop(&mut self) {
        self.guard.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct InputHandle<D: Data> {
    pub ch_info: ChannelInfo,
    pull: GeneralPull<MicroBatch<D>>,
    stash_index: TidyTagMap<StashedQueue<D>>,
    current_end: VecDeque<EndOfScope>,
    parent_ends: VecDeque<EndOfScope>,
    data_exhaust: bool,
    event_emitter: EventEmitter,
    // scope skip manager:
    cancel: TidyTagMap<()>,
    parent_cancel: AHashSet<Tag>,
}

impl<D: Data> InputHandle<D> {
    pub fn new(
        ch_info: ChannelInfo, pull: GeneralPull<MicroBatch<D>>, event_emitter: EventEmitter,
    ) -> Self {
        let scope_level = ch_info.scope_level;
        InputHandle {
            ch_info,
            pull,
            stash_index: TidyTagMap::new(scope_level),
            current_end: VecDeque::new(),
            parent_ends: VecDeque::new(),
            data_exhaust: false,
            event_emitter,
            cancel: TidyTagMap::new(scope_level),
            parent_cancel: AHashSet::new(),
        }
    }

    fn has_outstanding(&mut self) -> IOResult<bool> {
        if self.pull.has_next()? {
            Ok(true)
        } else {
            for s in self.stash_index.iter() {
                if !s.1.is_block() && !s.1.is_empty() {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }

    pub(crate) fn next(&mut self) -> IOResult<Option<MicroBatch<D>>> {
        if self.stash_index.is_empty() {
            if let Some(dataset) = self.pull()? {
                return Ok(Some(dataset));
            }
            Ok(None)
        } else {
            let mut stash_index = std::mem::replace(&mut self.stash_index, Default::default());
            let mut stashes = stash_index.iter_mut();
            // TODO: the iteration is without any order; Think more if order is needed;
            while let Some((_, stash)) = stashes.next() {
                if !stash.is_block() {
                    if let Some(dataset) = stash.pop_front() {
                        self.stash_index = stash_index;
                        return Ok(Some(dataset));
                    }
                }
            }
            self.stash_index = stash_index;
            while let Some(dataset) = self.pull()? {
                if let Some(stash) = self.stash_index.get_mut(&dataset.tag) {
                    if !stash.is_empty() || stash.is_block() {
                        //debug_worker!("stash data , len={}", dataset.len());
                        stash.stash(dataset);
                        continue;
                    }
                }
                return Ok(Some(dataset));
            }
            Ok(None)
        }
    }

    pub(crate) fn next_of(&mut self, tag: &Tag) -> IOResult<Option<MicroBatch<D>>> {
        let mut stash_index = std::mem::replace(&mut self.stash_index, Default::default());
        if let Some(stash) = stash_index.get_mut(tag) {
            if !stash.is_block() {
                if let Some(data_set) = stash.pop_front() {
                    self.stash_index = stash_index;
                    return Ok(Some(data_set));
                }
            } else {
                self.stash_index = stash_index;
                return Ok(None);
            }
        }
        self.stash_index = stash_index;
        for _ in 0..3 {
            if let Some(dataset) = self.pull()? {
                if &dataset.tag == tag {
                    return Ok(Some(dataset));
                } else {
                    self.stash_back(dataset);
                }
            }
        }

        Ok(None)
    }

    pub(crate) fn block(&mut self, tag: &Tag) -> InputBlockGuard {
        if let Some(queue) = self.stash_index.get_mut(tag) {
            if let Some(ref cnt) = queue.block_cnt {
                cnt.fetch_add(1, Ordering::SeqCst);
                InputBlockGuard { tag: tag.clone(), guard: cnt.clone() }
            } else {
                let cnt = Arc::new(AtomicUsize::new(1));
                queue.block_cnt = Some(cnt.clone());
                InputBlockGuard { tag: tag.clone(), guard: cnt }
            }
        } else {
            let mut queue = StashedQueue::new();
            let cnt = Arc::new(AtomicUsize::new(1));
            queue.block_cnt = Some(cnt.clone());
            self.stash_index.insert(tag.clone(), queue);
            InputBlockGuard { tag: tag.clone(), guard: cnt }
        }
    }

    fn stash_back(&mut self, dataset: MicroBatch<D>) {
        if let Some(stash) = self.stash_index.get_mut(&dataset.tag) {
            stash.stash(dataset);
        } else {
            let mut stashes = StashedQueue::new();
            let tag = dataset.tag.clone();
            stashes.push_back(dataset);
            self.stash_index.insert(tag.clone(), stashes);
        }
    }

    pub(crate) fn stash_block_front(&mut self, dataset: MicroBatch<D>) -> InputBlockGuard {
        let tag = dataset.tag.clone();
        if let Some(queue) = self.stash_index.get_mut(&dataset.tag) {
            queue.push_front(dataset);
            if let Some(ref cnt) = queue.block_cnt {
                let pre = cnt.fetch_add(1, Ordering::SeqCst);
                assert_eq!(0, pre);
                InputBlockGuard { tag, guard: cnt.clone() }
            } else {
                let cnt = Arc::new(AtomicUsize::new(1));
                queue.block_cnt = Some(cnt.clone());
                InputBlockGuard { tag: tag.clone(), guard: cnt }
            }
        } else {
            let mut queue = StashedQueue::new();
            queue.push_back(dataset);
            let cnt = Arc::new(AtomicUsize::new(1));
            queue.block_cnt = Some(cnt.clone());
            self.stash_index.insert(tag.clone(), queue);
            InputBlockGuard { tag, guard: cnt }
        }
    }

    pub(crate) fn end_on(&mut self, end: EndOfScope) {
        self.stash_index.remove(&end.tag);
        self.current_end.push_back(end);
    }

    pub(crate) fn is_exhaust(&self) -> bool {
        self.data_exhaust
            && self
                .stash_index
                .iter()
                .all(|(_, s)| s.is_empty())
    }

    pub(crate) fn extract_end(&mut self) -> Option<EndOfScope> {
        if !self.current_end.is_empty() {
            self.current_end.pop_front()
        } else {
            self.stash_index
                .retain(|_, s| !s.is_empty() || s.is_block());
            if self.stash_index.is_empty() {
                self.parent_ends.pop_front()
            } else {
                None
            }
        }
    }

    #[inline]
    fn pull(&mut self) -> IOResult<Option<MicroBatch<D>>> {
        if self.data_exhaust {
            return Ok(None);
        }
        loop {
            let result: IOResult<Option<MicroBatch<D>>> = self.pull.next();
            match result {
                Ok(Some(mut batch)) => {
                    if batch.tag.len() < self.ch_info.scope_level as usize {
                        // this is a end signal from parent scope;
                        assert!(batch.is_empty());
                        assert!(batch.is_last());
                        if batch.tag.is_root() {
                            debug_worker!("channel[{}] exhaust;", self.ch_info.index());
                            self.data_exhaust = true;
                        }
                        let end = batch.take_end().expect("unreachable");
                        trace_worker!(
                            "channel[{}] pulled end of scope{:?} peers: {:?}",
                            self.ch_info.index(),
                            batch.tag,
                            end.peers()
                        );
                        self.parent_ends.push_back(end);
                    } else {
                        if let Some(end) = batch.take_end() {
                            batch.set_end(end);
                            if batch.tag.is_root() {
                                debug_worker!("channel[{}] exhaust;", self.ch_info.index());
                                self.data_exhaust = true;
                            }
                        }

                        if self.is_discard(&batch.tag) {
                            batch.take_data();
                            trace_worker!(
                                "channel[{}] discard batch of {:?} from {};",
                                self.ch_info.id.index,
                                batch.tag,
                                batch.src
                            );
                            if batch.is_last() {
                                return Ok(Some(batch));
                            }
                        } else {
                            if log_enabled!(log::Level::Trace) {
                                if !batch.is_empty() {
                                    trace_worker!(
                                        "channel[{}] pulled batch(len={}) of {:?} from {}",
                                        self.ch_info.id.index,
                                        batch.len(),
                                        batch.tag,
                                        batch.src
                                    );
                                }

                                if let Some(end) = batch.get_end() {
                                    trace_worker!(
                                        "channel[{}] pulled end of scope{:?} peers: {:?}",
                                        self.ch_info.id.index,
                                        end.tag,
                                        end.peers(),
                                    );
                                }
                            }
                            return Ok(Some(batch));
                        }
                    }
                }
                Ok(None) => return Ok(None),
                Err(err) => {
                    return if err.is_source_exhaust() {
                        debug_worker!("channel[{}] closed;", self.ch_info.index());
                        assert!(self.data_exhaust);
                        Ok(None)
                    } else {
                        Err(err)
                    };
                }
            }
        }
    }

    pub fn cancel_scope(&mut self, tag: &Tag) {
        let level = tag.len() as u32;
        if level == self.ch_info.scope_level {
            // cancel scopes in current scope level;
            if self.cancel.insert(tag.clone(), ()).is_none() {
                trace_worker!(
                    "EARLY_STOP: channel[{}] cancel consume data of {:?};",
                    self.ch_info.index(),
                    tag
                );
                if let Some(stash) = self.stash_index.get_mut(tag) {
                    stash.discard();
                    if !stash.is_exhaust() {
                        self.propagate_cancel(tag);
                    } else {
                        // upstream had finished producing data of the tag;
                    }
                } else {
                    self.propagate_cancel(tag);
                }
            };
        } else if *crate::config::ENABLE_CANCEL_CHILD {
            // if it's a cancel signal from parent scope;
            assert!(level < self.ch_info.scope_level);
            if self.parent_cancel.insert(tag.clone()) {
                let mut stash_index = std::mem::replace(&mut self.stash_index, Default::default());
                for (child, stash) in stash_index.iter_mut() {
                    if tag.is_parent_of(&*child) {
                        trace_worker!("EARLY_STOP: channel[{}] cancel consume data of {:?} as it's parent scope {:?} been canceled;", self.ch_info.index(), child, tag);
                        self.cancel.insert((&*child).clone(), ());
                        stash.discard();
                        // todo: if need to propagate event of this child scope;
                    }
                }
                self.stash_index = stash_index;
                self.propagate_cancel(tag);
            }
        }
    }

    #[inline]
    fn is_discard(&self, tag: &Tag) -> bool {
        let level = tag.len() as u32;
        assert_eq!(level, self.ch_info.scope_level);
        if !self.cancel.is_empty() {
            if self.cancel.contains_key(tag) {
                return true;
            }
        }

        if !self.parent_cancel.is_empty() {
            let p = tag.to_parent_uncheck();
            if self.parent_cancel.contains(&p) {
                return true;
            }
        }

        false
    }

    pub fn propagate_cancel(&mut self, tag: &Tag) {
        let source = crate::worker_id::get_current_worker().index;
        let ch = self.ch_info.id.index;
        let event = Event::new(source, self.ch_info.source_port, EventKind::Cancel((ch, tag.clone())));
        let result = if self.ch_info.source_peers > 1 {
            trace_worker!(
                "EARLY_STOP: channel[{}] broadcast backward cancel signal of {:?} to {:?}",
                self.ch_info.index(),
                tag,
                self.ch_info.source_port
            );
            self.event_emitter.broadcast(event)
        } else {
            trace_worker!(
                "EARLY_STOP: channel[{}] send self backward cancel signal {:?} to {:?}",
                self.ch_info.index(),
                tag,
                self.ch_info.source_port
            );
            self.event_emitter.send(source, event)
        };

        if let Err(e) = result {
            error_worker!("propagate cancel of {:?} failure, error: {}", tag, e);
        }
    }
}

pub struct RefWrapInput<D: Data> {
    inbound: RefCell<InputHandle<D>>,
}

impl<D: Data> RefWrapInput<D> {
    pub fn downcast(input: &Box<dyn InputProxy>) -> &Self {
        input
            .as_any_ref()
            .downcast_ref::<Self>()
            .expect("downcast failure")
    }

    pub fn wrap(inbound: InputHandle<D>) -> Self {
        RefWrapInput { inbound: RefCell::new(inbound) }
    }

    pub fn new_session(&self) -> InputSession<D> {
        let input = self.inbound.borrow_mut();
        InputSession::new(input)
    }
}

impl<D: Data> AsAny for RefWrapInput<D> {
    #[inline(always)]
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    #[inline(always)]
    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D: Data> InputProxy for RefWrapInput<D> {
    #[inline]
    fn has_outstanding(&self) -> IOResult<bool> {
        self.inbound.borrow_mut().has_outstanding()
    }

    #[inline]
    fn block(&self, tag: &Tag) -> InputBlockGuard {
        self.inbound.borrow_mut().block(tag)
    }

    fn extract_end(&self) -> Option<EndOfScope> {
        self.inbound.borrow_mut().extract_end()
    }

    fn is_exhaust(&self) -> bool {
        self.inbound.borrow().is_exhaust()
    }

    fn cancel_scope(&self, tag: &Tag) {
        self.inbound.borrow_mut().cancel_scope(tag)
    }
}

struct StashedQueue<D> {
    skip: bool,
    block_cnt: Option<Arc<AtomicUsize>>,
    queue: VecDeque<MicroBatch<D>>,
}

impl<D> StashedQueue<D> {
    fn new() -> Self {
        StashedQueue { skip: false, block_cnt: None, queue: VecDeque::new() }
    }

    #[inline]
    fn is_block(&self) -> bool {
        if let Some(ref b) = self.block_cnt {
            let blocks = b.load(Ordering::SeqCst);
            blocks != 0
        } else {
            false
        }
    }

    fn is_exhaust(&self) -> bool {
        self.queue
            .back()
            .map(|b| b.is_last())
            .unwrap_or(false)
    }

    fn stash(&mut self, mut batch: MicroBatch<D>) {
        if batch.is_empty() {
            if let Some(end) = batch.take_end() {
                if let Some(last) = self.queue.back_mut() {
                    last.set_end(end);
                } else {
                    batch.set_end(end);
                    self.queue.push_back(batch);
                }
            }
        } else {
            if !self.skip {
                self.queue.push_back(batch)
            }
        }
    }

    fn discard(&mut self) {
        self.skip = true;
        let last = self.queue.pop_back();
        self.queue.clear();
        if let Some(mut last) = last {
            last.take_data();
            if last.is_last() {
                self.queue.push_back(last);
            }
        }
    }
}

impl<D> Deref for StashedQueue<D> {
    type Target = VecDeque<MicroBatch<D>>;

    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}

impl<D> DerefMut for StashedQueue<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.queue
    }
}
