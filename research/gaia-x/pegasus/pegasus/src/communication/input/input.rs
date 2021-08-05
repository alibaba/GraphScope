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
use crate::communication::input::{InputProxy, InputSession};
use crate::config::CANCEL_DESC;
use crate::data::DataSet;
use crate::data_plane::{GeneralPull, Pull};
use crate::errors::IOResult;
use crate::event::emitter::EventEmitter;
use crate::event::{Event, Signal};
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
use pegasus_common::downcast::*;
use std::cell::RefCell;
use std::collections::{HashSet, VecDeque};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    pull: GeneralPull<DataSet<D>>,
    stash_index: TidyTagMap<StashedQueue<D>>,
    current_end: VecDeque<EndSignal>,
    parent_ends: VecDeque<EndSignal>,
    data_exhaust: bool,
    event_emitter: EventEmitter,
    delta: MergedScopeDelta,
    skips: HashSet<Tag>,
}

impl<D: Data> InputHandle<D> {
    pub fn new(
        ch_info: ChannelInfo, pull: GeneralPull<DataSet<D>>, event_emitter: EventEmitter,
        delta: MergedScopeDelta,
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
            delta,
            skips: HashSet::new(),
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

    pub(crate) fn next(&mut self) -> IOResult<Option<DataSet<D>>> {
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

    pub(crate) fn next_of(&mut self, tag: &Tag) -> IOResult<Option<DataSet<D>>> {
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

    fn stash_back(&mut self, dataset: DataSet<D>) {
        if let Some(stash) = self.stash_index.get_mut(&dataset.tag) {
            stash.stash(dataset);
        } else {
            let mut stashes = StashedQueue::new();
            let tag = dataset.tag.clone();
            stashes.push_back(dataset);
            self.stash_index.insert(tag.clone(), stashes);
        }
    }

    pub(crate) fn stash_block_front(&mut self, dataset: DataSet<D>) -> InputBlockGuard {
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

    pub(crate) fn end_on(&mut self, end: EndSignal) {
        self.stash_index.remove(&end.tag);
        self.current_end.push_back(end);
    }

    pub fn is_exhaust(&self) -> bool {
        self.data_exhaust
            && self
                .stash_index
                .iter()
                .all(|(_, s)| s.is_empty())
    }

    pub fn extract_end(&mut self) -> Option<EndSignal> {
        if !self.current_end.is_empty() {
            self.current_end.pop_front()
        } else {
            self.stash_index.retain(|_, s| !s.is_empty());
            if self.stash_index.is_empty() {
                self.parent_ends.pop_front()
            } else {
                None
            }
        }
    }

    #[inline]
    fn pull(&mut self) -> IOResult<Option<DataSet<D>>> {
        if self.data_exhaust {
            return Ok(None);
        }
        loop {
            let result: IOResult<Option<DataSet<D>>> = self.pull.next();
            match result {
                Ok(Some(mut dataset)) => {
                    if self.is_skipped(&dataset.tag) {
                        if dataset.is_last() {
                            dataset.clear();
                        } else {
                            continue;
                        }
                    }
                    if dataset.tag.len() < self.ch_info.scope_level {
                        // this is a end signal from parent scope;
                        assert!(dataset.is_empty());
                        assert!(dataset.is_last());
                        if dataset.tag.is_root() {
                            debug_worker!("channel {:?} exhaust;", self.ch_info.id);
                            self.data_exhaust = true;
                        }
                        let end = dataset.take_end().expect("unreachable");
                        self.parent_ends.push_back(end);
                    } else {
                        if let Some(mut end) = dataset.take_end() {
                            end.update();
                            dataset.set_last(end);
                            if dataset.tag.is_root() {
                                debug_worker!("channel {:?} exhaust;", self.ch_info.id);
                                self.data_exhaust = true;
                            }
                        }
                        if log_enabled!(log::Level::Trace) {
                            if !dataset.is_empty() {
                                trace_worker!(
                                    "ch[{}] pulled dataset of {:?}, len = {} from {}",
                                    self.ch_info.id.index,
                                    dataset.tag,
                                    dataset.len(),
                                    dataset.src
                                );
                            }

                            if dataset.is_last() {
                                trace_worker!(
                                    "ch[{}] pulled end of scope {:?}",
                                    self.ch_info.id.index,
                                    dataset.tag
                                )
                            }
                        }
                        return Ok(Some(dataset));
                    }
                }
                Ok(None) => return Ok(None),
                Err(err) => {
                    return if err.is_source_exhaust() {
                        debug_worker!("channel {:?} closed;", self.ch_info.id);
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
        self.skips.insert(tag.clone());
        if *CANCEL_DESC && self.ch_info.scope_level > tag.len() {
            self.stash_index.retain(|t, stash| {
                if tag.is_parent_of(t) {
                    InputHandle::clear_stash(stash);
                }
                !stash.is_empty()
            });
        } else {
            if let Some(mut stash) = self.stash_index.remove(tag) {
                InputHandle::clear_stash(&mut stash);
                if !stash.is_empty() {
                    self.stash_index.insert(tag.clone(), stash);
                }
            }
        }
    }

    /// `clean_stash` is used to clean up all of the data except end signals of a given stash queue.
    /// Note that end signals may be combined with batch into `DataSet`, so we should carefully clean
    /// the data but retain the end signals.
    #[inline]
    fn clear_stash(stash: &mut StashedQueue<D>) {
        let len = stash.len();
        for _ in 0..len {
            if let Some(mut dataset) = stash.pop_front() {
                if dataset.is_last() {
                    // clear the batch data but keep the end signal
                    dataset.clear();
                    stash.push_back(dataset);
                }
            }
        }
    }

    #[inline]
    fn is_skipped(&self, tag: &Tag) -> bool {
        if *CANCEL_DESC && self.ch_info.scope_level > tag.len() {
            for skip_tag in self.skips.iter() {
                if skip_tag.is_parent_of(tag) {
                    return true;
                }
            }
            false
        } else {
            self.skips.contains(tag)
        }
    }

    /// `propagate_cancel` is used to propagate cancel based on the input delta.
    /// There are three situations of propagation:
    /// 1. When propagating through `ToSibling` delta, we should `evolve_back` the tag by reduce
    /// the current scope id by 1, this usually happens in loop cancel;
    /// 2. When propagating through `ToParent` or `None` delta, we should propagate the original tag without evolving;
    /// 4. When propagating through `ToChild` delta, we should stop the propagation.
    /// Return `true` means propagating successfully, `false` means reaching top-most.
    /// TODO(yyy): We may remove delta in input for two reasons: 1. delta is the concept of output for evolving
    /// TODO(yyy): data, and it looks strange here; 2. early-stop signal is able to be propagated one more operator to cancel
    /// TODO(yyy): the data of source output when handling tag evolving after receiving.
    pub fn propagate_cancel(&mut self, tag: &Tag) -> IOResult<bool> {
        let scope_level = self.ch_info.scope_level;
        if tag.len() == scope_level {
            // early-stop signal in the same scope level, tag evolving back may be needed
            let backward_tag = self.delta.evolve_back(tag);
            if backward_tag.len() == tag.len() {
                // ScopeDelta::None or ScopeDelta::ToSibling
                self.send_cancel(&backward_tag)?;
                Ok(true)
            } else if backward_tag.len() > tag.len() {
                // ScopeDelta::ToParent
                self.send_cancel(tag)?;
                Ok(true)
            } else {
                // ScopeDelta::ToChild, early-stop signal should not propagate backward to parent scope
                Ok(false)
            }
        } else if tag.len() < scope_level {
            // parent early-stop signal
            self.send_cancel(tag)?;
            Ok(true)
        } else {
            // tag.len() > scope_level, impossible
            unreachable!();
        }
    }

    /// propagating early-stop signal without checking the scope delta
    /// usually used for optimization
    pub fn propagate_cancel_uncheck(&mut self, tag: &Tag) -> IOResult<()> {
        let backward_tag = self.delta.evolve_back(tag);
        self.send_cancel(&backward_tag)?;
        Ok(())
    }

    #[inline]
    fn send_cancel(&mut self, tag: &Tag) -> IOResult<()> {
        let source = crate::worker_id::get_current_worker().index;
        let event = Event::new(source, self.ch_info.source_port, Signal::CancelSignal(tag.clone()));
        if self.ch_info.source_peers > 1 {
            debug_worker!("EARLY-STOP: broadcast backward cancel signal {:?}", event);
            self.event_emitter.broadcast(event)?;
        } else {
            debug_worker!("EARLY-STOP: send self backward cancel signal {:?}", event);
            self.event_emitter.send(source, event)?;
        }
        Ok(())
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

    fn extract_end(&self) -> Option<EndSignal> {
        self.inbound.borrow_mut().extract_end()
    }

    fn is_exhaust(&self) -> bool {
        self.inbound.borrow().is_exhaust()
    }

    fn propagate_cancel(&self, tag: &Tag) -> IOResult<bool> {
        self.inbound.borrow_mut().propagate_cancel(tag)
    }

    fn propagate_cancel_uncheck(&self, tag: &Tag) -> IOResult<()> {
        self.inbound
            .borrow_mut()
            .propagate_cancel_uncheck(tag)
    }

    fn cancel_scope(&self, tag: &Tag) {
        self.inbound.borrow_mut().cancel_scope(tag)
    }
}

struct StashedQueue<D> {
    block_cnt: Option<Arc<AtomicUsize>>,
    queue: VecDeque<DataSet<D>>,
}

impl<D> StashedQueue<D> {
    fn new() -> Self {
        StashedQueue { block_cnt: None, queue: VecDeque::new() }
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

    fn stash(&mut self, dataset: DataSet<D>) {
        if dataset.is_empty() {
            if dataset.is_last() {
                if let Some(last) = self.queue.back_mut() {
                    last.end = dataset.end;
                } else {
                    self.queue.push_back(dataset);
                }
            }
        } else {
            self.queue.push_back(dataset)
        }
    }
}

impl<D> Deref for StashedQueue<D> {
    type Target = VecDeque<DataSet<D>>;

    fn deref(&self) -> &Self::Target {
        &self.queue
    }
}

impl<D> DerefMut for StashedQueue<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.queue
    }
}
