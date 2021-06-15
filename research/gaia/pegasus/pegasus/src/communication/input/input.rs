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

use crate::channel_id::SubChannelId;
use crate::communication::channel::ChannelMeta;
use crate::communication::input::InputProxy;
use crate::data::DataSet;
use crate::data_plane::{GeneralPull, Pull};
use crate::errors::IOResult;
use crate::event::{ChannelRxState, Event, EventBus, EventKind, Panel};
use crate::{Data, Tag};
use ahash::AHashMap;
use pegasus_common::downcast::*;
use pegasus_common::rc::RcPointer;
use std::cell::{Cell, Ref, RefCell};
use std::collections::{HashSet, VecDeque};
use std::time::Instant;

struct Stash<D> {
    one_shot: Option<DataSet<D>>,
    queued: Option<VecDeque<DataSet<D>>>,
}

impl<D: Data> Stash<D> {
    pub fn new(data: DataSet<D>) -> Self {
        Stash { one_shot: Some(data), queued: None }
    }

    pub fn push(&mut self, data: DataSet<D>) {
        if let Some(ref mut queued) = self.queued {
            queued.push_back(data);
        } else if self.one_shot.is_none() {
            self.one_shot = Some(data);
        } else {
            let mut queue = VecDeque::new();
            queue.push_back(data);
            self.queued = Some(queue);
        }
    }

    pub fn pop(&mut self) -> Option<DataSet<D>> {
        if let Some(one_shot) = self.one_shot.take() {
            Some(one_shot)
        } else if let Some(ref mut queued) = self.queued {
            queued.pop_front()
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.one_shot = None;
        if let Some(mut queued) = self.queued.take() {
            queued.clear()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.one_shot.is_none() && self.queued.as_ref().map(|q| q.is_empty()).unwrap_or(true)
    }
}

struct StashedData<D> {
    pub tag: Tag,
    stash: RefCell<Stash<D>>,
    panel: RefCell<Option<RcPointer<Panel>>>,
    abandoned: Cell<bool>,
    len: Cell<usize>,
}

impl<D: Data> StashedData<D> {
    pub fn new(tag: Tag, data: DataSet<D>, panel: Option<RcPointer<Panel>>) -> Self {
        let len = data.len();
        // trace_worker!("stash {} data of {:?}", len, tag);
        StashedData {
            tag,
            stash: RefCell::new(Stash::new(data)),
            panel: RefCell::new(panel),
            abandoned: Cell::new(false),
            len: Cell::new(len),
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<DataSet<D>> {
        assert!(!self.abandoned.get());
        if let Some(data) = self.stash.borrow_mut().pop() {
            self.len.set(self.len.get() - data.len());
            Some(data)
        } else {
            None
        }
    }

    #[inline]
    pub fn stash(&self, data: DataSet<D>) -> bool {
        if let Some(panel) = self.panel.borrow().as_ref() {
            if panel.is_skipped() {
                return false;
            }
        }
        // trace_worker!("stash {} data of {:?}", data.len(), self.tag);
        self.len.set(self.len.get() + data.len());
        self.stash.borrow_mut().push(data);
        true
    }

    #[inline]
    pub fn has_stash(&self) -> bool {
        !self.stash.borrow().is_empty()
    }

    pub fn skip_all(&self) -> usize {
        self.stash.borrow_mut().clear();
        if let Some(panel) = self.panel.borrow().as_ref() {
            panel.skip();
        }
        self.abandoned.set(true);
        let len = self.len.get();
        self.len.set(0);
        len
    }

    pub fn get_panel(&self) -> Ref<Option<RcPointer<Panel>>> {
        self.panel.borrow()
    }

    pub fn has_panel(&self) -> bool {
        self.panel.borrow().is_some()
    }

    pub fn set_panel(&self, panel: RcPointer<Panel>) {
        assert!(self.panel.borrow().is_none());
        self.panel.borrow_mut().replace(panel);
    }

    pub fn set_panel_if_absent(&self, state: &RcPointer<ChannelRxState>) {
        if self.panel.borrow().is_none() {
            if let Some(panel) = state.get_panel(&self.tag) {
                self.set_panel(panel);
            }
        }
    }

    #[inline]
    pub fn is_abandoned(&self) -> bool {
        self.abandoned.get()
    }
}

pub struct InboundChannel<D: Data> {
    pub ch_id: SubChannelId,
    pub peers: usize,
    pub scope_depth: usize,
    pub disconnected: bool,
    pub forbid_cancel: bool,
    pull: GeneralPull<DataSet<D>>,
    stash_index: AHashMap<Tag, usize>,
    stash_data: Vec<StashedData<D>>,
    stashed_scope: VecDeque<Option<Tag>>,
    event_bus: EventBus,
    state: RcPointer<ChannelRxState>,
    stash_cost: u128,
    skip_st: usize,
}

struct Session {
    ch_index: Option<u32>,
    stash_index: Option<usize>,
    panel: Option<RcPointer<Panel>>,
}

impl Session {
    fn new() -> Self {
        Session { ch_index: None, stash_index: None, panel: None }
    }
}

thread_local! {
    static PIN_SESSION : RefCell<Session> = RefCell::new(Session::new());
}

impl<D: Data> InboundChannel<D> {
    pub(crate) fn new(
        meta: ChannelMeta, scope_depth: usize, event_bus: EventBus, pull: GeneralPull<DataSet<D>>,
    ) -> Self {
        let ch_id = meta.id;
        let push_peers = meta.push_peers;
        let mut queue = VecDeque::new();
        queue.push_back(None);
        InboundChannel {
            ch_id,
            peers: push_peers,
            scope_depth,
            disconnected: false,
            forbid_cancel: meta.forbid_cancel,
            pull,
            stash_index: AHashMap::new(),
            stash_data: Vec::new(),
            stashed_scope: queue,
            event_bus,
            state: RcPointer::new(ChannelRxState::new(ch_id.index(), push_peers, scope_depth)),
            stash_cost: 0,
            skip_st: 0,
        }
    }

    #[inline]
    pub(crate) fn pin(&self, tag: &Tag) -> bool {
        let ch_index = self.ch_id.index();
        let index = self.stash_index.get(tag).map(|x| *x);
        let panel = if let Some(index) = index {
            self.stash_data[index].get_panel().clone().or_else(|| self.state.get_panel(tag))
        } else {
            self.state.get_panel(tag)
        };

        // if panel.is_none() {
        //     warn_worker!("can not find panel for {:?} in ch: {}", tag, self.ch_id.index());
        // }
        let pinned = panel.as_ref().map(|p| p.has_outstanding()).unwrap_or(false);
        PIN_SESSION.with(|pin| {
            let mut pin = pin.borrow_mut();
            pin.ch_index = Some(ch_index);
            pin.stash_index = index;
            pin.panel = panel;
        });
        pinned
    }

    fn fetch_stash(&self, tag: &Tag) -> Option<&StashedData<D>> {
        if let Some(offset) = PIN_SESSION.with(|pin| pin.borrow().stash_index) {
            Some(&self.stash_data[offset])
        } else {
            if let Some(offset) = self.stash_index.get(tag) {
                Some(&self.stash_data[*offset])
            } else {
                None
            }
        }
    }

    fn get_stash(&self, tag: &Tag) -> Option<&StashedData<D>> {
        if let Some(offset) = self.stash_index.get(tag) {
            Some(&self.stash_data[*offset])
        } else {
            None
        }
    }

    pub fn pull_scope(&mut self, tag: &Tag) -> IOResult<Option<(DataSet<D>, bool)>> {
        assert_eq!(tag.len(), self.scope_depth);
        if let Some(st) = self.fetch_stash(tag) {
            st.set_panel_if_absent(&self.state);
            if let Some(panel) = st.get_panel().as_ref() {
                if panel.has_outstanding() {
                    if let Some(data) = st.pop() {
                        panel.add_pulled(data.len());
                        let has_more = panel.has_outstanding();
                        return Ok(Some((data, has_more)));
                    }
                } else {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }

        let pin_panel = PIN_SESSION.with(|pin| pin.borrow().panel.clone());
        // out of stash;
        if let Some(panel) = pin_panel {
            let start = Instant::now();
            let result = if let Some(data) = self.pull_until(tag)? {
                panel.add_pulled(data.len());
                let has_more = panel.has_outstanding();
                Some((data, has_more))
            } else {
                None
            };
            self.stash_cost += start.elapsed().as_micros();
            Ok(result)
        } else {
            Ok(None)
        }
    }

    fn pull_until(&mut self, tag: &Tag) -> IOResult<Option<DataSet<D>>> {
        let mut limit = 8;
        while limit > 0 {
            match self.pull.pull()? {
                Some(data) => {
                    // trace_worker!("pull data {:?} in ch: {}", data, self.ch_id.index());
                    if &data.tag == tag {
                        return Ok(Some(data));
                    } else {
                        self.stash(data);
                        limit -= 1;
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    pub fn next(&mut self, target: &HashSet<Tag>) -> IOResult<Option<Tag>> {
        for stash in self.stash_data.iter() {
            stash.set_panel_if_absent(&self.state);
            if stash.has_panel() && stash.has_stash() && target.contains(&stash.tag) {
                return Ok(Some(stash.tag.clone()));
            }
        }

        while !self.is_exhaust() {
            match self.pull.pull()? {
                Some(data) => {
                    let tag = data.tag();
                    if self.stash(data) {
                        return Ok(Some(tag));
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    fn stash(&mut self, data: DataSet<D>) -> bool {
        let tag = data.tag();
        let log_trace = crate::worker_id::is_in_trace();
        let mut has_stashed = true;
        if let Some(stashed) = self.get_stash(&tag) {
            let len = data.len();
            if !stashed.stash(data) {
                has_stashed = false;
                self.skip_st += len;
                if log_trace {
                    debug_worker!(
                        "discard {} data of scope {:?} in ch: {};",
                        len,
                        tag,
                        self.ch_id.index()
                    );
                }
            }
        } else if let Some(panel) = self.state.get_panel(&tag) {
            if !panel.is_skipped() {
                panel.set_seq(0);
                let stashed = StashedData::new(tag.clone(), data, Some(panel));
                self.stash_data.push(stashed);
                self.stash_index.insert(tag.clone(), self.stash_data.len() - 1);
            } else {
                has_stashed = false;
                self.skip_st += data.len();
                if log_trace {
                    debug_worker!(
                        "discard {} data of scope {:?} in ch: {}",
                        data.len(),
                        &tag,
                        self.ch_id.index()
                    );
                }
            }
        } else {
            // the panel is absent,
            if !self.state.is_scope_skipped(&tag.to_parent_uncheck()) {
                let stashed = StashedData::new(tag.clone(), data, None);
                self.stash_data.push(stashed);
                self.stash_index.insert(tag.clone(), self.stash_data.len() - 1);
            } else {
                has_stashed = false;
                self.skip_st += data.len();
                if log_trace {
                    debug_worker!(
                        "discard {} data of scope {:?} in ch: {}",
                        data.len(),
                        &tag,
                        self.ch_id.index()
                    );
                }
            }
        }

        if has_stashed {
            self.stashed_scope.push_front(Some(tag));
        }
        has_stashed
    }

    pub fn cancel(&mut self, tag: &Tag) {
        if self.forbid_cancel {
            return;
        }
        debug_worker!("will discard data of scope : {:?} in ch: {}", tag, self.ch_id.index());
        if tag.len() == self.scope_depth {
            if let Some(offset) = self.stash_index.remove(tag) {
                let stashed = &self.stash_data[offset];
                if !stashed.is_abandoned() {
                    let len = stashed.skip_all();
                    self.skip_st += len;
                    if len > 0 && crate::worker_id::is_in_trace() {
                        info_worker!(
                            "discard {} data of scope {:?} in ch: {};",
                            len,
                            tag,
                            self.ch_id.index()
                        );
                    }
                }
            } else if let Some(panel) = self.state.get_panel(tag) {
                if !panel.is_skipped() {
                    panel.skip();
                }
            }
        } else {
            let index = self.ch_id.index();
            let skip_st = &mut self.skip_st;
            let stash_data = std::mem::replace(&mut self.stash_data, vec![]);
            self.stash_index.retain(|child, offset| {
                let remove = tag.is_parent_of(child);
                if remove {
                    let len = stash_data[*offset].skip_all();
                    *skip_st += len;
                    if len > 0 && crate::worker_id::is_in_trace() {
                        info_worker!("discard {} data of scope {:?} in ch: {};", len, child, index);
                    }
                }
                !remove
            });

            self.state.skip_data_of(tag);
        }

        if tag.len() <= self.scope_depth {
            self.propagate_cancel(tag);
        }
    }

    #[inline]
    pub fn is_exhaust(&self) -> bool {
        self.state.is_exhaust()
    }

    #[inline]
    pub fn get_state(&self) -> &RcPointer<ChannelRxState> {
        &self.state
    }

    #[inline]
    fn propagate_cancel(&self, tag: &Tag) {
        let event =
            Event::new(tag.clone(), self.ch_id.index(), EventKind::Discard(self.ch_id.worker));
        if self.peers > 1 {
            self.event_bus.broadcast(event).ok();
        } else {
            self.event_bus.send_self(event).ok();
        }
    }
}

impl<D: Data> Drop for InboundChannel<D> {
    fn drop(&mut self) {
        if crate::worker_id::is_in_trace() {
            let cost = self.stash_cost as f64 / 1000.0;
            info_worker!(
                "[ch: {}] => stash data cost : {:.1} millis, skip pull {};",
                self.ch_id.index(),
                cost,
                self.skip_st
            );
        }
    }
}

pub struct RefWrapInput<D: Data> {
    state: RcPointer<ChannelRxState>,
    pub(super) inbound: RefCell<InboundChannel<D>>,
}

impl<D: Data> RefWrapInput<D> {
    pub fn downcast(input: &Box<dyn InputProxy>) -> &Self {
        input.as_any_ref().downcast_ref::<Self>().expect("downcast failure")
    }

    pub fn wrap(inbound: InboundChannel<D>) -> Self {
        let state = inbound.get_state().clone();
        RefWrapInput { state, inbound: RefCell::new(inbound) }
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
    #[inline(always)]
    fn index(&self) -> usize {
        self.inbound.borrow().ch_id.index() as usize
    }

    #[inline(always)]
    fn is_exhaust(&self) -> bool {
        self.inbound.borrow().is_exhaust()
    }

    fn cancel(&self, tag: &Tag) {
        self.inbound.borrow_mut().cancel(tag)
    }

    fn next(&self, target: &HashSet<Tag>) -> IOResult<Option<Tag>> {
        self.inbound.borrow_mut().next(target)
    }

    fn get_state(&self) -> &RcPointer<ChannelRxState> {
        &self.state
    }
}
