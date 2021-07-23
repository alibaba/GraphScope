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

use super::utils::CountDownLatchTree;
use crate::tag::tools::{BlockGuard, TagAntiChainSet};
use crate::Tag;
use ahash::AHashMap;
use pegasus_common::rc::RcPointer;
use std::cell::{Cell, RefCell, RefMut};
use std::collections::HashSet;
use std::fmt;

#[derive(Default)]
pub struct Panel {
    seq: Cell<usize>,
    pushed: Cell<usize>,
    pulled: Cell<usize>,
    skipped: Cell<bool>,
    exhaust: Cell<bool>,
    block_guard: Cell<Option<BlockGuard>>,
}

impl Panel {
    #[inline]
    pub fn has_outstanding(&self) -> bool {
        (!self.skipped.get()) && self.pushed.get() > self.pulled.get()
    }

    #[inline]
    pub fn skip(&self) {
        if !self.skipped.get() {
            self.skipped.set(true);
            if let Some(guard) = self.block_guard.take() {
                guard.clear();
                self.block_guard.set(Some(guard));
            }
        }
    }

    #[inline]
    pub fn add_pushed(&self, size: usize) -> usize {
        let pushed = self.pushed.get() + size;
        self.pushed.set(pushed);
        if !self.skipped.get() {
            if let Some(guard) = self.block_guard.take() {
                assert!(size <= std::u32::MAX as usize, "pushed len overflow");
                guard.incr(size as u32);
                self.block_guard.set(Some(guard));
            }
        }
        pushed
    }

    #[inline]
    pub fn add_pulled(&self, size: usize) -> usize {
        let pulled = self.pulled.get() + size;
        self.pulled.set(pulled);
        if let Some(guard) = self.block_guard.take() {
            assert!(size <= std::u32::MAX as usize, "pulled len overflow");
            guard.decr(size as u32);
            self.block_guard.set(Some(guard));
        }
        trace_worker!("pulled {} panel: [{:?}]", size, self);
        pulled
    }

    #[inline]
    pub fn store_block_guard(&self, guard: BlockGuard) {
        self.block_guard.set(Some(guard));
    }

    #[inline]
    pub fn set_seq(&self, seq: usize) {
        self.seq.set(seq);
    }

    #[inline]
    pub fn get_seq(&self) -> usize {
        self.seq.get()
    }

    #[inline]
    pub fn has_block_guard(&self) -> bool {
        let x = self.block_guard.take();
        let r = x.is_some();
        self.block_guard.set(x);
        r
    }

    #[inline]
    pub fn is_source_exhaust(&self, tag: &Tag) -> bool {
        self.exhaust.get()
            || (!self.has_outstanding() && {
                if let Some(guard) = self.block_guard.take() {
                    if let Some(end) = guard.get() {
                        assert!(tag.eq(&end) || end.is_parent_of(tag));
                        self.exhaust.set(true);
                        true
                    } else {
                        self.block_guard.set(Some(guard));
                        false
                    }
                } else {
                    unreachable!("block guard lost;");
                }
            })
    }

    #[inline]
    pub fn outstanding_size(&self) -> usize {
        if self.pushed > self.pulled {
            self.pushed.get() - self.pulled.get()
        } else {
            0
        }
    }

    #[inline]
    pub fn is_skipped(&self) -> bool {
        self.skipped.get()
    }
}

impl fmt::Debug for Panel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "pushed={}, pulled={}, skip={}, exhaust={}",
            self.pushed.get(),
            self.pulled.get(),
            self.skipped.get(),
            self.exhaust.get()
        )
    }
}

pub struct ChannelRxState {
    pub tx_peers: usize,
    pub scope_depth: usize,
    pub index: u32,
    scope_end: CountDownLatchTree,
    scope_data: RefCell<AHashMap<Tag, RcPointer<Panel>>>,
    parent_scope_skipped: RefCell<HashSet<Tag>>,
    notifications: RefCell<TagAntiChainSet>,
    seq_gen: Cell<usize>,
    is_source_exhaust: Cell<bool>,
}

impl ChannelRxState {
    pub fn new(index: u32, tx_peers: usize, scope_depth: usize) -> Self {
        ChannelRxState {
            tx_peers,
            index,
            scope_depth,
            scope_end: CountDownLatchTree::new(tx_peers),
            scope_data: RefCell::new(AHashMap::new()),
            parent_scope_skipped: RefCell::new(HashSet::new()),
            notifications: RefCell::new(TagAntiChainSet::new()),
            seq_gen: Cell::new(0),
            is_source_exhaust: Cell::new(false),
        }
    }

    pub fn pushed(&self, tag: Tag, len: usize) {
        assert_eq!(tag.len(), self.scope_depth);
        let mut scope_data = self.scope_data.borrow_mut();
        if let Some(panel) = scope_data.get(&tag) {
            if !panel.has_outstanding() {
                let seq = self.seq_gen.get() + 1;
                panel.set_seq(seq);
                self.seq_gen.set(seq);
            }
            panel.add_pushed(len);
        } else {
            assert!(len <= std::i64::MAX as usize, "pushed len overflow;");
            let guard = self.notifications.borrow_mut().block_anyway(&tag, len as i64);
            let panel = Panel::default();
            let seq = self.seq_gen.get() + 1;
            panel.set_seq(seq);
            self.seq_gen.set(seq);
            panel.add_pushed(len);
            panel.store_block_guard(guard);
            let p_s = self.parent_scope_skipped.borrow();
            if !p_s.is_empty() {
                let mut p = tag.to_parent_uncheck();
                while !p.is_root() {
                    if p_s.contains(&p) {
                        panel.skip();
                        break;
                    }
                    p = p.to_parent_uncheck();
                }
            }
            scope_data.insert(tag, RcPointer::new(panel));
        }
    }

    pub fn pulled(&self, tag: &Tag, len: usize) {
        assert_eq!(tag.len(), self.scope_depth);
        // trace!("[worker_{:?}] Pulled {} data of {:?} in ch: {}", self.worker_id, len, tag, self.index);
        let scope_data = self.scope_data.borrow();
        if let Some(panel) = scope_data.get(tag) {
            panel.add_pulled(len);
        } else {
            unreachable!("data pulled ahead of event is forbidden;");
        }
    }

    #[inline]
    pub fn get_panel(&self, tag: &Tag) -> Option<RcPointer<Panel>> {
        self.scope_data.borrow().get(tag).map(|p| p.clone())
    }

    pub fn has_outstanding(&self) -> bool {
        self.scope_data.borrow().iter().any(|(_, v)| v.has_outstanding())
    }

    pub fn skip_data_of(&self, tag: &Tag) {
        if tag.len() == self.scope_depth {
            let scope_data = self.scope_data.borrow();
            if let Some(panel) = scope_data.get(tag) {
                panel.skip();
            }
        } else {
            if self.parent_scope_skipped.borrow_mut().insert(tag.clone()) {
                let scope_data = self.scope_data.borrow();
                for (t, panel) in scope_data.iter() {
                    if tag.is_parent_of(t) {
                        panel.skip();
                    }
                }
            }
        }
    }

    pub fn is_scope_skipped(&self, tag: &Tag) -> bool {
        if tag.len() == self.scope_depth {
            self.scope_data.borrow().get(tag).map(|panel| panel.is_skipped()).unwrap_or(false)
        } else {
            let parent_scope_skipped = self.parent_scope_skipped.borrow();
            let mut p = tag.clone();
            while !p.is_root() {
                if parent_scope_skipped.contains(&p) {
                    return true;
                }
                p = p.to_parent_uncheck();
            }
            false
        }
    }

    pub fn give_scope_end_of(&self, tag: Tag, source: u32) -> bool {
        if self.tx_peers == 1 && tag.len() == self.scope_depth {
            if tag.is_root() {
                self.is_source_exhaust.set(true);
            }
            self.notifications.borrow_mut().push(tag);
            true
        } else {
            let mut count_down = self.scope_end.count_down(tag, source);
            let len = count_down.len();
            if len > 0 {
                for notification in count_down.drain(..) {
                    if notification.is_root() {
                        self.is_source_exhaust.set(true);
                        debug_worker!("receive root end notification in ch: {};", self.index);
                    } else {
                        trace_worker!(
                            "receive end notification of {:?} in ch: {};",
                            notification,
                            self.index
                        );
                    }
                    self.notifications.borrow_mut().push(notification);
                }
            }
            len > 0
        }
    }

    #[inline]
    pub fn give_scope_end_all(&self, tag: Tag) {
        self.notifications.borrow_mut().push(tag);
    }

    pub fn for_each_outstanding<F: FnMut(&Tag)>(&self, mut func: F) {
        let scope_data = self.scope_data.borrow();
        for (t, panel) in scope_data.iter() {
            if panel.has_outstanding() {
                func(t);
            }
        }
    }

    #[inline]
    pub fn outstanding_size_of(&self, tag: &Tag) -> Option<usize> {
        self.scope_data.borrow().get(tag).map(|p| p.outstanding_size())
    }

    pub fn is_exhaust(&self) -> bool {
        if self.is_source_exhaust.get() {
            let scope_data = self.scope_data.borrow();
            if !scope_data.is_empty() {
                for (tag, panel) in scope_data.iter() {
                    if panel.has_outstanding() || !panel.is_source_exhaust(tag) {
                        return false;
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn notifications(&self) -> RefMut<Vec<Tag>> {
        if self.notifications.borrow().is_dirty() {
            self.scope_data.borrow_mut().retain(|tag, panel| !panel.is_source_exhaust(tag));
        }

        RefMut::map(self.notifications.borrow_mut(), |n| n.take_fronts())
    }
}
