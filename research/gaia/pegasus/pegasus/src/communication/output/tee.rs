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

use crate::communication::decorator::DataPush;
use crate::data::DataSet;
use crate::data_plane::Push;
use crate::errors::IOResult;
use crate::event::{Event, EventBus, EventKind};
use crate::{Data, Tag};
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};

pub struct ChannelPush<D: Data> {
    pub index: u32,
    pub is_local: bool,
    pub scope_depth: usize,
    push: DataPush<D>,
    skips: HashSet<Tag>,
    parent_skips: HashSet<Tag>,
    skip_st: usize,
}

impl<D: Data> ChannelPush<D> {
    pub fn new(index: u32, is_local: bool, scope_depth: usize, push: DataPush<D>) -> Self {
        ChannelPush {
            index,
            is_local,
            scope_depth,
            push,
            skips: HashSet::new(),
            parent_skips: HashSet::new(),
            skip_st: 0,
        }
    }

    pub fn push_ref(&mut self, msg: &DataSet<D>) -> IOResult<()> {
        if !self.check_skip(msg) {
            self.push.push(msg.clone())?;
        }
        Ok(())
    }

    pub fn push(&mut self, msg: DataSet<D>) -> IOResult<()> {
        if !self.check_skip(&msg) {
            self.push.push(msg)?;
        }
        Ok(())
    }

    #[inline]
    fn check_skip(&mut self, msg: &DataSet<D>) -> bool {
        if !self.skips.is_empty() {
            for tag in &self.skips {
                if tag.is_parent_of(&msg.tag) || tag.eq(&msg.tag) {
                    self.skip_st += msg.len();
                    if crate::worker_id::is_in_trace() {
                        debug_worker!(
                            "cancel output {} data of scope {:?} in ch: {}",
                            msg.len(),
                            msg.tag,
                            self.index
                        );
                    }
                    return true;
                }
            }
            false
        } else if !self.parent_skips.is_empty() {
            for tag in &self.parent_skips {
                if tag.is_parent_of(&msg.tag) || tag.eq(&msg.tag) {
                    self.skip_st += msg.len();
                    if crate::worker_id::is_in_trace() {
                        debug_worker!(
                            "cancel output {} data of scope {:?} in ch: {}",
                            msg.len(),
                            msg.tag,
                            self.index
                        );
                    }
                    return true;
                }
            }
            false
        } else {
            false
        }
    }

    #[inline]
    pub fn skip(&mut self, tag: &Tag) {
        if tag.len() == self.scope_depth {
            self.skips.insert(tag.clone());
        } else if tag.len() < self.scope_depth {
            self.parent_skips.insert(tag.clone());
        }
    }

    #[inline]
    pub fn remove_skip(&mut self, tag: &Tag) {
        if tag.len() == self.scope_depth {
            self.skips.remove(tag);
        } else if tag.len() < self.scope_depth {
            self.parent_skips.remove(tag);
        }
    }

    #[inline]
    pub fn flush(&mut self) -> IOResult<()> {
        self.push.flush()
    }

    #[inline]
    pub fn is_skipped(&self, tag: &Tag) -> bool {
        if tag.len() == self.scope_depth {
            if !self.skips.is_empty() {
                self.skips.contains(tag)
            } else {
                false
            }
        } else {
            if !self.parent_skips.is_empty() {
                true
            } else {
                false
            }
        }
    }

    pub fn close(&mut self) -> IOResult<()> {
        self.skips.clear();
        self.parent_skips.clear();
        self.push.close()?;
        if crate::worker_id::is_in_trace() {
            info_worker!("[ch: {}] => skip push {};", self.index, self.skip_st);
        }
        Ok(())
    }
}

pub struct Tee<D: Data> {
    pushes: SmallVec<[ChannelPush<D>; 2]>,
    index: HashMap<u32, usize>,
    event_bus: EventBus,
}

impl<D: Data> Tee<D> {
    pub fn new(ch: &EventBus) -> Self {
        Tee { pushes: SmallVec::new(), index: HashMap::new(), event_bus: ch.clone() }
    }

    pub fn add_push(&mut self, index: u32, p: ChannelPush<D>) {
        let len = self.pushes.len();
        self.pushes.push(p);
        self.index.insert(index, len);
    }

    pub fn push(&mut self, msg: DataSet<D>) -> IOResult<()> {
        let len = self.pushes.len();
        if len > 0 {
            for i in 1..len {
                self.pushes[i].push_ref(&msg)?;
            }
            self.pushes[0].push(msg)?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.flush()?;
        }
        Ok(())
    }

    pub fn skip(&mut self, ch_index: u32, tag: &Tag) -> bool {
        if let Some(idx) = self.index.get(&ch_index).map(|i| *i) {
            self.pushes[idx].skip(tag);
            self.pushes.iter().all(|p| p.is_skipped(tag))
        } else {
            true
        }
    }

    pub fn notify(&mut self, tag: Tag, kind: EventKind) -> IOResult<()> {
        for ch in self.pushes.iter() {
            let event = Event::new(tag.clone(), ch.index, kind);
            if !ch.is_local {
                self.event_bus.broadcast(event)?;
            } else {
                let worker_id = crate::worker_id::get_current_worker_uncheck();
                self.event_bus.send_to(worker_id, event)?;
            }
        }
        Ok(())
    }

    pub fn give_end(&mut self, tag: Tag) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.remove_skip(&tag);
        }
        self.flush()?;
        let worker_idx = crate::worker_id::get_current_worker_uncheck().index;
        let kind = EventKind::end_of(worker_idx);
        self.notify(tag, kind)
    }

    pub fn give_global_end(&mut self, tag: Tag) -> IOResult<()> {
        for p in self.pushes.iter_mut() {
            p.remove_skip(&tag);
        }
        self.flush()?;
        let kind = EventKind::end_all();
        self.notify(tag, kind)
    }

    #[inline]
    pub fn is_skipped(&self, tag: &Tag) -> bool {
        self.pushes.iter().all(|p| p.is_skipped(tag))
    }

    pub fn close(&mut self) -> IOResult<()> {
        let mut error = vec![];
        for p in self.pushes.iter_mut() {
            if let Err(err) = p.close() {
                error.push(err);
            }
        }
        Ok(())
    }
}
