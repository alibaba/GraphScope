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
use std::rc::Rc;

use ahash::AHashSet;

use crate::api::scope::MergedScopeDelta;
use crate::channel_id::ChannelInfo;
use crate::communication::cancel::{CancelHandle, CancelListener};
use crate::communication::decorator::{BlockPush, MicroBatchPush};
use crate::communication::IOResult;
use crate::data::MicroBatch;
use crate::data_plane::Push;
use crate::errors::IOError;
use crate::graph::Port;
use crate::progress::DynPeers;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};

struct ChannelCancel {
    scope_level: u32,
    inner: CancelHandle,
    delta: MergedScopeDelta,
    /// used to check cancel before evolve and push;
    before_send: TidyTagMap<()>,
    parent: AHashSet<Tag>,
}

impl CancelListener for ChannelCancel {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        let tag = self.inner.cancel(tag, to)?;
        let level = tag.len() as u32;

        if self.delta.scope_level_delta() < 0 {
            // leave: channel send data to parent scope;
            // cancel from parent scope;
            assert!(level < self.before_send.scope_level);
            self.parent.insert(tag.clone());
            Some(tag.clone())
        } else {
            // scope_level delta >= 0;
            if level == self.scope_level {
                let before_enter = self.delta.evolve_back(&tag);
                self.before_send
                    .insert(before_enter.clone(), ());
                Some(before_enter)
            } else if level < self.scope_level {
                // cancel from parent scope;
                self.parent.insert(tag.clone());
                Some(tag.clone())
            } else {
                warn_worker!(
                    "unexpected cancel of scope {:?} expect scope level {};",
                    tag,
                    self.scope_level
                );
                // ignore:
                None
            }
        }
    }
}

impl ChannelCancel {
    fn is_canceled(&self, tag: &Tag) -> bool {
        let level = tag.len() as u32;
        if level == self.before_send.scope_level {
            if !self.before_send.is_empty() && self.before_send.contains_key(tag) {
                return true;
            }

            if !tag.is_root() && !self.parent.is_empty() {
                let p = tag.to_parent_uncheck();
                self.check_parent(p)
            } else {
                false
            }
        } else if level < self.before_send.scope_level {
            self.check_parent(tag.clone())
        } else {
            false
        }
    }

    fn check_parent(&self, mut p: Tag) -> bool {
        loop {
            if self.parent.contains(&p) {
                return true;
            }
            if p.is_root() {
                break;
            } else {
                p = p.to_parent_uncheck();
            }
        }

        false
    }
}

#[derive(Clone)]
pub(crate) struct ChannelCancelPtr {
    inner: Rc<RefCell<ChannelCancel>>,
}

impl CancelListener for ChannelCancelPtr {
    fn cancel(&mut self, tag: &Tag, to: u32) -> Option<Tag> {
        self.inner.borrow_mut().cancel(tag, to)
    }
}

impl ChannelCancelPtr {
    fn new(scope_level: u32, delta: MergedScopeDelta, ch: CancelHandle) -> Self {
        let level_before = delta.origin_scope_level as u32;
        let inner = ChannelCancel {
            scope_level,
            inner: ch,
            delta,
            before_send: TidyTagMap::new(level_before),
            parent: AHashSet::new(),
        };
        ChannelCancelPtr { inner: Rc::new(RefCell::new(inner)) }
    }

    fn is_canceled(&self, tag: &Tag) -> bool {
        self.inner.borrow().is_canceled(tag)
    }
}

unsafe impl Send for ChannelCancelPtr {}

#[allow(dead_code)]
pub(crate) struct PerChannelPush<D: Data> {
    pub ch_info: ChannelInfo,
    pub src: u32,
    pub(crate) delta: MergedScopeDelta,
    push: MicroBatchPush<D>,
    cancel_handle: ChannelCancelPtr,
    re_seq: TidyTagMap<u64>,
}

impl<D: Data> PerChannelPush<D> {
    pub(crate) fn new(
        ch_info: ChannelInfo, delta: MergedScopeDelta, push: MicroBatchPush<D>, ch: CancelHandle,
    ) -> Self {
        let src = crate::worker_id::get_current_worker().index;
        let cancel_handle = ChannelCancelPtr::new(ch_info.scope_level, delta.clone(), ch);
        let re_seq = TidyTagMap::new(ch_info.scope_level);
        PerChannelPush { ch_info, src, delta, push, cancel_handle, re_seq }
    }

    pub(crate) fn get_cancel_handle(&self) -> ChannelCancelPtr {
        self.cancel_handle.clone()
    }

    #[inline]
    fn is_canceled(&self, tag: &Tag) -> bool {
        self.cancel_handle.is_canceled(tag)
    }
}

impl<D: Data> Push<MicroBatch<D>> for PerChannelPush<D> {
    fn push(&mut self, mut batch: MicroBatch<D>) -> Result<(), IOError> {
        if self.is_canceled(&batch.tag) {
            if batch.is_last() {
                batch.clear();
            } else {
                return Ok(());
            }
        }

        if batch.tag.len() == self.delta.origin_scope_level {
            let tag = self.delta.evolve(&batch.tag);
            if let Some(end) = batch.take_end() {
                if self.delta.scope_level_delta() > 0 {
                    // enter / to-child
                    let mut end_cp = end.clone();
                    batch.set_end(end);
                    batch.set_tag(tag);
                    trace_worker!("channel[{}] pushed end of scope{:?};", self.ch_info.id.index, batch.tag);
                    self.push.push(batch)?;
                    end_cp.update_peers(DynPeers::all());
                    let last = MicroBatch::last(self.src, end_cp);
                    trace_worker!("channel[{}] pushed end of scope{:?};", self.ch_info.id.index, last.tag);
                    self.push.push(last)
                } else if self.delta.scope_level_delta() == 0 {
                    batch.set_end(end);
                    batch.set_tag(tag);
                    trace_worker!("channel[{}] pushed end of scope{:?};", self.ch_info.id.index, batch.tag);
                    self.push.push(batch)
                } else {
                    // leave / to parent
                    if !batch.is_empty() {
                        let seq = self.re_seq.get_mut_or_insert(&tag);
                        batch.set_tag(tag);
                        batch.set_seq(*seq);
                        *seq += 1;
                        self.push.push(batch)
                    } else {
                        Ok(())
                    }
                }
            } else if !batch.is_empty() {
                // is not end, is not empty;
                if self.delta.scope_level_delta() < 0 {
                    let seq = self.re_seq.get_mut_or_insert(&tag);
                    batch.set_seq(*seq);
                    *seq += 1;
                }
                batch.set_tag(tag);
                self.push.push(batch)
            } else {
                //is not end, and is emtpy, ignore;
                Ok(())
            }
        } else if batch.tag.len() < self.delta.origin_scope_level {
            // batch from parent scope;
            assert!(batch.is_empty(), "batch from parent is not empty;");
            assert!(batch.is_last(), "batch from parent is not last;");
            if batch.tag.len() as u32 == self.ch_info.scope_level {
                let seq = self.re_seq.remove(&batch.tag).unwrap_or(0);
                batch.set_seq(seq);
            }
            trace_worker!("channel[{}] pushed end of scope{:?};", self.ch_info.id.index, batch.tag);
            self.push.push(batch)
        } else {
            unreachable!("unrecognized batch from child scope {:?}", batch.tag);
        }
    }

    fn flush(&mut self) -> Result<(), IOError> {
        trace_worker!("output[{:?}] flush channel [{}]", self.ch_info.source_port, self.ch_info.index());
        self.push.flush()
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.push.close()
    }
}

impl<D: Data> BlockPush for PerChannelPush<D> {
    fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError> {
        self.push.try_unblock(tag)
    }

    fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()> {
        self.push.clean_block_of(tag)
    }
}

#[allow(dead_code)]
pub(crate) struct Tee<D: Data> {
    port: Port,
    scope_level: u32,
    main_push: PerChannelPush<D>,
    other_pushes: Vec<PerChannelPush<D>>,
}

impl<D: Data> Tee<D> {
    pub fn new(port: Port, scope_level: u32, push: PerChannelPush<D>) -> Self {
        Tee { port, scope_level, main_push: push, other_pushes: Vec::new() }
    }

    pub fn add_push(&mut self, push: PerChannelPush<D>) {
        self.other_pushes.push(push);
    }
}

impl<D: Data> Push<MicroBatch<D>> for Tee<D> {
    fn push(&mut self, mut msg: MicroBatch<D>) -> Result<(), IOError> {
        let mut would_block = false;
        if !self.other_pushes.is_empty() {
            for tx in self.other_pushes.iter_mut() {
                let msg_cp = msg.share();
                if let Err(err) = tx.push(msg_cp) {
                    if err.is_would_block() {
                        trace_worker!(
                            "tee[{:?}] other push blocked on push batch of {:?} ;",
                            self.port,
                            msg.tag
                        );
                        would_block = true;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        match self.main_push.push(msg) {
            Ok(_) => {
                if would_block {
                    would_block!("underlying channel push blocked")
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                if err.is_would_block() {
                    trace_worker!("tee[{:?}] main push blocked on push batch;", self.port,);
                }
                Err(err)
            }
        }
    }

    fn flush(&mut self) -> Result<(), IOError> {
        self.main_push.flush()?;
        for o in self.other_pushes.iter_mut() {
            o.flush()?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), IOError> {
        self.main_push.close()?;
        for o in self.other_pushes.iter_mut() {
            o.close()?;
        }
        Ok(())
    }
}

impl<D: Data> BlockPush for Tee<D> {
    fn try_unblock(&mut self, tag: &Tag) -> Result<bool, IOError> {
        let mut would_block = self.main_push.try_unblock(tag)?;
        for o in self.other_pushes.iter_mut() {
            would_block |= o.try_unblock(tag)?;
        }
        Ok(would_block)
    }

    fn clean_block_of(&mut self, tag: &Tag) -> IOResult<()> {
        self.main_push.clean_block_of(tag)?;
        for o in self.other_pushes.iter_mut() {
            o.clean_block_of(tag)?;
        }
        Ok(())
    }
}
