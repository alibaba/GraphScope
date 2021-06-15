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
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use crate::communication::output::tee::Tee;
use crate::communication::output::{OutputDelta, OutputProxy};
use crate::data::DataSet;
use crate::errors::IOResult;
use crate::event::EventKind;
use crate::graph::Port;
use crate::tag::tools::{BlockGuard, TagAntiChainSet};
use crate::{Data, Tag};

use crossbeam_channel::{Receiver, Sender};
use pegasus_common::downcast::*;

pub struct OutputHandle<D: Data> {
    pub port: Port,
    pub delta: OutputDelta,
    pub batch_size: usize,
    pub capacity: u32,
    pub scope_depth: usize,
    pub mem_limit: Option<usize>,
    pub recycle_hook: Sender<Vec<D>>,
    tee: Tee<D>,
    end_scopes: TagAntiChainSet,
    blocked: HashMap<Tag, BlockGuard>,
    recycle_bin: Receiver<Vec<D>>,
    poisoned: bool,
    global_scope_ends: HashSet<Tag>,

    reuse_st: (usize, usize),
    skip_st: usize,
}

impl<D: Data> OutputHandle<D> {
    pub fn new(
        port: Port, batch_size: usize, capacity: u32, delta: OutputDelta, scope_depth: usize,
        output: Tee<D>,
    ) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(capacity as usize);
        OutputHandle {
            port,
            delta,
            batch_size,
            capacity,
            scope_depth,
            mem_limit: None,
            tee: output,
            end_scopes: TagAntiChainSet::new(),
            blocked: HashMap::new(),
            recycle_bin: rx,
            recycle_hook: tx,
            poisoned: false,
            global_scope_ends: HashSet::new(),
            reuse_st: (0, 0),
            skip_st: 0,
        }
    }

    pub fn set_job_mem_limit(&mut self, limit_in_bytes: usize) {
        assert!(limit_in_bytes > 0);
        self.mem_limit.replace(limit_in_bytes);
    }

    pub fn push(&mut self, tag: Tag, buf: Vec<D>) -> IOResult<()> {
        let data = DataSet::with_hook(tag, buf, &self.recycle_hook);
        self.tee.push(data)
    }

    #[inline]
    pub fn push_data_set(&mut self, data_set: DataSet<D>) -> IOResult<()> {
        self.tee.push(data_set)
    }

    #[inline]
    pub fn flush(&mut self) -> IOResult<()> {
        self.tee.flush()
    }

    #[inline]
    pub fn scope_end(&mut self, tag: Tag) {
        // if tag.is_root() {
        //     debug_worker!("get eos on output port {:?}", self.port);
        // }
        self.end_scopes.push(tag);
    }

    pub fn global_scope_end(&mut self, tag: Tag) {
        self.end_scopes.push(tag.clone());
        self.global_scope_ends.insert(tag);
    }

    #[inline]
    pub fn retain(&mut self, tag: &Tag) {
        //self.end_scopes.block_always(tag)
        let g = self.end_scopes.block_anyway(tag, 1);
        self.blocked.insert(tag.clone(), g);
    }

    #[inline]
    pub fn drop_retain(&mut self, tag: &Tag) {
        if let Some(g) = self.blocked.get(tag) {
            g.decr(1);
        }
    }

    pub fn ignore(&mut self, tag: &Tag) {
        self.end_scopes.remove(tag);
    }

    pub fn close(&mut self) -> IOResult<()> {
        if !self.poisoned {
            self.flush()?;
            for (_, g) in self.blocked.drain() {
                g.clear();
            }
            self.close_scopes()?;
            if crate::worker_id::is_in_trace() {
                info_worker!(
                    "output({:?}) => reuse st : {:?}, skip push : {};",
                    self.port,
                    self.reuse_st,
                    self.skip_st
                );
            }
            debug_worker!("close output on port ({:?});", self.port);
            self.tee.close().ok();
            self.poisoned = true;
        }
        Ok(())
    }

    pub fn close_scopes(&mut self) -> IOResult<()> {
        if !self.poisoned {
            let mut ends = vec![];
            match self.delta {
                OutputDelta::None | OutputDelta::ToChild => {
                    for end in self.end_scopes.take_fronts().drain(..) {
                        //fold.add_node(end);
                        ends.push(end);
                    }
                }
                OutputDelta::Advance => {
                    for mut end in self.end_scopes.take_fronts().drain(..) {
                        if end.len() == self.scope_depth {
                            end = end.advance();
                        }
                        // trace!("[worker_{:?}] close scope {:?} on port {:?}", self.tee.worker, end, self.port);
                        ends.push(end);
                    }
                }
                OutputDelta::ToParent(n) => {
                    let n = n as usize;
                    assert!(self.scope_depth > n);
                    for end in self.end_scopes.take_fronts().drain(..) {
                        if end.len() <= n {
                            // trace!("[worker_{:?}] close scope {:?} on port {:?}", self.tee.worker, end, self.port);
                            ends.push(end);
                        }
                    }
                }
            }
            for e in ends {
                if e.len() >= self.scope_depth && self.global_scope_ends.remove(&e) {
                    self.tee.give_global_end(e)?;
                } else {
                    if e.is_root() {
                        debug_worker!("close root scope on port {:?}", self.port);
                    }
                    self.tee.give_end(e)?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.poisoned
    }

    #[inline]
    pub fn notify(&mut self, tag: Tag, kind: EventKind) -> IOResult<()> {
        self.tee.notify(tag, kind)
    }

    #[inline]
    pub fn evolve_output(&self, tag: &Tag) -> Tag {
        match self.delta {
            OutputDelta::None => tag.clone(),
            OutputDelta::Advance => tag.advance(),
            OutputDelta::ToParent(n) => {
                let mut tag = tag.clone();
                let n = n as usize;
                while tag.len() > n {
                    tag = tag.to_parent_uncheck();
                }
                tag
            }
            OutputDelta::ToChild => Tag::inherit(tag, 0),
        }
    }

    #[inline]
    pub fn skip(&mut self, ch_index: u32, tag: &Tag) -> bool {
        self.tee.skip(ch_index, tag)
    }

    #[inline]
    pub fn is_skipped(&self, tag: &Tag) -> bool {
        self.tee.is_skipped(tag)
    }

    #[inline]
    pub(crate) fn fetch_buf(&mut self) -> Option<Vec<D>> {
        match self.recycle_bin.try_recv() {
            Ok(buf) => {
                self.reuse_st.0 += 1;
                Some(buf)
            }
            Err(_) => {
                if let Some(limit) = self.mem_limit {
                    if let Some(used) = pegasus_memory::alloc::check_current_task_memory() {
                        if used >= limit {
                            return None;
                        }
                    }
                }
                self.reuse_st.1 += 1;
                Some(Vec::with_capacity(self.batch_size))
            }
        }
    }

    #[inline]
    pub(crate) fn add_skip_st(&mut self, len: usize) {
        self.skip_st += len;
    }
}

pub struct RefWrapOutput<D: Data> {
    pub(crate) output: RefCell<OutputHandle<D>>,
    pub(super) capacity: Arc<AtomicI64>,
    output_delta: OutputDelta,
}

impl<D: Data> AsAny for RefWrapOutput<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D: Data> OutputProxy for RefWrapOutput<D> {
    #[inline]
    fn output_delta(&self) -> OutputDelta {
        self.output_delta
    }

    #[inline]
    fn has_capacity(&self) -> bool {
        self.capacity.load(SeqCst) > 0
    }

    #[inline]
    fn reset_capacity(&self) {
        let ca = self.output.borrow().capacity as i64;
        self.capacity.store(ca, SeqCst);
    }

    #[inline]
    fn batch_size(&self) -> usize {
        self.output.borrow().batch_size
    }

    #[inline]
    fn scope_end(&self, tag: Tag) {
        self.output.borrow_mut().scope_end(tag)
    }

    #[inline]
    fn global_scope_end(&self, tag: Tag) {
        self.output.borrow_mut().global_scope_end(tag)
    }

    #[inline]
    fn retain(&self, tag: &Tag) {
        self.output.borrow_mut().retain(tag)
    }

    #[inline]
    fn drop_retain(&self, tag: &Tag) {
        self.output.borrow_mut().drop_retain(tag)
    }

    fn ignore(&self, tag: &Tag) {
        self.output.borrow_mut().ignore(tag);
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.output.borrow().is_closed()
    }

    #[inline]
    fn skip(&self, ch_index: u32, tag: &Tag) -> bool {
        self.output.borrow_mut().skip(ch_index, tag)
    }

    #[inline]
    fn close_scopes(&self) -> IOResult<()> {
        self.output.borrow_mut().close_scopes()
    }

    #[inline]
    fn close(&self) -> IOResult<()> {
        self.output.borrow_mut().close()
    }
}

impl<D: Data> RefWrapOutput<D> {
    pub fn wrap(output: OutputHandle<D>) -> Self {
        let ca = output.capacity as i64;
        RefWrapOutput {
            output_delta: output.delta,
            output: RefCell::new(output),
            capacity: Arc::new(AtomicI64::new(ca)),
        }
    }

    #[inline]
    pub fn downcast(generic: &Box<dyn OutputProxy>) -> &Self {
        generic.as_any_ref().downcast_ref::<Self>().expect("downcast failure")
    }
}

impl<D: Data> Deref for RefWrapOutput<D> {
    type Target = RefCell<OutputHandle<D>>;

    fn deref(&self) -> &Self::Target {
        &self.output
    }
}
