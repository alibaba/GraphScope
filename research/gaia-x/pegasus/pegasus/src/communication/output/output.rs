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

use crate::communication::output::tee::Tee;
use crate::errors::IOResult;
use crate::graph::Port;
use crate::{Data, Tag};

use crate::communication::decorator::{ScopeStreamBuffer, ScopeStreamPush};
use crate::config::CANCEL_DESC;
use crate::data::DataSet;
use crate::progress::EndSignal;
use crate::tag::tools::map::TidyTagMap;
use std::collections::{HashSet, VecDeque};

pub(crate) struct OutputHandle<D: Data> {
    pub port: Port,
    pub scope_level: usize,
    tee: Tee<D>,
    in_block: TidyTagMap<Box<dyn Iterator<Item = D> + Send + 'static>>,
    blocks: VecDeque<Tag>,
    is_closed: bool,
    skips: HashSet<Tag>,
}

impl<D: Data> OutputHandle<D> {
    pub(crate) fn new(port: Port, scope_level: usize, output: Tee<D>) -> Self {
        OutputHandle {
            port,
            scope_level,
            tee: output,
            in_block: TidyTagMap::new(scope_level),
            blocks: VecDeque::new(),
            is_closed: false,
            skips: HashSet::new(),
        }
    }

    pub fn forward(&mut self, dataset: DataSet<D>) -> IOResult<()> {
        self.tee.forward(dataset)
    }

    pub fn push_entire_iter<I: Iterator<Item = D> + Send + 'static>(
        &mut self, tag: &Tag, mut iter: I,
    ) -> IOResult<()> {
        match self.push_iter(tag, &mut iter) {
            Err(e) => {
                let iter = Box::new(iter);
                if e.is_would_block() || e.is_interrupted() {
                    self.in_block.insert(tag.clone(), iter);
                    self.blocks.push_back(tag.clone());
                }
                Err(e)
            }
            _ => Ok(()),
        }
    }

    pub fn try_unblock(&mut self, unblocked: &mut Vec<Tag>) -> IOResult<()> {
        let len = self.blocks.len();
        if len > 0 {
            for _ in 0..len {
                if let Some(tag) = self.blocks.pop_front() {
                    if let Some(mut iter) = self.in_block.remove(&tag) {
                        match self.push_iter(&tag, &mut iter) {
                            Err(e) => {
                                if e.is_would_block() || e.is_interrupted() {
                                    self.in_block.insert(tag.clone(), iter);
                                    self.blocks.push_back(tag);
                                } else {
                                    return Err(e);
                                }
                            }
                            _ => {
                                trace_worker!("data in scope {:?} unblocked", tag);
                                unblocked.push(tag);
                            }
                        }
                    } else {
                        unblocked.push(tag);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get_blocks(&self) -> &VecDeque<Tag> {
        &self.blocks
    }

    #[inline]
    pub fn skip(&mut self, tag: &Tag) {
        self.skips.insert(tag.clone());
        let len = self.blocks.len();
        for _ in 0..len {
            if let Some(t) = self.blocks.pop_front() {
                if *CANCEL_DESC {
                    if !t.eq(tag) && !tag.is_parent_of(&t) {
                        self.blocks.push_back(t);
                    }
                } else {
                    if !t.eq(tag) {
                        self.blocks.push_back(t);
                    }
                }
            }
        }
        if *CANCEL_DESC && self.scope_level > tag.len() {
            self.in_block
                .retain(|t, _| !tag.is_parent_of(t));
        } else {
            self.in_block.remove(tag);
        }
    }

    #[inline]
    fn is_skipped(&self, tag: &Tag) -> bool {
        if *CANCEL_DESC && self.scope_level > tag.len() {
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

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }
}

impl<D: Data> ScopeStreamBuffer for OutputHandle<D> {
    fn scope_size(&self) -> usize {
        self.tee.scope_size()
    }

    #[inline]
    fn ensure_capacity(&mut self, tag: &Tag) -> IOResult<usize> {
        self.tee.ensure_capacity(tag)
    }

    fn flush_scope(&mut self, tag: &Tag) -> IOResult<()> {
        self.tee.flush_scope(tag)
    }
}

impl<D: Data> ScopeStreamPush<D> for OutputHandle<D> {
    #[inline]
    fn port(&self) -> Port {
        self.port
    }

    #[inline]
    fn push(&mut self, tag: &Tag, msg: D) -> IOResult<()> {
        if !self.is_skipped(tag) {
            self.tee.push(tag, msg)?;
        }
        Ok(())
    }

    #[inline]
    fn push_last(&mut self, msg: D, end: EndSignal) -> IOResult<()> {
        self.tee.push_last(msg, end)
    }

    #[inline]
    fn push_iter<I: Iterator<Item = D>>(&mut self, tag: &Tag, iter: &mut I) -> IOResult<()> {
        if !self.is_skipped(tag) {
            self.tee.push_iter(tag, iter)?;
        }
        Ok(())
    }

    #[inline]
    fn notify_end(&mut self, end: EndSignal) -> IOResult<()> {
        self.tee.notify_end(end)
    }

    #[inline]
    fn flush(&mut self) -> IOResult<()> {
        self.tee.flush()
    }

    fn close(&mut self) -> IOResult<()> {
        if !self.is_closed {
            debug_worker!("close output on port ({:?});", self.port);
            if let Err(err) = self.tee.close() {
                error_worker!("close output on port {:?} failure, caused by: {}", self.port, err);
            }
            self.is_closed = true;
        }
        Ok(())
    }
}

///////////////////////////////////////////////////

