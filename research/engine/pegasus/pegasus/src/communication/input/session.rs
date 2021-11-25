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

use std::cell::RefMut;

use crate::communication::input::input::InputBlockGuard;
use crate::communication::input::InputHandle;
use crate::data::MicroBatch;
use crate::errors::{ErrorKind, JobExecError};
use crate::{Data, Tag};

pub struct InputSession<'a, D: Data> {
    scope_level: u32,
    input: RefMut<'a, InputHandle<D>>,
    block_tmp: Vec<InputBlockGuard>,
}

impl<'a, D: Data> InputSession<'a, D> {
    pub fn new(input: RefMut<'a, InputHandle<D>>) -> Self {
        InputSession { scope_level: input.ch_info.scope_level, input, block_tmp: Vec::new() }
    }

    pub fn for_each<F>(&mut self, mut func: F) -> Result<(), JobExecError>
    where
        F: FnMut(D) -> Result<(), JobExecError>,
    {
        self.for_each_batch(|batch| {
            for item in batch.drain() {
                func(item)?;
            }
            Ok(())
        })
    }

    pub fn for_each_batch<F>(&mut self, mut func: F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut MicroBatch<D>) -> Result<(), JobExecError>,
    {
        loop {
            if self.input.is_exhaust() {
                return Ok(());
            } else {
                if let Some(mut batch) = self.input.next()? {
                    let is_last = batch.is_last();
                    if log_enabled!(log::Level::Trace) {
                        if !batch.is_empty() {
                            if is_last {
                                trace_worker!(
                                    "handle last batch of {:?}, len = {}",
                                    batch.tag,
                                    batch.len()
                                );
                            } else {
                                trace_worker!("handle batch of {:?}, len = {}", batch.tag, batch.len());
                            }
                        } else if is_last {
                            trace_worker!("handle end of {:?}", batch.tag);
                        }
                    }
                    match func(&mut batch) {
                        Ok(_) => {
                            if self.on_consumed(is_last, &mut batch) {
                                self.for_each_batch_of(&batch.tag, &mut func)?;
                            }
                        }
                        Err(mut err) => match &mut err.kind {
                            ErrorKind::WouldBlock(_) => {
                                if batch.tag.is_root() {
                                    self.on_interrupt(batch);
                                    return Err(err);
                                } else {
                                    self.on_interrupt(batch);
                                }
                            }
                            ErrorKind::Interrupted => {
                                self.on_interrupt(batch);
                                return Err(err);
                            }
                            _ => return Err(err),
                        },
                    }
                    if self.scope_level == 0 {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
        }
    }

    pub(crate) fn for_each_batch_of<F>(&mut self, tag: &Tag, func: &mut F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut MicroBatch<D>) -> Result<(), JobExecError>,
    {
        while let Some(mut batch) = self.input.next_of(tag)? {
            let is_last = batch.is_last();
            if log_enabled!(log::Level::Trace) {
                if !batch.is_empty() {
                    if is_last {
                        trace_worker!("handle last batch of {:?}, len = {}", batch.tag, batch.len());
                    } else {
                        trace_worker!("handle batch of {:?}, len = {}", batch.tag, batch.len());
                    }
                } else if is_last {
                    trace_worker!("handle end of {:?}", batch.tag);
                }
            }
            match (*func)(&mut batch) {
                Ok(_) => {
                    if !self.on_consumed(is_last, &mut batch) {
                        return Ok(());
                    }
                }
                Err(err) => match &err.kind {
                    ErrorKind::WouldBlock(_) => {
                        if batch.tag.is_root() {
                            self.on_interrupt(batch);
                            return Err(err);
                        } else {
                            self.on_interrupt(batch);
                            return Ok(());
                        }
                    }
                    ErrorKind::Interrupted => {
                        self.on_interrupt(batch);
                        return Err(err);
                    }
                    _ => return Err(err),
                },
            }
        }
        Ok(())
    }

    #[inline]
    fn on_interrupt(&mut self, batch: MicroBatch<D>) {
        trace_worker!("block pull data of scope {:?}", batch.tag);
        if !batch.is_empty() || batch.is_last() {
            let b = self.input.stash_block_front(batch);
            self.block_tmp.push(b);
        } else {
            // batch is empty and is not last;
            let b = self.input.block(&batch.tag);
            self.block_tmp.push(b);
        }
    }

    #[inline]
    fn on_consumed(&mut self, is_last: bool, batch: &mut MicroBatch<D>) -> bool {
        if batch.is_discarded() {
            batch.take_data();
            if is_last {
                if let Some(end) = batch.take_end() {
                    self.input.end_on(end);
                }
            } else {
                self.input.cancel_scope(&batch.tag);
            }
            false
        } else {
            if !batch.is_empty() {
                warn_worker!(
                    "ch[{:?}]: {} data in batch of {:?} not consumed",
                    self.input.ch_info.id,
                    batch.len(),
                    batch.tag
                );
                batch.clear();
            }
            if let Some(end) = batch.take_end() {
                self.input.end_on(end);
            }
            !is_last
        }
    }
}
