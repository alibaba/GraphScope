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

use crate::communication::input::input::InputBlockGuard;
use crate::communication::input::InputHandle;
use crate::data::DataSet;
use crate::errors::{ErrorKind, IOResult, JobExecError};
use crate::{Data, Tag};
use std::cell::RefMut;

pub struct InputSession<'a, D: Data> {
    scope_level: usize,
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
        self.for_each_batch(|dataset| {
            for item in dataset.drain() {
                func(item)?;
            }
            Ok(())
        })
    }

    pub fn for_each_batch<F>(&mut self, mut func: F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut DataSet<D>) -> Result<(), JobExecError>,
    {
        loop {
            if self.input.is_exhaust() {
                return Ok(());
            } else {
                if let Some(mut data) = self.input.next()? {
                    let is_last = data.is_last();
                    if log_enabled!(log::Level::Trace) {
                        if !data.is_empty() {
                            if is_last {
                                trace_worker!("handle last batch of {:?}, len = {}", data.tag, data.len());
                            } else {
                                trace_worker!("handle batch of {:?}, len = {}", data.tag, data.len());
                            }
                        } else if is_last {
                            trace_worker!("handle end of {:?}", data.tag);
                        }
                    }
                    match func(&mut data) {
                        Ok(_) => {
                            self.on_cancel(&mut data)?;
                            self.on_finish(&mut data);
                            if !is_last {
                                self.for_each_batch_of(&data.tag, &mut func)?;
                            }
                        }
                        Err(mut err) => match &mut err.kind {
                            ErrorKind::WouldBlock(tag) => {
                                self.on_interrupt(data);
                                if let Some(tag) = tag {
                                    if tag.is_root() {
                                        err.kind = ErrorKind::Interrupted;
                                        return Err(err);
                                    }
                                }
                            }
                            ErrorKind::Interrupted => {
                                self.on_interrupt(data);
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
        F: FnMut(&mut DataSet<D>) -> Result<(), JobExecError>,
    {
        while let Some(mut dataset) = self.input.next_of(tag)? {
            let is_last = dataset.is_last();
            if log_enabled!(log::Level::Trace) {
                if !dataset.is_empty() {
                    if is_last {
                        trace_worker!("handle last batch of {:?}, len = {}", dataset.tag, dataset.len());
                    } else {
                        trace_worker!("handle batch of {:?}, len = {}", dataset.tag, dataset.len());
                    }
                } else if is_last {
                    trace_worker!("handle end of {:?}", dataset.tag);
                }
            }
            match (*func)(&mut dataset) {
                Ok(_) => {
                    self.on_cancel(&mut dataset)?;
                    self.on_finish(&mut dataset);
                    if is_last {
                        return Ok(());
                    }
                }
                Err(mut err) => match &err.kind {
                    ErrorKind::WouldBlock(tag) => {
                        self.on_interrupt(dataset);
                        if let Some(tag) = tag {
                            if tag.is_root() {
                                err.kind = ErrorKind::Interrupted;
                                return Err(err);
                            }
                        }
                        return Ok(());
                    }
                    ErrorKind::Interrupted => {
                        self.on_interrupt(dataset);
                        return Err(err);
                    }
                    _ => return Err(err),
                },
            }
        }
        Ok(())
    }

    #[inline]
    fn on_interrupt(&mut self, dataset: DataSet<D>) {
        if !dataset.is_empty() || dataset.is_last() {
            trace_worker!("block pull data of scope {:?}", dataset.tag);
            let b = self.input.stash_block_front(dataset);
            self.block_tmp.push(b);
        }
    }

    #[inline]
    fn on_finish(&mut self, dataset: &mut DataSet<D>) {
        assert!(dataset.is_empty());
        if let Some(end) = dataset.take_end() {
            self.input.end_on(end);
        }
    }

    #[inline]
    fn on_cancel(&mut self, dataset: &mut DataSet<D>) -> Result<(), JobExecError> {
        if dataset.is_discarded() {
            dataset.clear();
            self.cancel_scope(&dataset.tag);
            self.propagate_cancel(&dataset.tag)?;
        }
        Ok(())
    }

    pub fn cancel_scope(&mut self, tag: &Tag) {
        self.input.cancel_scope(tag);
    }

    pub fn propagate_cancel(&mut self, tag: &Tag) -> IOResult<bool> {
        debug_worker!("EARLY-STOP: trigger propagation of early-stop signal of {:?}", tag);
        self.input.propagate_cancel(tag)
    }
}
