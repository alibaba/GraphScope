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

use crate::communication::output::output::OutputHandle;
use crate::data::DataSet;
use crate::errors::{IOError, IOResult, JobExecError};
use crate::{Data, Tag};
use std::cell::{Cell, RefMut};
use std::error::Error;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

pub struct OutputSession<'a, D: Data> {
    pub(crate) tag: Tag,
    output: RefMut<'a, OutputHandle<D>>,
    capacity: Arc<AtomicI64>,
    buffer: Vec<D>,
    allow_interrupt: Cell<bool>,
    is_skipped: bool,
    skip_st: usize,
    trace: bool,
}

impl<'a, D: Data> OutputSession<'a, D> {
    pub fn new(
        capacity: &Arc<AtomicI64>, output: RefMut<'a, OutputHandle<D>>, in_tag: &Tag,
    ) -> Self {
        let tag = output.evolve_output(in_tag);
        let is_skipped = output.is_skipped(&tag);
        OutputSession {
            output,
            capacity: capacity.clone(),
            tag,
            buffer: vec![],
            allow_interrupt: Cell::new(false),
            is_skipped,
            skip_st: 0,
            trace: crate::worker_id::is_in_trace(),
        }
    }

    pub fn advance(&mut self, to: u32) -> IOResult<()> {
        if let Some(cur) = self.tag.current() {
            if cur != to {
                self.flush(false)?;
                self.tag = self.tag.advance_to(to);
            }
        }
        Ok(())
    }

    pub fn give(&mut self, msg: D) -> IOResult<()> {
        if !self.is_skipped {
            self.push(msg)?;
        } else {
            self.skip_st += 1;
        }
        Ok(())
    }

    pub fn give_batch(&mut self, msg: &mut Vec<D>) -> IOResult<()> {
        if !self.is_skipped && !msg.is_empty() {
            let extern_batch = std::mem::replace(msg, vec![]);
            let buffer = std::mem::replace(&mut self.buffer, extern_batch);
            *msg = buffer;
            self.flush(false)?;
        } else {
            self.skip_st += msg.len();
        }
        Ok(())
    }

    pub fn forward(&mut self, data_set: &mut DataSet<D>) -> IOResult<()> {
        if !self.is_skipped {
            let mut data = std::mem::replace(data_set, DataSet::new(self.tag.clone(), vec![]));
            data.tag = self.tag.clone();
            self.output.push_data_set(data)
        } else {
            self.skip_st += data_set.len();
            Ok(())
        }
    }

    pub fn give_iterator<I: Iterator<Item = D>>(&mut self, iter: &mut I) -> IOResult<()> {
        if !self.is_skipped {
            self.allow_interrupt.set(true);
            if let Some(item) = iter.next() {
                if self.buffer.capacity() == 0 {
                    if let Some(buffer) = self.output.fetch_buf() {
                        self.buffer = buffer;
                    } else {
                        return Err(IOError::new(std::io::ErrorKind::Interrupted));
                    }
                }
                self.push(item)?;
                while let Some(item) = iter.next() {
                    self.push(item)?;
                }
            }
        } else {
            if self.trace {
                let mut count = 0;
                for _data in iter {
                    // do nothing;
                    count += 1;
                }
                self.skip_st += count;
            }
        }
        Ok(())
    }

    pub fn give_entire_iter<I: IntoIterator<Item = D>>(&mut self, iter: I) -> IOResult<()> {
        if !self.is_skipped {
            for item in iter {
                self.push(item)?;
            }
        } else {
            if self.trace {
                let mut count = 0;
                for _data in iter {
                    count += 1;
                }
                self.skip_st += count;
            }
        }
        Ok(())
    }

    pub fn give_result_set<I: Iterator<Item = Result<D, Box<dyn Error + Send>>>>(
        &mut self, iter: &mut I,
    ) -> Result<(), JobExecError> {
        if !self.is_skipped {
            self.allow_interrupt.set(true);
            if let Some(resp) = iter.next() {
                let r = resp?;
                if self.buffer.capacity() == 0 {
                    if let Some(buffer) = self.output.fetch_buf() {
                        self.buffer = buffer;
                    } else {
                        return Err(JobExecError::from(IOError::new(
                            std::io::ErrorKind::Interrupted,
                        )));
                    }
                }

                self.push(r)?;
                while let Some(r) = iter.next() {
                    self.push(r?)?;
                }
            }
        } else {
            if self.trace {
                let mut count = 0;
                for d in iter {
                    if let Ok(_) = d {
                        count += 1;
                    }
                }
                self.skip_st += count;
            }
        }
        Ok(())
    }

    pub fn has_capacity(&self) -> bool {
        self.capacity.load(SeqCst) > 0
    }

    pub fn check_interrupt(&self) -> IOResult<()> {
        if self.capacity.load(SeqCst) <= 0 {
            Err(IOError::new(std::io::ErrorKind::Interrupted))
        } else {
            Ok(())
        }
    }

    fn push(&mut self, msg: D) -> IOResult<()> {
        self.buffer.push(msg);
        if self.buffer.len() == self.output.batch_size {
            self.flush(false)?;
        }
        Ok(())
    }

    fn flush(&mut self, before_close: bool) -> IOResult<()> {
        let buffer = self.detach_buffer(before_close);
        if !buffer.is_empty() {
            self.output.push(self.tag.clone(), buffer)?;
            let sub = self.capacity.fetch_sub(1, SeqCst);
            if sub <= 1 && self.allow_interrupt.get() {
                return Err(IOError::new(std::io::ErrorKind::Interrupted));
            }
        }
        Ok(())
    }

    fn detach_buffer(&mut self, before_close: bool) -> Vec<D> {
        if self.buffer.is_empty() {
            vec![]
        } else {
            let len = self.buffer.len();
            if len < 32 && self.buffer.capacity() >= len * 2 {
                let mut detach = Vec::with_capacity(len);
                for item in self.buffer.drain(..) {
                    detach.push(item);
                }
                self.buffer.clear();
                detach
            } else {
                if !before_close {
                    match self.output.fetch_buf() {
                        Some(fetched) => std::mem::replace(&mut self.buffer, fetched),
                        None => {
                            self.capacity.store(1, SeqCst);
                            std::mem::replace(&mut self.buffer, vec![])
                        }
                    }
                } else {
                    std::mem::replace(&mut self.buffer, vec![])
                }
            }
        }
    }
}

impl<'a, D: Data> Drop for OutputSession<'a, D> {
    fn drop(&mut self) {
        self.allow_interrupt.set(false);
        if let Err(err) = self.flush(true) {
            if !err.is_interrupted() {
                // should be unreachable, as interrupt is disabled;
                error_worker!("OutputSession[{:?}] flush error {:?};", self.tag, err);
            }
        }

        if let Err(err) = self.output.flush() {
            error_worker!("OutputProxy[{:?}] flush error {:?};", self.output.port, err);
            pegasus_executor::report_error(JobExecError::from(err));
        }

        if self.buffer.capacity() > 1 {
            let buffer = std::mem::replace(&mut self.buffer, vec![]);
            //debug!("reuse {} buffer for next session;", buffer.capacity());
            self.output.recycle_hook.try_send(buffer).ok();
        }
        if self.skip_st > 0 {
            self.output.add_skip_st(self.skip_st);
        }
    }
}
