//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use super::*;
use std::rc::Rc;
use std::cell::RefCell;
use crate::channel::eventio::EventsBuffer;
use crate::channel::tee::{WrappedTee, Tee};
use crate::common::Port;

/// Describing how dataset tags will be changed when output from an output port.
///
/// # Please note!
/// Since we've just a few built-in operators manipulating dataset tag, for simplicity
/// reasons, in our system, `OutputDelta` is defined on per output perspective and
/// against all inputs.
/// For example, to generate output dataset, if a binary operator advance dataset tag
/// of one input, it will and must to the same to the other input.
#[derive(Clone, Copy, Debug)]
pub enum OutputDelta {
    /// Dataset tag won't be changed.
    None,
    /// Advance the current counter of tag, usually the loop body output
    /// of LoopController.
    Advance,
    /// Add a new dimension for tag, usually the EnterScope operator.
    ToChild,
    /// Remove current dimension of tag, usually the LeaveScope operator.
    ToParent,
}

impl OutputDelta {
    pub fn matcher_of(&self, tag: &Tag) -> TagMatcher {
        match self {
            OutputDelta::None => TagMatcher::Equals(tag.clone()),
            OutputDelta::Advance => TagMatcher::Equals(tag.retreat()),
            OutputDelta::ToChild => TagMatcher::Equals(tag.to_parent()),
            OutputDelta::ToParent => TagMatcher::Prefix(tag.clone()),
        }
    }
}

pub struct OutputBuilder<D> {
    pub batch_size : usize,
    pub worker: WorkerId,
    pub port: Port,
    shared: Rc<RefCell<Vec<(Box<dyn Push<DataSet<D>>>, ChannelId, bool)>>>,
    events_buf: EventsBuffer
}

impl<D: Data> OutputBuilder<D> {
    pub fn new(batch: usize, worker: WorkerId, port: Port, events_buf: &EventsBuffer) -> Self {
        OutputBuilder {
            batch_size: batch,
            worker,
            port,
            shared: Rc::new(RefCell::new(Vec::new())),
            events_buf: events_buf.clone()
        }
    }

    pub fn add_push<P>(&self, ch_id: ChannelId, local: bool, push: P) where P: Push<DataSet<D>> + 'static {
        self.shared.borrow_mut().push((Box::new(push), ch_id, local));
    }

    pub fn build_tee(self) -> WrappedTee<DataSet<D>> {
        let mut pushes = Vec::new();
        let mut ch_ids = Vec::new();
        {
            let mut shared = self.shared.borrow_mut();
            for (p, c, l) in shared.drain(..) {
                pushes.push(p);
                ch_ids.push((c, l));
            }
        }

        let tee = Tee::<DataSet<D>>::from(pushes);
        WrappedTee::new(self.worker, tee, ch_ids, &self.events_buf)
    }
}

impl<D: Data> Clone for OutputBuilder<D> {
    fn clone(&self) -> Self {
        OutputBuilder {
            batch_size: self.batch_size,
            worker: self.worker,
            port: self.port,
            shared: self.shared.clone(),
            events_buf: self.events_buf.clone()
        }
    }
}

pub trait TaggedOutput: AsAny + Send {

    fn set_output_capacity(&mut self, capacity: usize);

    fn has_capacity(&self) -> bool;

    fn clear_capacity(&mut self);

    fn transmit_end(&mut self, tag: Tag) -> IOResult<()>;

    fn delta(&self) -> &OutputDelta;

    fn close(&mut self) -> IOResult<()>;

    fn is_closed(&self) -> bool;
}

pub trait TaggedOutputBuilder {
    fn build_output(self: Box<Self>, delta: OutputDelta) -> Box<dyn TaggedOutput>;
}

pub struct OutputHandle<D: Data> {
    pub port: Port,
    pub delta: OutputDelta,
    inner: WrappedTee<DataSet<D>>,
    capacity: Option<usize>,
    batch_size: usize,
    poisoned: bool
}

impl<D: Data> AsAny for OutputHandle<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D: Data> TaggedOutput for OutputHandle<D> {
    #[inline]
    fn set_output_capacity(&mut self, capacity: usize) {
        self.capacity.replace(capacity);
    }

    #[inline]
    fn has_capacity(&self) -> bool {
        if let Some(left) = self.capacity.as_ref() {
            *left > 0
        } else {
            true
        }
    }

    #[inline]
    fn clear_capacity(&mut self) {
        self.capacity = None;
    }

    #[inline]
    fn transmit_end(&mut self, tag: Tag) -> IOResult<()> {
        //let matched = self.match_output(&tag).unwrap_or(tag);
        self.inner.transmit_end(tag)
    }

    #[inline]
    fn delta(&self) -> &OutputDelta {
        &self.delta
    }

    #[inline]
    fn close(&mut self) -> IOResult<()> {
        if !self.poisoned {
            trace!("Worker[{}], output[{:?}] is closing ...", self.inner.worker, self.port);
            self.poisoned = true;
            self.inner.close()?;
        }
        Ok(())
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.poisoned
    }
}

impl<D: Data> OutputHandle<D> {
    pub fn new(output: WrappedTee<DataSet<D>>, batch: usize, port: Port, delta: OutputDelta) -> Self {
        OutputHandle {
            port,
            delta,
            inner: output,
            capacity: None,
            batch_size: batch,
            poisoned: false
        }
    }

    pub fn downcast(origin: &mut Box<dyn TaggedOutput>) -> &mut Self {
        // TODO: handle downcast failure
        origin.as_any_mut().downcast_mut::<Self>().expect("Downcast to OutputHandle failure")
    }

    #[inline]
    fn match_output(&self, tag: &Tag) -> Option<Tag> {
        match self.delta {
            OutputDelta::None => None,
            OutputDelta::Advance => Some(tag.advance()),
            OutputDelta::ToParent => Some(tag.to_parent()),
            OutputDelta::ToChild => Some(Tag::from(tag, 0))
        }
    }
}

impl<D: Data> TaggedOutputBuilder for OutputBuilder<D> {

    fn build_output(self: Box<Self>, delta: OutputDelta) -> Box<dyn TaggedOutput> {
        let batch_size = self.batch_size;
        let port = self.port;
        let tee = self.build_tee();
        let output = OutputHandle::new(tee, batch_size, port, delta);
        Box::new(output) as Box<dyn TaggedOutput>
    }
}

pub struct Session<'a, D: Data> {
    output: &'a mut WrappedTee<DataSet<D>>,
    capacity: Option<&'a mut usize>,
    batch_size: usize,
    tag: Tag,
    buffer: Vec<D>
}

impl<'a, D: Data> Session<'a, D> {
    pub fn new(output: &'a mut WrappedTee<DataSet<D>>, tag: Tag, batch: usize, capacity: Option<&'a mut usize>) -> Self {
        Session {
            output,
            capacity,
            batch_size: batch,
            tag,
            buffer: Vec::with_capacity(batch),
        }
    }

    /// Output one message, if success, return true or false represent whether output capacity is available;
    pub fn give(&mut self, msg: D) -> IOResult<bool> {
        self.push(msg)?;
        Ok(self.update_capacity(1))
    }

    pub fn give_iterator<I: Iterator<Item = D>>(&mut self, iter: &mut I) -> IOResult<bool> {
        if let Some(capacity) = self.capacity.as_ref().map(|c| **c) {
            let mut count = 0;
            while count < capacity {
                if let Some(item) = iter.next() {
                    self.push(item)?;
                } else {
                    break
                }
                count += 1;
            }
            Ok(self.update_capacity(count))
        } else {
            for item in iter {
                self.push(item)?;
            }
            Ok(true)
        }
    }

    pub fn give_entire_iterator<I: IntoIterator<Item = D>>(&mut self, iter: I) -> IOResult<bool> {
        let mut count = 0;
        for datum in iter.into_iter() {
            count += 1;
            self.push(datum)?;
        }
        Ok(self.update_capacity(count))
    }

    ///
    pub fn give_batch(&mut self, batch: Vec<D>) -> IOResult<bool> {
        self.flush()?;
        let size = batch.len();
        self.output.push(DataSet::new(self.tag.clone(), batch))?;
        self.output.flush()?;
        Ok(self.update_capacity(size))
    }

    #[inline]
    pub fn transmit_end(mut self) -> IOResult<()> {
        self.flush()?;
        self.output.transmit_end(self.tag.clone())?;
        Ok(())
    }

    #[inline]
    pub fn has_capacity(&self) -> bool {
        self.check_capacity()
    }

    pub fn flush(&mut self) -> IOResult<()> {
        if !self.buffer.is_empty() {
            let size = self.buffer.len();
            let msgs = ::std::mem::replace(&mut self.buffer,
                                           Vec::with_capacity(size));
            self.output.push(DataSet::new(self.tag.clone(), msgs))?;
            self.output.flush()?;
        }
        Ok(())
    }

    #[inline]
    fn push(&mut self, msg: D) -> IOResult<()> {
        self.buffer.push(msg);
        if self.buffer.len() == self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    /// update the capacity of output channel's left space;
    #[inline]
    fn update_capacity(&mut self, decr: usize) -> bool {
        if let Some(ref mut ca) = self.capacity {
            if **ca <= decr {
                **ca = 0;
                false
            } else {
                **ca -= decr;
                true
            }
        } else {
            true
        }
    }

    /// Return `true` if there is capacity left;
    #[inline]
    fn check_capacity(&self) -> bool {
        self.capacity.as_ref().map(|ca| **ca > 0).unwrap_or(true)
    }
}

impl<'a, D: Data> Drop for Session<'a, D> {
    fn drop(&mut self) {
        match self.flush() {
            Ok(_) => (),
            Err(e) => {
                error!("Session flush failed, caused by {:?}", e);
            }
        }
    }
}

impl<D: Data> OutputHandle<D> {
    #[inline]
    pub fn session(&mut self, tag: &Tag) -> Session<D> {
        let matched = self.match_output(tag).unwrap_or(tag.clone());
        let ca = self.capacity.as_mut();
        Session::new(&mut self.inner, matched, self.batch_size, ca)
    }

    #[inline]
    pub fn session_of<T: Into<Tag>>(&mut self, tag: T) -> Session<D> {
        let t = tag.into();
        let matched = self.match_output(&t).unwrap_or(t);
        let ca = self.capacity.as_mut();
        Session::new(&mut self.inner, matched, self.batch_size, ca)
    }
}






