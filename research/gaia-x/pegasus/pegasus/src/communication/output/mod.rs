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

use crate::errors::IOResult;
use crate::{Data, Tag};
use pegasus_common::downcast::AsAny;
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::VecDeque;

pub trait OutputProxy: AsAny + Send {
    fn flush(&self) -> IOResult<()>;

    fn get_blocks(&self) -> Ref<VecDeque<Tag>>;

    fn try_unblock(&self, unblocked: &mut Vec<Tag>) -> IOResult<()>;

    /// Notify this output that the scope with tag was closed, no more data of this scope will be send
    /// on this output;
    fn notify_end(&self, end: EndSignal) -> IOResult<()>;

    /// Stop to output data of scope with this tag in output port from now on;
    fn skip(&self, tag: &Tag);

    /// Close all output ports, all the channels connected on this output would be notified;
    fn close(&self) -> IOResult<()>;

    /// Check if this output has been closed;
    fn is_closed(&self) -> bool;
}

pub trait OutputBuilder: AsAny {
    fn build(self: Box<Self>) -> Option<Box<dyn OutputProxy>>;
}

mod builder;
mod output;
mod tee;
use crate::communication::decorator::ScopeStreamPush;
use crate::communication::output::output::OutputHandle;
use crate::data::MicroBatch;
use crate::progress::EndSignal;
pub(crate) use builder::OutputBuilderImpl;
pub use output::OutputSession;
pub(crate) use tee::ChannelPush;

pub struct RefWrapOutput<D: Data> {
    pub(crate) output: RefCell<OutputHandle<D>>,
}

impl<D: Data> AsAny for RefWrapOutput<D> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

impl<D: Data> RefWrapOutput<D> {
    pub(crate) fn wrap(output: OutputHandle<D>) -> Self {
        RefWrapOutput { output: RefCell::new(output) }
    }

    #[inline]
    pub(crate) fn downcast(generic: &Box<dyn OutputProxy>) -> &Self {
        generic
            .as_any_ref()
            .downcast_ref::<Self>()
            .expect("downcast failure")
    }

    pub fn new_session(&self, tag: &Tag) -> IOResult<OutputSession<D>> {
        let output = self.output.borrow_mut();
        Ok(OutputSession::new(tag.clone(), output))
    }

    pub fn push_batch(&self, dataset: MicroBatch<D>) -> IOResult<()> {
        self.output.borrow_mut().push_batch(dataset)
    }

    pub fn push_batch_mut(&self, batch: &mut MicroBatch<D>) -> IOResult<()> {
        self.output.borrow_mut().push_batch_mut(batch)
    }

    pub fn push_into_iter<I: Iterator<Item = D> + Send + 'static>(
        &self, tag: &Tag, iter: I,
    ) -> IOResult<()> {
        self.output
            .borrow_mut()
            .push_into_iter(tag, iter)
    }
}

impl<D: Data> OutputProxy for RefWrapOutput<D> {
    #[inline]
    fn flush(&self) -> IOResult<()> {
        self.output.borrow_mut().flush()
    }

    fn get_blocks(&self) -> Ref<VecDeque<Tag>> {
        let b = self.output.borrow();
        Ref::map(b, |b| b.get_blocks())
    }

    #[inline]
    fn try_unblock(&self, unblocked: &mut Vec<Tag>) -> IOResult<()> {
        self.output.borrow_mut().try_unblock(unblocked)
    }

    #[inline]
    fn notify_end(&self, end: EndSignal) -> IOResult<()> {
        self.output.borrow_mut().notify_end(end)
    }

    #[inline]
    fn skip(&self, tag: &Tag) {
        self.output.borrow_mut().skip(tag);
    }

    #[inline]
    fn close(&self) -> IOResult<()> {
        self.output.borrow_mut().close()
    }

    #[inline]
    fn is_closed(&self) -> bool {
        self.output.borrow().is_closed()
    }
}

#[inline(always)]
pub fn new_output<'a, D: Data>(generic: &'a Box<dyn OutputProxy>) -> &'a RefWrapOutput<D> {
    RefWrapOutput::<D>::downcast(generic)
}
