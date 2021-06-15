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

/// Describing how data's tag will be changed when being output from an output port.
///
/// # Please note!
/// Since we've just a few built-in operators manipulating data's tag, for simplicity
/// reasons, in the system, `OutputDelta` is defined on per output perspective and
/// against all inputs.
///
/// For example, to generate output data, if a binary operator would advance data tag
/// of one input, it will and must do the same for the other inputs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputDelta {
    /// Data's tag won't be changed.
    None,
    /// Advance the current counter of tag, usually the loop body output
    /// of LoopController.
    Advance,
    /// Add a new dimension for tag, usually the EnterScope operator.
    ToChild,
    /// Remove current dimension of tag, usually the LeaveScope operator.
    ToParent(u32),
}

pub trait OutputProxy: AsAny + Send {
    fn output_delta(&self) -> OutputDelta;

    /// To check if this port has enough space to output data;
    fn has_capacity(&self) -> bool;

    /// Reset the output capacity;
    fn reset_capacity(&self);

    fn batch_size(&self) -> usize;

    /// Notify this output that the scope with tag in parameter was closed in upstreams;
    fn scope_end(&self, tag: Tag);

    fn global_scope_end(&self, tag: Tag);

    /// Hint this output that keep the scope with tag in parameter open even if it was closed in upstreams;
    fn retain(&self, tag: &Tag);

    /// Notify this output won't retain scope with tag in parameter any more;
    fn drop_retain(&self, tag: &Tag);

    fn ignore(&self, tag: &Tag);

    /// Check if this output port has been closed;
    fn is_closed(&self) -> bool;

    /// Stop to output data with tag in parameter to the communication_old with index in parameter from now on;
    fn skip(&self, ch_index: u32, tag: &Tag) -> bool;

    /// Close scopes that are notified was already closed in upstreams, and are not requested to retain;
    fn close_scopes(&self) -> IOResult<()>;

    /// Close this output port, all the channels attached on this port would be notified;
    fn close(&self) -> IOResult<()>;
}

pub trait OutputBuilder: AsAny {
    fn build(self: Box<Self>) -> Box<dyn OutputProxy>;
}

mod builder;
mod output;
mod session;
mod tee;
pub(crate) use builder::{OutputBuilderImpl, OutputEntry};
pub(crate) use output::RefWrapOutput;
pub use session::OutputSession;

impl<D: Data> RefWrapOutput<D> {
    fn new_session(&self, tag: &Tag) -> OutputSession<D> {
        let borrow = self.output.borrow_mut();
        OutputSession::new(&self.capacity, borrow, tag)
    }
}

#[inline(always)]
pub fn new_output_session<'a, D: Data>(
    generic: &'a Box<dyn OutputProxy>, tag: &Tag,
) -> OutputSession<'a, D> {
    RefWrapOutput::<D>::downcast(generic).new_session(tag)
}
