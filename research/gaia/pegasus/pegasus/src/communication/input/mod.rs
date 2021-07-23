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

use crate::communication::channel::ChannelMeta;
use crate::data::DataSet;
use crate::data_plane::GeneralPull;
use crate::errors::IOResult;
use crate::event::{ChannelRxState, EventBus};
use crate::{Data, Tag};
use pegasus_common::downcast::*;
use pegasus_common::rc::RcPointer;
use std::collections::HashSet;

/// Abstraction proxy of a communication_old consumer; Used to get the inner state of the communication_old;
pub trait InputProxy: AsAny + Send {
    /// Get the identifier of communication_old whose output connect to this input proxy;
    fn index(&self) -> usize;

    /// Check if the communication_old is closed;
    fn is_exhaust(&self) -> bool;

    fn cancel(&self, tag: &Tag);

    fn next(&self, targets: &HashSet<Tag>) -> IOResult<Option<Tag>>;

    fn get_state(&self) -> &RcPointer<ChannelRxState>;
}

mod input;
mod session;
use input::InboundChannel;
use input::RefWrapInput;
pub use session::InputSession;

impl<D: Data> RefWrapInput<D> {
    pub fn new_session(&self, tag: Tag) -> InputSession<D> {
        let borrow = self.inbound.borrow_mut();
        InputSession::new(tag, borrow)
    }
}

#[inline]
pub fn new_input_session<'a, D: Data>(
    raw: &'a Box<dyn InputProxy>, tag: &Tag,
) -> InputSession<'a, D> {
    RefWrapInput::<D>::downcast(raw).new_session(tag.clone())
}

#[inline]
pub(crate) fn new_input<D: Data>(
    meta: ChannelMeta, scope_depth: usize, event_bus: &EventBus, pull: GeneralPull<DataSet<D>>,
) -> Box<dyn InputProxy> {
    let input = InboundChannel::new(meta, scope_depth, event_bus.clone(), pull);
    Box::new(RefWrapInput::wrap(input)) as Box<dyn InputProxy>
}
