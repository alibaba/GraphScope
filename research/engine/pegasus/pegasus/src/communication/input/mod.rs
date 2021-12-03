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

use pegasus_common::downcast::*;

use crate::channel_id::ChannelInfo;
use crate::data::MicroBatch;
use crate::data_plane::GeneralPull;
use crate::errors::IOResult;
use crate::event::emitter::EventEmitter;
use crate::progress::EndOfScope;
use crate::{Data, Tag};

/// Input abstraction without data type;
pub trait InputProxy: AsAny + Send {
    fn has_outstanding(&self) -> IOResult<bool>;

    fn block(&self, tag: &Tag) -> InputBlockGuard;

    fn extract_end(&self) -> Option<EndOfScope>;

    fn is_exhaust(&self) -> bool;

    fn cancel_scope(&self, tag: &Tag);
}

mod input;
mod session;

pub use input::InputBlockGuard;
use input::{InputHandle, RefWrapInput};
pub use session::InputSession;

#[inline]
pub(crate) fn new_input<D: Data>(
    ch_info: ChannelInfo, pull: GeneralPull<MicroBatch<D>>, event_emitter: &EventEmitter,
) -> Box<dyn InputProxy> {
    let input = InputHandle::new(ch_info, pull, event_emitter.clone());
    Box::new(RefWrapInput::wrap(input)) as Box<dyn InputProxy>
}

pub fn new_input_session<D: Data>(input: &Box<dyn InputProxy>) -> InputSession<D> {
    RefWrapInput::<D>::downcast(input).new_session()
}
