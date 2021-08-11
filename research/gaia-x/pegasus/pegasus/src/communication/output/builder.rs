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

use crate::api::scope::{MergedScopeDelta, ScopeDelta};
use crate::channel_id::ChannelInfo;
use crate::communication::decorator::MicroBatchPush;
use crate::communication::output::output::OutputHandle;
use crate::communication::output::tee::{ChannelPush, Tee};
use crate::communication::output::{OutputBuilder, OutputProxy, RefWrapOutput};
use crate::graph::Port;
use crate::Data;
use pegasus_common::downcast::*;
use std::cell::RefCell;
use std::rc::Rc;

pub struct OutputBuilderImpl<D: Data> {
    ///
    pub(crate) port: Port,
    /// The scope level of operator this output belongs to; Not the scope level of data it will output;
    pub(crate) scope_level: usize,
    ///
    cursor: usize,
    ///
    shared: Rc<RefCell<Vec<Option<ChannelPush<D>>>>>,
}

impl<D: Data> OutputBuilderImpl<D> {
    pub fn new(port: Port, scope_level: usize) -> Self {
        let shared = vec![None];
        OutputBuilderImpl { port, scope_level, cursor: 0, shared: Rc::new(RefCell::new(shared)) }
    }

    #[inline]
    pub fn set_push(&self, ch_info: ChannelInfo, delta: MergedScopeDelta, push: MicroBatchPush<D>) {
        let push = ChannelPush::new(ch_info, delta, push);
        self.shared.borrow_mut()[self.cursor] = Some(push);
    }

    #[inline]
    pub fn output_size(&self) -> usize {
        self.shared.borrow().len()
    }

    pub fn add_output_delta(&self, delta: ScopeDelta) {
        if let Some(ref mut p) = self.shared.borrow_mut()[self.cursor] {
            p.delta.add_delta(delta);
        }
    }

    pub fn copy_data(&self) -> Self {
        self.shared.borrow_mut().push(None);
        OutputBuilderImpl {
            port: self.port,
            scope_level: self.scope_level,
            cursor: self.cursor + 1,
            shared: self.shared.clone(),
        }
    }
}

impl<D: Data> Clone for OutputBuilderImpl<D> {
    fn clone(&self) -> Self {
        OutputBuilderImpl {
            port: self.port,
            scope_level: self.scope_level,
            cursor: self.cursor,
            shared: self.shared.clone(),
        }
    }
}

impl_as_any!(OutputBuilderImpl<D: Data>);

impl<D: Data> OutputBuilder for OutputBuilderImpl<D> {
    fn build(self: Box<Self>) -> Option<Box<dyn OutputProxy>> {
        let mut shared = self.shared.borrow_mut();
        let mut main_push = None;
        for p in shared.iter_mut() {
            if let Some(push) = p.take() {
                main_push = Some(push);
                break;
            }
        }

        if let Some(main_push) = main_push {
            let mut tee = Tee::<D>::new(self.port, self.scope_level, main_push);
            for p in shared.iter_mut() {
                if let Some(push) = p.take() {
                    tee.add_push(push);
                }
            }

            let output = OutputHandle::new(self.port, self.scope_level, tee);
            Some(Box::new(RefWrapOutput::wrap(output)) as Box<dyn OutputProxy>)
        } else {
            None
        }
    }
}
