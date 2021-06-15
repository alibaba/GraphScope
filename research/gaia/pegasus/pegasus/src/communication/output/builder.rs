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

use crate::communication::decorator::DataPush;
use crate::communication::output::output::{OutputHandle, RefWrapOutput};
use crate::communication::output::tee::{ChannelPush, Tee};
use crate::communication::output::{OutputBuilder, OutputDelta, OutputProxy};
use crate::event::EventBus;
use crate::graph::Port;
use crate::Data;
use pegasus_common::downcast::*;
use smallvec::SmallVec;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

pub struct OutputEntry<D: Data> {
    pub ch_index: u32,
    pub is_local: bool,
    push: DataPush<D>,
}

impl<D: Data> OutputEntry<D> {
    pub fn new(ch_index: u32, is_local: bool, push: DataPush<D>) -> Self {
        OutputEntry { ch_index, is_local, push }
    }
}

pub struct OutputBuilderImpl<D: Data> {
    pub port: Port,
    pub delta: Rc<Cell<OutputDelta>>,
    pub batch_size: usize,
    pub capacity: u32,
    pub scope_depth: usize,
    pub mem_limit: usize,
    shared: Rc<RefCell<SmallVec<[OutputEntry<D>; 2]>>>,
    event_bus: EventBus,
}

impl<D: Data> OutputBuilderImpl<D> {
    pub fn new(port: Port, delta: OutputDelta, event_bus: &EventBus) -> Self {
        OutputBuilderImpl {
            port,
            delta: Rc::new(Cell::new(delta)),
            batch_size: 1024,
            scope_depth: 0,
            mem_limit: (!0u32) as usize,
            capacity: 64,
            shared: Rc::new(RefCell::new(SmallVec::new())),
            event_bus: event_bus.clone(),
        }
    }

    #[inline]
    pub fn add_push(&self, push: OutputEntry<D>) {
        self.shared.borrow_mut().push(push);
    }

    pub fn set_capacity(&mut self, capacity: u32) {
        self.capacity = capacity;
    }

    #[inline]
    pub fn output_size(&self) -> usize {
        self.shared.borrow().len()
    }

    #[inline]
    pub fn leave(&self) -> bool {
        let scope_depth = self.scope_depth as u32;
        if scope_depth >= 1 {
            let delta = self.delta.get();
            return match delta {
                OutputDelta::ToParent(mut n) => {
                    if n >= 1 {
                        n -= 1;
                        self.delta.set(OutputDelta::ToParent(n));
                        true
                    } else {
                        false
                    }
                }
                _ => {
                    self.delta.set(OutputDelta::ToParent(scope_depth - 1));
                    true
                }
            };
        }
        false
    }
}

impl<D: Data> Clone for OutputBuilderImpl<D> {
    fn clone(&self) -> Self {
        OutputBuilderImpl {
            port: self.port,
            delta: self.delta.clone(),
            batch_size: self.batch_size,
            capacity: self.capacity,
            scope_depth: self.scope_depth,
            mem_limit: self.mem_limit.clone(),
            shared: self.shared.clone(),
            event_bus: self.event_bus.clone(),
        }
    }
}

impl_as_any!(OutputBuilderImpl<D: Data>);

impl<D: Data> OutputBuilder for OutputBuilderImpl<D> {
    fn build(self: Box<Self>) -> Box<dyn OutputProxy> {
        let mut tee = Tee::<D>::new(&self.event_bus);
        let mut shared = self.shared.borrow_mut();
        for OutputEntry { ch_index, is_local, push } in shared.drain() {
            let p = ChannelPush::new(ch_index, is_local, self.scope_depth, push);
            tee.add_push(ch_index, p);
        }
        let mut output = OutputHandle::new(
            self.port,
            self.batch_size,
            self.capacity,
            self.delta.get(),
            self.scope_depth,
            tee,
        );
        output.set_job_mem_limit(self.mem_limit * 1 << 20);
        Box::new(RefWrapOutput::wrap(output)) as Box<dyn OutputProxy>
    }
}
