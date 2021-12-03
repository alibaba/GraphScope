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

use std::cell::RefCell;
use std::rc::Rc;

use pegasus_common::downcast::*;

use crate::api::scope::ScopeDelta;
use crate::communication::cancel::CancelListener;
use crate::communication::output::output::OutputHandle;
use crate::communication::output::tee::{PerChannelPush, Tee};
use crate::communication::output::{OutputBuilder, OutputProxy, RefWrapOutput};
use crate::graph::Port;
use crate::schedule::state::outbound::OutputCancelState;
use crate::Data;

#[derive(Copy, Clone, Debug)]
pub struct OutputMeta {
    pub port: Port,
    /// This is the the scope level of operator with this output port belongs to.
    pub scope_level: u32,
    pub batch_size: usize,
    pub batch_capacity: u32,
}

pub struct OutputBuilderImpl<D: Data> {
    ///
    meta: Rc<RefCell<OutputMeta>>,
    ///
    cursor: usize,
    ///
    shared: Rc<RefCell<Vec<Option<PerChannelPush<D>>>>>,
}

impl<D: Data> OutputBuilderImpl<D> {
    pub fn new(port: Port, scope_level: u32, batch_size: usize, batch_capacity: u32) -> Self {
        let shared = vec![None];
        OutputBuilderImpl {
            meta: Rc::new(RefCell::new(OutputMeta { port, scope_level, batch_size, batch_capacity })),
            cursor: 0,
            shared: Rc::new(RefCell::new(shared)),
        }
    }

    pub fn get_port(&self) -> Port {
        self.meta.borrow().port
    }

    pub fn get_batch_size(&self) -> usize {
        self.meta.borrow().batch_size
    }

    pub fn get_batch_capacity(&self) -> u32 {
        self.meta.borrow().batch_capacity
    }

    pub fn get_scope_level(&self) -> u32 {
        self.meta.borrow().scope_level
    }

    pub fn set_batch_size(&self, size: usize) {
        if self.cursor != 0 {
            warn!("detect reset batch size after stream copy, copy after set batch size would be better;")
        }
        self.meta.borrow_mut().batch_size = size;
    }

    pub fn set_batch_capacity(&self, cap: u32) {
        if self.cursor != 0 {
            warn!("detect reset batch capacity after stream copy, copy after set batch capacity would be better;")
        }
        self.meta.borrow_mut().batch_capacity = cap;
    }

    #[inline]
    pub(crate) fn set_push(&self, push: PerChannelPush<D>) {
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
        let cursor = self.shared.borrow().len() - 1;
        OutputBuilderImpl { meta: self.meta.clone(), cursor, shared: self.shared.clone() }
    }
}

impl<D: Data> Clone for OutputBuilderImpl<D> {
    fn clone(&self) -> Self {
        OutputBuilderImpl { meta: self.meta.clone(), cursor: self.cursor, shared: self.shared.clone() }
    }
}

impl_as_any!(OutputBuilderImpl<D: Data>);

impl<D: Data> OutputBuilder for OutputBuilderImpl<D> {
    fn build_cancel_handle(&self) -> Option<OutputCancelState> {
        let port = self.meta.borrow().port;
        let scope_level = self.meta.borrow().scope_level;

        let shared = self.shared.borrow();
        let size = shared.iter().filter(|x| x.is_some()).count();
        if size == 0 {
            None
        } else if size == 1 {
            for ch in shared.iter() {
                if let Some(ch) = ch.as_ref() {
                    let listener = ch.get_cancel_handle();
                    let index = ch.ch_info.id.index;
                    return Some(OutputCancelState::single(port, index, Box::new(listener)));
                }
            }
            None
        } else {
            let mut vec = Vec::with_capacity(size);
            for ch in shared.iter() {
                if let Some(ch) = ch.as_ref() {
                    let listener = ch.get_cancel_handle();
                    let index = ch.ch_info.id.index;
                    vec.push((index, Box::new(listener) as Box<dyn CancelListener>));
                }
            }
            Some(OutputCancelState::tee(port, scope_level, vec))
        }
    }

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
            let meta = self.meta.borrow();
            let mut tee = Tee::<D>::new(meta.port, meta.scope_level, main_push);
            for p in shared.iter_mut() {
                if let Some(push) = p.take() {
                    tee.add_push(push);
                }
            }

            let output = OutputHandle::new(*meta, tee);
            Some(Box::new(RefWrapOutput::wrap(output)) as Box<dyn OutputProxy>)
        } else {
            None
        }
    }
}
