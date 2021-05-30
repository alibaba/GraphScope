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

use crate::api::meta::OperatorMeta;
use crate::api::notify::Notification;
use crate::communication::input::InputProxy;
use crate::communication::output::{OutputBuilder, OutputBuilderImpl, OutputProxy};
use crate::errors::JobExecError;
use crate::event::EventBus;
use crate::graph::Port;
use crate::{Data, Tag};
use std::collections::HashMap;

/// Describe the operator's state after it been fired;
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum FiredState {
    /// Indicates that all outstanding works during this fire have been done;
    Idle,
    /// Indicates that there are still outstanding works unfinished during this fire;
    Active,
}

pub static FIRED_STATE: [FiredState; 2] = [FiredState::Idle, FiredState::Active];

pub trait OperatorCore: Send {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError>;

    fn on_active(
        &mut self, _active: &Tag, _o: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, _: Notification, _: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        Ok(())
    }
}

mod cancel;
use crate::operator::cancel::CancelSignal;
pub use cancel::CancelGuard;
use cancel::DefaultCancelGuard;

struct Active {
    notified_ports: Vec<usize>,
    state: FiredState,
}

impl Default for Active {
    fn default() -> Self {
        Active { notified_ports: Vec::new(), state: FiredState::Active }
    }
}

pub struct Operator {
    pub meta: OperatorMeta,
    inputs: Vec<Box<dyn InputProxy>>,
    outputs: Vec<Box<dyn OutputProxy>>,
    core: Box<dyn OperatorCore>,
    actives: HashMap<Tag, Active>,
    cancel: Box<dyn CancelGuard>,
}

impl Operator {
    #[inline]
    pub fn inputs(&self) -> &[Box<dyn InputProxy>] {
        &self.inputs
    }

    #[inline]
    pub fn outputs(&self) -> &[Box<dyn OutputProxy>] {
        &self.outputs
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        self.actives.is_empty()
            && (self.inputs.is_empty() || self.inputs.iter().all(|i| i.is_exhaust()))
    }

    #[inline]
    pub fn has_actives(&self) -> bool {
        !self.actives.is_empty()
    }

    #[inline]
    pub fn has_outstanding(&self) -> bool {
        let len = self.inputs.len();
        if len == 0 {
            false
        } else if len == 1 {
            self.inputs[0].get_state().has_outstanding()
        } else {
            self.inputs.iter().any(|i| i.get_state().has_outstanding())
        }
    }

    #[inline]
    pub fn has_output_capacity(&self) -> bool {
        let len = self.outputs.len();
        if len == 0 {
            true
        } else if len == 1 {
            self.outputs[0].has_capacity()
        } else {
            self.outputs.iter().all(|o| o.has_capacity())
        }
    }

    #[inline]
    pub fn has_notifications(&self) -> bool {
        let len = self.inputs.len();
        if len == 0 {
            false
        } else if len == 1 {
            !self.inputs[0].get_state().notifications().is_empty()
        } else {
            self.inputs.iter().any(|i| !i.get_state().notifications().is_empty())
        }
    }

    pub fn close_outputs(&self) {
        for output in self.outputs.iter() {
            if let Err(err) = output.close() {
                warn_worker!("close operator {:?}'s output failure, caused by {}", self.meta, err);
            }
        }
        trace_worker!("operator {:?} finished;", self.meta);
    }

    pub fn fire_actives(&mut self) -> Result<(), JobExecError> {
        let mut actives = std::mem::replace(&mut self.actives, HashMap::new());
        for (tag, active) in actives.iter_mut() {
            trace_worker!("fire operator {:?} on actives {:?};", self.meta, tag);
            if FiredState::Idle == self.core.on_active(tag, &self.outputs)? {
                active.state = FiredState::Idle;
                for p in active.notified_ports.drain(..) {
                    let notification = Notification::new(p, tag.clone());
                    trace_worker!("fire operator {:?} on notify {:?};", self.meta, notification);
                    self.core.on_notify(notification, &self.outputs)?;
                }
                self.outputs.iter().for_each(|o| o.drop_retain(tag));
            }
        }
        actives.retain(|_, v| v.state == FiredState::Active);
        self.actives = actives;
        Ok(())
    }

    pub fn fire_on_receive(&mut self, tag: &Tag) -> Result<(), JobExecError> {
        if self.actives.contains_key(tag) {
            Ok(())
        } else {
            trace_worker!("fire operator {:?} on receive {:?};", self.meta, tag);
            if FiredState::Active == self.core.on_receive(tag, &self.inputs, &self.outputs)? {
                let active = Active::default();
                self.actives.insert(tag.clone(), active);
                self.outputs.iter().for_each(|o| o.retain(tag));
            }
            Ok(())
        }
    }

    pub fn notify(&mut self) -> Result<(), JobExecError> {
        for (port, input) in self.inputs.iter().enumerate() {
            for n in input.get_state().notifications().drain(..) {
                self.outputs.iter().for_each(|o| o.scope_end(n.clone()));
                if let Some(active) = self.actives.get_mut(&n) {
                    active.notified_ports.push(port);
                    // self.outputs.iter().for_each(|o| o.retain(&n));
                } else if self.meta.notifiable {
                    let n = Notification::new(port, n.clone());
                    trace_worker!("fire operator {:?} on notify {:?};", self.meta, n);
                    self.core.on_notify(n, &self.outputs)?;
                }
            }
        }
        Ok(())
    }

    pub fn cancel(&mut self, port: usize, ch_index: u32, tag: Tag) -> Result<(), JobExecError> {
        assert!(port < self.outputs.len(), "{:?} : output port {:?} not exist;", self.meta, port);
        let signal = CancelSignal { port, ch_index, tag };
        debug_worker!("operator {:?} receive cancel signal {:?}", self.meta, signal);

        for tag in self.cancel.cancel(signal, &self.outputs).drain(..) {
            for input in self.inputs.iter() {
                input.cancel(&tag);
            }
            if let Some(v) = self.actives.remove(&tag) {
                for p in v.notified_ports {
                    for output in self.outputs.iter() {
                        output.drop_retain(&tag);
                    }
                    let n = Notification::new(p, tag.clone());
                    self.core.on_notify(n, &self.outputs)?;
                }
            }
        }
        Ok(())
    }
}

pub struct OperatorBuilder {
    pub meta: OperatorMeta,
    inputs: Vec<Box<dyn InputProxy>>,
    outputs: Vec<Box<dyn OutputBuilder>>,
    core: Box<dyn OperatorCore>,
    cancel: Option<Box<dyn CancelGuard>>,
    event_bus: EventBus,
}

impl OperatorBuilder {
    pub fn new(meta: OperatorMeta, core: Box<dyn OperatorCore>, event_bus: &EventBus) -> Self {
        OperatorBuilder {
            meta,
            inputs: vec![],
            outputs: vec![],
            core,
            cancel: None,
            event_bus: event_bus.clone(),
        }
    }

    pub fn index(&self) -> usize {
        self.meta.index
    }

    pub fn set_cancel_guard<G: CancelGuard>(&mut self, guard: G) {
        self.cancel = Some(Box::new(guard));
    }

    pub(crate) fn add_input(&mut self, input: Box<dyn InputProxy>) -> Port {
        self.inputs.push(input);
        Port::new(self.meta.index, self.inputs.len() - 1)
    }

    pub(crate) fn new_output<D: Data>(&mut self) -> OutputBuilderImpl<D> {
        let port = Port::new(self.meta.index, self.outputs.len());
        let mut output = OutputBuilderImpl::new(port, self.meta.delta, &self.event_bus);
        output.scope_depth = self.meta.scope_depth;
        output.batch_size = self.meta.batch_size;
        output.mem_limit = self.meta.mem_limit as usize;
        output.capacity = self.meta.capacity as u32;
        self.outputs.push(Box::new(output.clone()));
        output
    }

    pub(crate) fn build(mut self) -> Operator {
        let mut outputs = Vec::new();
        for ob in self.outputs {
            outputs.push(ob.build());
        }
        let mut actives = HashMap::new();
        if self.inputs.len() == 0 {
            actives.insert(Default::default(), Active::default());
        }
        let cancel =
            self.cancel.take().unwrap_or_else(|| Box::new(DefaultCancelGuard::new(outputs.len())));

        Operator { meta: self.meta, inputs: self.inputs, outputs, core: self.core, actives, cancel }
    }
}

mod binary;
mod branch;
mod concise;
mod iteration;
mod multiplex;
mod scope;
mod sink;
mod source;
mod unary;

pub use concise::{never_clone, NeverClone};
