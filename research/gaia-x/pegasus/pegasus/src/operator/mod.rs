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

use crate::api::meta::OperatorInfo;
use crate::api::scope::MergedScopeDelta;
use crate::api::Notification;
use crate::channel_id::ChannelInfo;
use crate::communication::input::{new_input, InputBlockGuard, InputProxy};
use crate::communication::output::{OutputBuilder, OutputBuilderImpl, OutputProxy};
use crate::config::BRANCH_OPT;
use crate::config::CANCEL_DESC;
use crate::data::DataSet;
use crate::data_plane::{GeneralPull, GeneralPush};
use crate::errors::{IOResult, JobExecError};
use crate::event::emitter::EventEmitter;
use crate::graph::Port;
use crate::progress::EndSignal;
use crate::schedule::state::inbound::InputEndNotify;
use crate::tag::tools::map::TidyTagMap;
use crate::{Data, Tag};
use std::cell::Cell;
use std::collections::HashSet;
use std::time::Instant;

pub trait Notifiable: Send + 'static {
    fn on_notify(&mut self, n: Notification, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError>;

    /// `on_cancel` is used to judge if the all the signals are received,
    /// as well as propagating signals and cleaning data.
    /// Return `Ok(true)` means that early-stop signals from all the ports are received.  
    fn on_cancel(
        &mut self, port: Port, tag: Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<bool, JobExecError>;
}

impl<T: ?Sized + Notifiable> Notifiable for Box<T> {
    fn on_notify(&mut self, n: Notification, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        (**self).on_notify(n, outputs)
    }

    fn on_cancel(
        &mut self, port: Port, tag: Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<bool, JobExecError> {
        (**self).on_cancel(port, tag, inputs, outputs)
    }
}

struct MultiNotifyMerge {
    input_size: usize,
    in_merge: Vec<TidyTagMap<(EndSignal, usize)>>,
}

impl MultiNotifyMerge {
    pub fn new(input_size: usize, scope_level: usize) -> Self {
        let mut merge = Vec::with_capacity(scope_level + 1);
        for i in 0..scope_level + 1 {
            merge.push(TidyTagMap::new(i))
        }
        MultiNotifyMerge { input_size, in_merge: merge }
    }

    fn merge(&mut self, n: Notification) -> Option<EndSignal> {
        let idx = n.tag().len();
        assert!(idx < self.in_merge.len());
        if let Some((mut sig, mut count)) = self.in_merge[idx].remove(n.tag()) {
            trace_worker!("merge {}th end of {:?} from input port {}", count + 1, n.tag(), n.port);
            let (tag, weight, _) = n.take_end().take();
            sig.source_weight.merge(weight);
            count += 1;
            if count == self.input_size {
                Some(sig)
            } else {
                self.in_merge[idx].insert(tag, (sig, count));
                None
            }
        } else {
            trace_worker!("merge first end of {:?} from input port {}", n.tag(), n.port);
            let end = n.take_end();
            self.in_merge[idx].insert(end.tag.clone(), (end, 1));
            None
        }
    }
}

enum DefaultNotify {
    Single,
    Merge(MultiNotifyMerge),
}

pub struct DefaultNotifyOperator<T> {
    op: T,
    notify: DefaultNotify,
    cancels_received: Vec<TidyTagMap<HashSet<usize>>>,
}

impl<T> DefaultNotifyOperator<T> {
    fn new(input_size: usize, scope_level: usize, op: T) -> Self {
        let notify = if input_size > 1 {
            DefaultNotify::Merge(MultiNotifyMerge::new(input_size, scope_level))
        } else {
            DefaultNotify::Single
        };
        let cancels_received = (0..scope_level + 1)
            .map(|i| TidyTagMap::new(i))
            .collect();
        DefaultNotifyOperator { op, notify, cancels_received }
    }

    fn notify_output(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        if outputs.len() > 0 {
            match self.notify {
                DefaultNotify::Single => {
                    assert_eq!(n.port, 0);
                    let end = n.take_end();
                    if outputs.len() > 1 {
                        for output in &outputs[1..] {
                            output.notify_end(end.clone())?;
                        }
                    }
                    outputs[0].notify_end(end)?;
                    Ok(())
                }
                DefaultNotify::Merge(ref mut m) => {
                    if let Some(end) = m.merge(n) {
                        if outputs.len() > 1 {
                            for output in &outputs[1..] {
                                output.notify_end(end.clone())?;
                            }
                        }
                        outputs[0].notify_end(end)?;
                    }
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }
}

impl<T: Send + 'static> Notifiable for DefaultNotifyOperator<T> {
    fn on_notify(&mut self, n: Notification, outputs: &[Box<dyn OutputProxy>]) -> Result<(), JobExecError> {
        if !outputs.is_empty() {
            self.notify_output(n, outputs)
        } else {
            Ok(())
        }
    }

    fn on_cancel(
        &mut self, port: Port, tag: Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<bool, JobExecError> {
        if outputs.len() == 1 {
            for input in inputs.iter() {
                input.cancel_scope(&tag);
                input.propagate_cancel(&tag)?;
            }
            outputs[0].skip(&tag);
            Ok(true)
        } else {
            let idx = tag.len();
            if *BRANCH_OPT {
                outputs[port.port].skip(&tag);
            }
            if let Some(mut port_set) = self.cancels_received[idx].remove(&tag) {
                port_set.insert(port.port);
                if port_set.len() == outputs.len() {
                    // received from all the ports, propagate cancel signal and clear data
                    for input in inputs.iter() {
                        input.cancel_scope(&tag);
                        input.propagate_cancel(&tag)?;
                    }
                    if !*BRANCH_OPT {
                        for output in outputs.iter() {
                            output.skip(&tag);
                        }
                    }
                    Ok(true)
                } else {
                    self.cancels_received[idx].insert(tag, port_set);
                    Ok(false)
                }
            } else {
                let mut port_set = HashSet::new();
                port_set.insert(port.port);
                self.cancels_received[idx].insert(tag, port_set);
                Ok(false)
            }
        }
    }
}

pub trait OperatorCore: Send + 'static {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError>;
}

impl<T: ?Sized + OperatorCore> OperatorCore for Box<T> {
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        (**self).on_receive(inputs, outputs)
    }
}

impl<T: OperatorCore> OperatorCore for DefaultNotifyOperator<T> {
    #[inline]
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        self.op.on_receive(inputs, outputs)
    }
}

pub trait NotifiableOperator: Notifiable + OperatorCore {}

impl<T: ?Sized + OperatorCore + Notifiable> NotifiableOperator for T {}

pub enum GeneralOperator {
    Simple(Box<dyn OperatorCore>),
    Notifiable(Box<dyn NotifiableOperator>),
}

/// 算子调度的输入条件：
///
/// 1. input 有数据 ;
/// 2. operator 是 active ;
/// 3. output 有capacity ;
///
/// 可调度判断：
/// 3 and (1 or 2)
///
///
pub struct Operator {
    pub info: OperatorInfo,
    inputs: Vec<Box<dyn InputProxy>>,
    outputs: Vec<Box<dyn OutputProxy>>,
    core: Box<dyn NotifiableOperator>,
    block_guards: Vec<TidyTagMap<Vec<InputBlockGuard>>>,
    unblocked: Vec<Tag>,
    fire_times: u128,
    exec_st: Cell<u128>,
}

impl Operator {
    // #[inline]
    // pub fn index(&self) -> usize {
    //     self.info.index
    // }

    // #[inline]
    // pub fn inputs(&self) -> &[Box<dyn InputProxy>] {
    //     &self.inputs
    // }
    //
    // #[inline]
    // pub fn outputs(&self) -> &[Box<dyn OutputProxy>] {
    //     &self.outputs
    // }

    pub fn has_outstanding(&self) -> IOResult<bool> {
        for input in self.inputs.iter() {
            if input.has_outstanding()? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        for output in self.outputs.iter() {
            if !output.get_blocks().is_empty() {
                return false;
            }
        }

        self.inputs.is_empty() || self.inputs.iter().all(|i| i.is_exhaust())
    }

    pub fn is_idle(&self) -> IOResult<bool> {
        for output in self.outputs.iter() {
            if !output.get_blocks().is_empty() {
                return Ok(false);
            }
        }

        Ok(!self.has_outstanding()?)
    }

    #[inline]
    pub fn fire(&mut self) -> Result<(), JobExecError> {
        let _f = Finally::new(&self.exec_st);
        debug_worker!("fire operator {:?}", self.info);
        self.fire_times += 1;

        let mut unblocks = std::mem::replace(&mut self.unblocked, vec![]);
        for (i, output) in self.outputs.iter().enumerate() {
            output.try_unblock(&mut unblocks)?;
            for x in unblocks.drain(..) {
                self.block_guards[i].remove(&x);
            }
        }
        self.unblocked = unblocks;

        let result = self
            .core
            .on_receive(&self.inputs, &self.outputs);

        for (i, output) in self.outputs.iter().enumerate() {
            for b in output.get_blocks().iter() {
                if !self.block_guards[i].contains_key(b) {
                    let mut guards = Vec::with_capacity(self.inputs.len());
                    for input in self.inputs.iter() {
                        let g = input.block(b);
                        guards.push(g);
                    }
                    self.block_guards[i].insert(b.clone(), guards);
                }
            }
        }

        let mut r = Ok(());
        if let Err(err) = result {
            if !err.can_be_retried() {
                return Err(err);
            } else {
                r = Err(err);
            }
        };

        for (port, input) in self.inputs.iter().enumerate() {
            while let Some(end) = input.extract_end() {
                let notification = Notification::new(port, end);
                self.core
                    .on_notify(notification, &self.outputs)?;
            }
        }

        for output in self.outputs.iter() {
            output.flush()?;
        }
        debug_worker!("after fire operator {:?}", self.info);
        r
    }

    pub fn cancel(&mut self, port: Port, tag: Tag) -> Result<(), JobExecError> {
        debug_worker!(
            "EARLY-STOP: try to cancel scope tag {:?} of port {:?} in operator {:?}",
            tag,
            port,
            self.info
        );
        if *BRANCH_OPT {
            if *CANCEL_DESC {
                self.block_guards[port.port].retain(|t, _| !tag.is_parent_of(t) && !tag.eq(t));
            } else {
                self.block_guards[port.port].remove(&tag);
            }
        }
        let tag_clone = tag.clone();
        if self
            .core
            .on_cancel(port, tag, &self.inputs, &self.outputs)?
        {
            debug_worker!(
            "EARLY-STOP: received cancel signals tag {:?} from all ports in operator {:?}, cancel data and propagate backward", 
            tag_clone,
            self.info
        );
            if !*BRANCH_OPT {
                if *CANCEL_DESC {
                    for block_guard in self.block_guards.iter_mut() {
                        block_guard.retain(|t, _| !tag_clone.is_parent_of(t) && !tag_clone.eq(t));
                    }
                } else {
                    for block_guard in self.block_guards.iter_mut() {
                        block_guard.remove(&tag_clone);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn close(&self) {
        for output in self.outputs.iter() {
            if let Err(err) = output.close() {
                warn_worker!("close operator {:?}'s output failure, caused by {}", self.info, err);
            }
        }
        debug_worker!(
            "operator {:?}\tfinished, used {:.2}ms, fired {} times, avg fire use {}us",
            self.info,
            self.exec_st.get() as f64 / 1000.0,
            self.fire_times,
            self.exec_st.get() / self.fire_times
        );
    }
}

pub struct OperatorBuilder {
    pub info: OperatorInfo,
    inputs: Vec<Box<dyn InputProxy>>,
    inputs_notify: Vec<Option<Box<dyn InputEndNotify>>>,
    outputs: Vec<Box<dyn OutputBuilder>>,
    core: GeneralOperator,
}

impl OperatorBuilder {
    pub fn new(meta: OperatorInfo, core: GeneralOperator) -> Self {
        OperatorBuilder { info: meta, inputs: vec![], inputs_notify: vec![], outputs: vec![], core }
    }

    pub fn index(&self) -> usize {
        self.info.index
    }

    pub(crate) fn add_input<T: Data>(
        &mut self, ch_info: ChannelInfo, pull: GeneralPull<DataSet<T>>,
        notify: Option<GeneralPush<DataSet<T>>>, event_emitter: &EventEmitter, delta: MergedScopeDelta,
    ) {
        assert_eq!(ch_info.target_port.port, self.inputs.len());
        let input = new_input(ch_info, pull, event_emitter, delta);
        self.inputs.push(input);
        let n = notify.map(|p| Box::new(p) as Box<dyn InputEndNotify>);
        self.inputs_notify.push(n);
    }

    pub(crate) fn next_input_port(&self) -> Port {
        Port::new(self.info.index, self.inputs.len())
    }

    pub(crate) fn new_output<D: Data>(&mut self) -> OutputBuilderImpl<D> {
        let port = Port::new(self.info.index, self.outputs.len());
        let output = OutputBuilderImpl::new(port, self.info.scope_level);
        self.outputs.push(Box::new(output.clone()));
        output
    }

    pub(crate) fn take_inputs_notify(&mut self) -> Vec<Option<Box<dyn InputEndNotify>>> {
        std::mem::replace(&mut self.inputs_notify, vec![])
    }

    pub(crate) fn build(self) -> Operator {
        let mut outputs = Vec::new();
        let mut block_guards = Vec::new();
        for ob in self.outputs {
            if let Some(o) = ob.build() {
                outputs.push(o);
            }
            block_guards.push(TidyTagMap::new(self.info.scope_level));
        }

        let core = match self.core {
            GeneralOperator::Simple(op) => {
                let scope_level = self.info.scope_level;
                let op = DefaultNotifyOperator::new(self.inputs.len(), scope_level, op);
                Box::new(op) as Box<dyn NotifiableOperator>
            }
            GeneralOperator::Notifiable(op) => op,
        };
        Operator {
            info: self.info,
            inputs: self.inputs,
            outputs,
            core,
            block_guards,
            unblocked: vec![],
            fire_times: 0,
            exec_st: Cell::new(0),
        }
    }
}

struct Finally<'a> {
    exec_st: &'a Cell<u128>,
    start: Instant,
}

impl<'a> Finally<'a> {
    pub fn new(exec_st: &'a Cell<u128>) -> Self {
        Finally { exec_st, start: Instant::now() }
    }
}

impl<'a> Drop for Finally<'a> {
    fn drop(&mut self) {
        let s = self.exec_st.get() + self.start.elapsed().as_micros();
        self.exec_st.set(s);
    }
}

mod concise;
mod iteration;
mod multiplex;
mod primitives;
mod scope;
