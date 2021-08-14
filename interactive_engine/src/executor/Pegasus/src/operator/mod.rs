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

use std::collections::{HashSet, VecDeque, HashMap};
use std::cell::{Cell, RefCell};
use std::sync::Arc;
use super::*;
use crate::channel::{IOResult, IOError};
use crate::channel::input::{TaggedInput, InputHandle};
use crate::channel::output::{TaggedOutput, TaggedOutputBuilder, OutputBuilder, OutputHandle, OutputDelta};
use crate::stream::{Stream, DataflowBuilder};
use crate::communication::Communicate;
use crate::common::Port;
use crate::strategy::Reserved;

pub mod unary;
pub mod binary;
pub mod source;
pub mod sink;
pub mod branch;
pub mod lazy;
pub mod advanced;

pub use unary::Unary;
pub use binary::Binary;
pub use branch::Branch;
pub use sink::{Sink, SinkBinary};
pub use source::IntoStream;
pub use lazy::unary::LazyUnary;
pub use lazy::binary::LazyBinary;
pub use advanced::iterate::Iteration;
pub use advanced::map::Map;
pub use advanced::exchange::Exchange;
pub use advanced::scope::Scope;
pub use advanced::inspect::Inspect;

/// Describe the operator's runtime state;
pub enum ScheduleState {
    /// Denote there no more computation to do;
    Idle,
    /// Denote there are some computation(or outputs) not finished, caused by some reasons,
    /// e.g. memory or other resource bound;
    /// The `Vec<Tag>` in this enum indicates the computation was pended on which data streams;
    Active(Vec<Tag>),
}

#[derive(Copy, Clone, Debug)]
pub enum OperatorMode {
    /// Describe the operator has no input channel, but has some outer(out of framework) message source;
    Source,
    /// Describe the operator output the same number of messages with it consumed inputs;
    Pass,
    /// Describe the operator will output more messages than its inputs;
    Expand,
    /// Describe the operator will output less message thin its inputs;
    Clip,
    /// There is no knowledge to know anything;
    Unknown,
    /// Describe the operator has no output, all messages will be send out of framework;
    Sink
}

#[derive(Clone, Debug)]
pub struct OperatorInfo {
    pub worker: WorkerId,
    pub name: String,
    pub index: usize,
    pub peers: usize,
    pub scopes: usize,
    pub mode: Cell<OperatorMode>,
}

impl OperatorInfo {
    pub fn new(worker: WorkerId, name: &str, index: usize, peers: usize, scopes: usize) -> Self {
        OperatorInfo {
            worker,
            name: name.to_owned(),
            index,
            peers,
            scopes,
            mode: Cell::new(OperatorMode::Unknown),
        }
    }

    #[inline]
    pub fn mode(&self) -> OperatorMode {
        self.mode.get()
    }

    #[inline]
    pub fn set_pass(&self) {
        self.mode.replace(OperatorMode::Pass);
    }

    #[inline]
    pub fn set_clip(&self) {
        self.mode.replace(OperatorMode::Clip);
    }

    #[inline]
    pub fn set_expand(&self) {
        self.mode.replace(OperatorMode::Expand);
    }

    pub fn set_source(&self) {
        self.mode.replace(OperatorMode::Source);
    }

    pub fn set_sink(&self) {
        self.mode.replace(OperatorMode::Sink);
    }
}

impl ::std::fmt::Display for OperatorInfo {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "[{}@{}.{}]", self.worker, self.name, self.index)
    }
}

#[derive(Debug)]
pub enum Notification {
    End(ChannelId, Tag),
    EOS(ChannelId),
    /// Iteration special;
    Iteration(Tag, WorkerId, ChannelId),
}

impl Notification {
    #[inline]
    pub fn channel(&self) -> ChannelId {
        match self {
            Notification::End(idx, _) => *idx,
            Notification::EOS(idx) => *idx,
            Notification::Iteration(_, _, idx) => *idx,
        }
    }

    #[inline]
    pub fn get_end(&self) -> Option<&Tag> {
        match self {
            Notification::End(_, tag) => Some(tag),
            _ => None
        }
    }
}

pub type FireResult = IOResult<ScheduleState>;

/// OperatorCore is a user define function support plug-in some user defined computation logic;
pub trait OperatorCore: Send {
    /// Define behavior when there are new available inputs;
    /// As a operator may have multi-inputs with different type of data, there use an array of trait
    /// object to represent all inputs, user's implementation should `downcast` these trait inputs
    /// into specific typed structs, as well as outputs;
    ///
    /// It is suggested that the user implementation of this function should be suspendable, and can yield
    /// cpu control to scheduler. Especially there are heavy works or lots of outputs;
    /// It's user's responsibility to maintain the computation state when it is suspended.
    fn on_receive(&mut self, inputs: &[RefCell<Box<dyn TaggedInput>>],
            outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<ScheduleState>;

    /// Resume the computation on the active data, which was suspended by the scheduler previously;
    /// Parameter `actives` is a set of `Tag`s, only the data with these tags are permitted to output;
    fn on_active(&mut self, _actives: &mut Vec<Tag>,
                 _outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()> {
        unimplemented!()
    }

    /// Define behavior when there are new notifications;
    fn on_notify(&mut self, notifies: &mut Vec<Notification>,
                 outputs: &[RefCell<Box<dyn TaggedOutput>>]) -> IOResult<()>;
}

pub struct Operator {
    inputs: Vec<RefCell<Box<dyn TaggedInput>>>,
    outputs: Vec<RefCell<Box<dyn TaggedOutput>>>,
    info: OperatorInfo,
    core: Box<dyn OperatorCore>,
    actives: HashSet<Tag>,
    notifications: VecDeque<Notification>,
    available_inputs: Vec<(Tag, usize)>,
}

impl Debug for Operator {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "Operator: {:?}", self.info)
    }
}


pub struct OperatorBuilder {
    inputs: Vec<RefCell<Box<dyn TaggedInput>>>,
    outputs: Vec<(Box<dyn TaggedOutputBuilder>, OutputDelta)>,
    info: OperatorInfo,
    core: Option<Box<dyn OperatorCore>>,
}

impl OperatorBuilder {
    pub fn new(info: OperatorInfo) -> Self {
        OperatorBuilder {
            inputs: Vec::new(),
            outputs: Vec::new(),
            info,
            core: None
        }
    }

    pub fn core<U: OperatorCore + 'static>(mut self, core: U) -> Self {
        self.core = Some(Box::new(core));
        self
    }

    pub fn add_input<D: Data>(mut self, input: InputHandle<D>) -> Self {
        self.inputs.push(RefCell::new(Box::new(input) as Box<dyn TaggedInput>));
        self
    }

    pub fn add_output<D: Data>(mut self, output: OutputBuilder<D>, delta: OutputDelta) -> Self {
        self.outputs.push((Box::new(output) as Box<dyn TaggedOutputBuilder>, delta));
        self
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.info.index
    }

    pub fn build(mut self) -> Result<Operator, String> {
        if let Some(core) = self.core.take() {
            let mut outputs = Vec::with_capacity(self.outputs.len());
            for (ob, delta) in self.outputs {
                outputs.push(RefCell::new(ob.build_output(delta)));
            }

            let mut actives = HashSet::new();
            if self.inputs.len() == 0 {
                actives.insert(Default::default());
            }

            Ok(Operator {
                inputs: self.inputs,
                outputs,
                info: self.info,
                core,
                actives,
                notifications: VecDeque::new(),
                available_inputs: Vec::new(),
            })
        } else {
            Err("Operator core is not defined".to_ascii_lowercase())
        }
    }
}

lazy_static! {
    static ref EMPTY_TAG_SET: Arc<HashSet<Tag>> =  Arc::new(HashSet::new()) ;
}

pub struct OutputCapacity {
    pub tag: Tag,
    pub capacity: i64
}

impl Operator {
    #[inline]
    pub fn info(&self) -> &OperatorInfo {
        &self.info
    }

    #[inline]
    pub fn add_notification(&mut self, n: Notification) {
        trace!("### Worker[{}] Operator {} new notify: {:?}", self.info.worker, self.info.name, n);
        self.notifications.push_back(n);
    }

    #[inline]
    pub fn add_available_input(&mut self, tag: Tag, count: usize) {
        self.available_inputs.push((tag, count));
    }

    #[inline]
    pub fn take_available_inputs(&mut self) -> ::std::vec::Drain<(Tag, usize)> {
        self.available_inputs.drain(..)
    }

    pub fn stash_input(&self, tags: &[Tag]) {
        for input in self.inputs.iter() {
            input.borrow_mut().stash(tags);
        }
    }

    pub fn stash_pop_input(&self, tags: &[Tag]) {
        for input in self.inputs.iter() {
            input.borrow_mut().stash_pop(tags);
        }
    }

    #[inline]
    pub fn set_outputs_capacity(&self, capacity: usize) {
        for port in 0..self.outputs.len() {
            self.outputs[port].borrow_mut().set_output_capacity(capacity)
        }
    }

    pub fn get_output_delta(&self, port: usize) -> OutputDelta {
        debug_assert!(port < self.outputs.len());
        *self.outputs[port].borrow().delta()
    }

    pub fn fire_on(&mut self, tag: Tag, remaining: usize) -> IOResult<()> {
        if let Some(active) = self.actives.take(&tag) {
            let mut tag = vec![active];
            trace!("### Worker[{}]: fire operator '{}' on active;", self.info.worker, self.info);
            self.core.on_active(&mut tag, &self.outputs)?;
            self.actives.extend(tag);
        }

        if !self.actives.contains(&tag) && remaining > 0 && self.has_capacity() {
            trace!("### Worker[{}]: fire operator '{}' on receive;", self.info.worker, self.info);
            match self.core.on_receive(&self.inputs, &self.outputs)? {
                ScheduleState::Active(ac) => self.actives.extend(ac),
                _ => (),
            }
        }

        self.fire_on_notify()
    }

    #[inline]
    fn has_capacity(&self) -> bool {
        self.outputs.iter().all(|o| o.borrow().has_capacity())
    }

    pub fn fire_on_notify(&mut self) -> IOResult<()> {
        //3. fire on notify;
        if !self.notifications.is_empty() {
            trace!("### Operator{}: ready notifications {:?}", self.info, self.notifications);
            let mut ready = Vec::new();
            let mut count = self.notifications.len();
            while let Some(notify) = self.notifications.pop_front() {
                match notify {
                    Notification::End(_, ref tag) => {
                        // Sub-scope's control event can't appears in parent environment,
                        // some scope guard should exist and be responsible for transforming;
                        debug_assert!(tag.len() <= self.info.scopes);
                        // Though end signal is received, but some data may active in operator,
                        // they are not really end;
                        if !self.actives.contains(tag) {
                            ready.push(notify);
                        } else {
                            self.notifications.push_back(notify);
                        }
                    },
                    Notification::Iteration(_, _, _) => ready.push(notify),
                    Notification::EOS(ch) => {
                        if self.actives.is_empty() {
                            self.inputs.iter()
                                .find(|&input| {
                                    input.borrow().channel() == ch
                                })
                                .map(|input| {
                                    trace!("### Operator{}: input channel[{}] exhausted", self.info, ch);
                                    input.borrow_mut().exhausted();
                                    Ok(())
                                }).unwrap_or(Err(IOError::ChannelNotFound(ch)))?;
                            ready.push(notify);
                        } else {
                            self.notifications.push_back(notify);
                        }
                    }
                }
                count -= 1;
                if count == 0 {
                    break
                }
            }

            if !ready.is_empty() {
                trace!("### Worker[{}] fire operator '{}' on notify: {:?}", self.info.worker, self.info, ready);
                self.core.on_notify(&mut ready, &self.outputs)?;
            }
        }
        Ok(())
    }

    #[inline]
    pub fn is_finish(&self) -> bool {
        if self.actives.is_empty() {
            if self.inputs.is_empty() {
                return true;
            } else {
                self.inputs.iter()
                    .all(|input| input.borrow().is_exhausted())
            }
        } else {
            return false;
        }
    }

    /// Return true represents that the operator is in active state;
    /// `Active` means that there are work of the operator remain unfinished, caused by some system
    /// consideration, e.g. output capacity bound, stashed.
    #[inline]
    pub fn is_active(&self) -> bool {
        !self.actives.is_empty() || !self.available_inputs.is_empty() || !self.notifications.is_empty()
    }

    #[inline]
    pub fn extract_actives(&self) -> HashMap<Tag, (usize, Option<usize>)> {
        let mut map = HashMap::new();
        for t in self.actives.iter() {
            map.insert(t.clone(), (0, None));
        }
        map
    }

    #[inline]
    pub fn has_notifications(&self) -> bool {
        !self.notifications.is_empty()
    }

    #[inline]
    pub fn output_len(&self) -> usize {
        self.outputs.len()
    }

    #[inline]
    pub fn input_len(&self) -> usize {
        self.inputs.len()
    }

    pub fn close(&mut self) {
        for output in self.outputs.iter() {
            match output.borrow_mut().close() {
                Err(err) => {
                    error!("Close operator failure, caused by {:?}", err);
                },
                _ => ()
            }
        }
    }
}

impl Drop for Operator {
    fn drop(&mut self) {
        self.close();
    }
}

pub trait OperatorState: Send + Default + 'static {}

impl<T: ?Sized + Send + Default + 'static> OperatorState for T {}

pub trait OperatorChain<D1: Data, B> {
    fn add_unary<D2, C, O>(&self, info: OperatorInfo, comm: C, core: O, delta: OutputDelta) -> Stream<D2, B>
        where D2: Data, C: Communicate<D1>, O: OperatorCore + 'static;

    fn add_binary<D2, D3, C1, C2, O>(&self, info: OperatorInfo, other: &Stream<D2, B>,
                                     comm_1: C1, comm_2: C2, core: O) -> Stream<D3, B>
        where D2: Data, D3: Data, C1: Communicate<D1>,
              C2: Communicate<D2>, O: OperatorCore + 'static;
}

impl<D1: Data, B: DataflowBuilder> OperatorChain<D1, B> for Stream<D1, B> {
    fn add_unary<D2, C, O>(&self, info: OperatorInfo, comm: C, core: O, delta: OutputDelta) -> Stream<D2, B>
        where D2: Data, C: Communicate<D1>, O: OperatorCore + 'static {
        // Init the first output port of the newly create operator;
        let port = Port::first(info.index);
        let new_stream = Stream::from(port, self);
        new_stream.set_local(comm.is_local());

        let input = self.connect(port, comm);
        let output = new_stream.get_output().clone();
        let op = OperatorBuilder::new(info)
            .add_input(input)
            .add_output(output, delta)
            .core(core);
        self.add_operator(op);
        new_stream
    }

    fn add_binary<D2, D3, C1, C2, O>(&self, info: OperatorInfo, other: &Stream<D2, B>,
                                     comm_1: C1, comm_2: C2, core: O) -> Stream<D3, B>
        where D2: Data, D3: Data, C1: Communicate<D1>, C2: Communicate<D2>, O: OperatorCore + 'static
    {
        let port = Port::first(info.index);
        let new_stream = Stream::from(port, self);
        new_stream.set_local(comm_1.is_local() && comm_2.is_local());

        let input1 = self.connect(port, comm_1);
        let input2 = other.connect(port, comm_2);
        let output = new_stream.get_output().clone();

        let op = OperatorBuilder::new(info)
            .core(core)
            .add_input(input1)
            .add_input(input2)
            .add_output(output, OutputDelta::None);
        self.add_operator(op);

        new_stream
    }
}

pub struct OperatorWrapper {
    inputs: HashMap<Tag, (usize, Option<usize>)>,
    inner: Operator,
    _permits: Vec<Reserved>
}

impl Debug for OperatorWrapper {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(f, "{:?}", self.inner.info)
    }
}

impl OperatorWrapper {
    pub fn new(op: Operator, inputs: HashMap<Tag, (usize, Option<usize>)>) -> Self {
        OperatorWrapper {
            inputs,
            inner: op,
            _permits: Vec::new()
        }
    }

    pub fn on_notify(op: Operator) -> Self {
        OperatorWrapper {
            inputs: HashMap::new(),
            inner: op,
            _permits: Vec::new()
        }
    }

    pub fn with_permits(op: Operator, inputs: HashMap<Tag, (usize, Option<usize>)>,
                        permits: Vec<Reserved>) -> Self {
        OperatorWrapper {
            inputs,
            inner: op,
            _permits: permits
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.info.name
    }

    pub fn fire(&mut self) -> IOResult<()> {
        if self.inputs.is_empty() {
            self.inner.fire_on_notify()
        } else {
            let mut inputs = ::std::mem::replace(&mut self.inputs, HashMap::new());
            let mut tags = inputs.iter()
                .map(|(t, _)|t.clone()).collect::<Vec<_>>();
            tags.sort_by(|t1, t2| t2.current().cmp(&t1.current()));
            self.inner.stash_input(&tags);

            for tag in tags.drain(..) {
                let (r, capacity) = inputs.remove(&tag).unwrap();
                let t = vec![tag];
                if let Some(c) = capacity {
                    if c > 0 {
                        debug!("fire operator '{}' with capacity {}", self.inner.info(), c);
                        self.inner.stash_pop_input(&t);
                        self.inner.set_outputs_capacity(c);
                        self.inner.fire_on(t[0].clone(), r)?;
                        self.inner.stash_input(&t);
                    }
                } else {
                    debug!("fire operator '{}' without limit", self.inner.info());
                    self.inner.stash_pop_input(&t);
                    self.inner.fire_on(t[0].clone(), r)?;
                    self.inner.stash_input(&t);
                }
            }
            Ok(())
        }
    }

    pub fn is_finish(&self) -> bool {
        self.inner.is_finish()
    }

    pub fn close(&mut self) {
        self.inner.close()
    }

    pub fn take(self) -> Operator {
        self.inner
    }
}
