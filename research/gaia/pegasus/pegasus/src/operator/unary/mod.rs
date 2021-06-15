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
use crate::api::notify::{Notification, NotifySubscriber};
use crate::api::state::{OperatorState, State, StateMap};
use crate::api::{Unary, UnaryNotify, UnaryState};
use crate::communication::input::{new_input_session, InputProxy, InputSession};
use crate::communication::output::{new_output_session, OutputProxy, OutputSession};
use crate::communication::{Channel, Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};

struct UnaryOperator<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> UnaryOperator<I, O, F> {
    pub fn new(func: F) -> Self {
        UnaryOperator { func, _ph: std::marker::PhantomData }
    }
}

impl<I, O, F> OperatorCore for UnaryOperator<I, O, F>
where
    I: Data,
    O: Data,
    F: Fn(&mut InputSession<I>, &mut OutputSession<O>) -> Result<(), JobExecError> + Send,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<I>(&inputs[0], tag);
        let mut output = new_output_session::<O>(&outputs[0], tag);
        (self.func)(&mut input, &mut output)?;
        Ok(FiredState::Idle)
    }
}

struct UnaryNotifyOperator<I, O, F> {
    func: F,
    state: StateMap<()>,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> UnaryNotifyOperator<I, O, F> {
    pub fn new(meta: &OperatorMeta, func: F) -> Self {
        UnaryNotifyOperator { func, state: StateMap::new(meta), _ph: std::marker::PhantomData }
    }
}

impl<I, O, F> OperatorCore for UnaryNotifyOperator<I, O, F>
where
    I: Data,
    O: Data,
    F: UnaryNotify<I, O>,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut output = new_output_session::<O>(&outputs[0], tag);
        let mut state = std::mem::replace(&mut self.state, StateMap::default());
        let subscribe = NotifySubscriber::new(&mut state);
        {
            let mut input = new_input_session::<I>(&inputs[0], tag);
            input.set_notify_sub(subscribe);
            self.func.on_receive(&mut input, &mut output)?;
        }
        self.state = state;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        self.state.notify(&n);
        for (n_x, _) in self.state.extract_notified().drain(..) {
            let notification = Notification::new(n.port, n_x);
            let result = self.func.on_notify(&notification);
            let mut session = new_output_session::<O>(&outputs[0], &notification.tag);
            session.give_entire_iter(result)?;
        }
        Ok(())
    }
}

struct UnaryStateOperator<I, O, S: State, F> {
    name: String,
    func: F,
    state: StateMap<OperatorState<S>>,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, S: State, F> UnaryStateOperator<I, O, S, F> {
    pub fn new(meta: &OperatorMeta, func: F) -> Self {
        let name = format!("{}_{}", meta.name, meta.index);
        UnaryStateOperator { name, func, state: StateMap::new(meta), _ph: std::marker::PhantomData }
    }
}

impl<I, O, S, F> OperatorCore for UnaryStateOperator<I, O, S, F>
where
    I: Data,
    O: Data,
    S: State,
    F: UnaryState<I, O, S>,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<I>(&inputs[0], tag);
        let mut output = new_output_session::<O>(&outputs[0], tag);
        let state = self.state.entry(tag).or_insert_with(|| OperatorState::<S>::default());
        self.func.on_receive(&mut input, &mut output, state)?;
        if state.is_reach_final() {
            debug_worker!("trigger cancel on scope {:?} by operator {:?}", tag, self.name);
            input.cancel_scope();
        }
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        self.state.notify(&n);
        for (n, state) in self.state.extract_notified().drain(..) {
            let result = self.func.on_notify(state.detach());
            let mut session = new_output_session(&outputs[0], &n);
            session.give_entire_iter(result)?;
        }
        Ok(())
    }
}

impl<I: Data> Unary<I> for Stream<I> {
    fn unary<O, C, B, F>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        B: FnOnce(&mut OperatorMeta) -> F,
        F: Fn(&mut Input<I>, &mut Output<O>) -> Result<(), JobExecError> + Send + 'static,
    {
        self.concat(name, channel, |meta| {
            let func = construct(meta);
            Box::new(UnaryOperator::new(func))
        })
    }

    fn unary_with_notify<O, C, B, F>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        B: FnOnce(&mut OperatorMeta) -> F,
        F: UnaryNotify<I, O>,
    {
        self.concat(name, channel, |meta| {
            meta.enable_notify();
            let func = construct(meta);
            Box::new(UnaryNotifyOperator::new(meta, func))
        })
    }

    fn unary_with_state<O, C, B, F, S>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        S: State,
        B: FnOnce(&mut OperatorMeta) -> F,
        F: UnaryState<I, O, S>,
    {
        self.concat(name, channel, |meta| {
            meta.enable_notify();
            let func = construct(meta);
            Box::new(UnaryStateOperator::new(meta, func))
        })
    }
}

mod lazy;
