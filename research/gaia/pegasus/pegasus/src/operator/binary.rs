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
use crate::api::state::{State, StateMap};
use crate::api::{Binary, BinaryInput, BinaryNotification, BinaryNotify, BinaryState};
use crate::communication::input::InputProxy;
use crate::communication::output::{new_output_session, OutputProxy};
use crate::communication::{Channel, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};
use std::collections::HashMap;
use std::marker::PhantomData;

bitflags! {
    #[derive(Default)]
    struct NotifyState : u32 {
        const LEFT  = 0b00000001;
        const RIGHT = 0b00000010;
        const BOTH  = Self::LEFT.bits | Self::RIGHT.bits;
    }
}

const BINARY_NOTIFIES: [NotifyState; 2] = [NotifyState::LEFT, NotifyState::RIGHT];

struct BinaryOperator<L, R, O, F> {
    scope_depth: usize,
    func: F,
    notifications: HashMap<Tag, NotifyState>,
    _ph: PhantomData<(L, R, O)>,
}

impl<L, R, O, F> BinaryOperator<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: FnMut(&mut BinaryInput<L, R>, &mut Output<O>) -> Result<(), JobExecError> + Send + 'static,
{
    pub fn new(scope_depth: usize, func: F) -> Self {
        BinaryOperator { scope_depth, func, notifications: HashMap::new(), _ph: PhantomData }
    }
}

impl<L, R, O, F> OperatorCore for BinaryOperator<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: FnMut(&mut BinaryInput<L, R>, &mut Output<O>) -> Result<(), JobExecError> + Send + 'static,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut output = new_output_session::<O>(&outputs[0], tag);
        let mut input = BinaryInput::new(tag, inputs);
        (self.func)(&mut input, &mut output)?;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        guard_binary_notifications(self.scope_depth, outputs, n, &mut self.notifications);
        self.notifications.retain(|_, v| *v != NotifyState::BOTH);
        Ok(())
    }
}

pub struct BinaryNotifyOperator<L, R, O, F> {
    pub scope_depth: usize,
    func: F,
    notifications: HashMap<Tag, NotifyState>,
    subscribers: Vec<StateMap<()>>,
    _ph: PhantomData<(L, R, O)>,
}

impl<L, R, O, F> BinaryNotifyOperator<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: BinaryNotify<L, R, O>,
{
    pub fn new(meta: &OperatorMeta, func: F) -> Self {
        let subscribers = vec![StateMap::new(meta), StateMap::new(meta)];
        BinaryNotifyOperator {
            scope_depth: meta.scope_depth,
            func,
            notifications: HashMap::new(),
            subscribers,
            _ph: PhantomData,
        }
    }
}

impl<L, R, O, F> OperatorCore for BinaryNotifyOperator<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: BinaryNotify<L, R, O>,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut output = new_output_session::<O>(&outputs[0], tag);
        let mut input = BinaryInput::new(tag, inputs);
        let (left, right) = self.subscribers.split_at_mut(1);
        input.set_left_subscriber(NotifySubscriber::new(&mut left[0]));
        input.set_right_subscriber(NotifySubscriber::new(&mut right[0]));
        self.func.on_receive(&mut input, &mut output)?;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        self.subscribers[n.port].notify(&n);

        for (t, _) in self.subscribers[n.port].extract_notified().drain(..) {
            let notify = if n.port == 0 {
                BinaryNotification::Left(t.clone())
            } else {
                BinaryNotification::Right(t.clone())
            };
            let result = self.func.on_notify(notify);
            new_output_session::<O>(&outputs[0], &t).give_entire_iter(result)?;
        }

        guard_binary_notifications(self.scope_depth, outputs, n, &mut self.notifications);
        self.notifications.retain(|_, v| *v != NotifyState::BOTH);
        Ok(())
    }
}

pub struct BinaryStateOperator<L, R, O, F, S> {
    scope_depth: usize,
    func: F,
    states: StateMap<S>,
    notifications: HashMap<Tag, NotifyState>,
    ready_notify: Vec<Tag>,
    _ph: PhantomData<(L, R, O)>,
}

impl<L, R, O, F, S> BinaryStateOperator<L, R, O, F, S>
where
    L: Data,
    R: Data,
    O: Data,
    S: State,
    F: BinaryState<L, R, O, S>,
{
    pub fn new(meta: &OperatorMeta, func: F) -> Self {
        BinaryStateOperator {
            scope_depth: meta.scope_depth,
            func,
            states: StateMap::new(meta),
            notifications: HashMap::new(),
            ready_notify: Vec::new(),
            _ph: PhantomData,
        }
    }
}

impl<L, R, O, F, S> OperatorCore for BinaryStateOperator<L, R, O, F, S>
where
    L: Data,
    R: Data,
    O: Data,
    S: State,
    F: BinaryState<L, R, O, S>,
{
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = BinaryInput::new(tag, inputs);
        let mut output = new_output_session::<O>(&outputs[0], tag);
        let state = self.states.entry(tag.clone()).or_insert_with(|| S::default());
        self.func.on_receive(&mut input, &mut output, state)?;
        Ok(FiredState::Idle)
    }

    fn on_notify(
        &mut self, n: Notification, outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        guard_binary_notifications(self.scope_depth, outputs, n, &mut self.notifications);
        let mut vec = std::mem::replace(&mut self.ready_notify, vec![]);
        self.notifications.retain(|k, v| {
            let retain = *v != NotifyState::BOTH;
            if !retain {
                vec.push(k.clone());
            }
            retain
        });

        for n in vec.drain(..) {
            self.states.notify(&n);
            for (t, s) in self.states.extract_notified().drain(..) {
                let result = self.func.on_notify(s);
                new_output_session::<O>(&outputs[0], &t).give_entire_iter(result)?;
            }
        }
        self.ready_notify = vec;
        Ok(())
    }
}

#[inline]
fn guard_binary_notifications(
    scope_depth: usize, outputs: &[Box<dyn OutputProxy>], n: Notification,
    notifies: &mut HashMap<Tag, NotifyState>,
) {
    let (port, sig) = n.take();
    trace_worker!("receive {:?} on port {:?}", sig, port);
    {
        let n_state = notifies.entry(sig.clone()).or_insert(Default::default());
        n_state.insert(BINARY_NOTIFIES[port]);
        if *n_state == NotifyState::BOTH {
            trace_worker!("receive both notifications of scope {:?}", sig);
            outputs[0].drop_retain(&sig);
        } else {
            outputs[0].retain(&sig);
        }
    }

    if sig.len() != scope_depth {
        let notify = BINARY_NOTIFIES[port];
        for (k, v) in notifies.iter_mut() {
            v.insert(notify);
            if *v == NotifyState::BOTH {
                outputs[0].drop_retain(k);
            }
        }
    }
}

impl<L: Data> Binary<L> for Stream<L> {
    fn binary<R, O, CL, CR, B, F>(
        &self, name: &str, other: &Stream<R>, ch_l: CL, ch_r: CR, builder: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        CL: Into<Channel<L>>,
        CR: Into<Channel<R>>,
        B: FnOnce(&OperatorMeta) -> F,
        F: FnMut(&mut BinaryInput<L, R>, &mut Output<O>) -> Result<(), JobExecError>
            + Send
            + 'static,
    {
        self.join(name, ch_l, other, ch_r, |meta| {
            meta.enable_notify();
            let func = builder(meta);
            Box::new(BinaryOperator::new(meta.scope_depth, func))
        })
    }

    fn binary_notify<R, O, CL, CR, B, F>(
        &self, name: &str, other: &Stream<R>, ch_l: CL, ch_r: CR, builder: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        CL: Into<Channel<L>>,
        CR: Into<Channel<R>>,
        B: FnOnce(&OperatorMeta) -> F,
        F: BinaryNotify<L, R, O>,
    {
        self.join(name, ch_l, other, ch_r, |meta| {
            meta.enable_notify();
            let func = builder(meta);
            Box::new(BinaryNotifyOperator::new(meta, func))
        })
    }

    fn binary_state<R, O, CL, CR, B, F, S>(
        &self, name: &str, other: &Stream<R>, ch_l: CL, ch_r: CR, builder: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        S: State,
        CL: Into<Channel<L>>,
        CR: Into<Channel<R>>,
        B: FnOnce(&OperatorMeta) -> F,
        F: BinaryState<L, R, O, S>,
    {
        self.join(name, ch_l, other, ch_r, |meta| {
            meta.enable_notify();
            let func = builder(meta);
            Box::new(BinaryStateOperator::new(meta, func))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_state_test() {
        let mut state = NotifyState::default();
        state.insert(NotifyState::LEFT);
        state.insert(NotifyState::RIGHT);
        assert_eq!(state, NotifyState::BOTH);
    }
}
