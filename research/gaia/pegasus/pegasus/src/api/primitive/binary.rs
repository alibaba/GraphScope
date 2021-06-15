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
use crate::api::notify::NotifySubscriber;
use crate::api::state::State;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::{Channel, Output};
use crate::data::DataSet;
use crate::errors::{BuildJobError, JobExecError};
use crate::stream::Stream;
use crate::{Data, Tag};

/// Construct binary operator, which consumes two input streams, and produces data into one output stream;
pub trait Binary<L: Data> {
    fn binary<R, O, CL, CR, B, F>(
        &self, name: &str, other: &Stream<R>, ch_l: CL, ch_r: CR, builder: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        CL: Into<Channel<L>>,
        CR: Into<Channel<R>>,
        B: FnOnce(&OperatorMeta) -> F,
        F: Fn(&mut BinaryInput<L, R>, &mut Output<O>) -> Result<(), JobExecError> + Send + 'static;

    fn binary_notify<R, O, CL, CR, B, F>(
        &self, name: &str, other: &Stream<R>, ch_l: CL, ch_r: CR, builder: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        CL: Into<Channel<L>>,
        CR: Into<Channel<R>>,
        B: FnOnce(&OperatorMeta) -> F,
        F: BinaryNotify<L, R, O>;

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
        F: BinaryState<L, R, O, S>;
}

/// A composite input stream proxy consist of two input streams;
pub struct BinaryInput<'a, L: Data, R: Data> {
    tag: &'a Tag,
    inputs: &'a [Box<dyn InputProxy>],
    left_subscriber: Option<NotifySubscriber<'a>>,
    right_subscriber: Option<NotifySubscriber<'a>>,
    _ph: std::marker::PhantomData<(L, R)>,
}

impl<'a, L: Data, R: Data> BinaryInput<'a, L, R> {
    pub fn new(tag: &'a Tag, inputs: &'a [Box<dyn InputProxy>]) -> Self {
        BinaryInput {
            tag,
            inputs,
            left_subscriber: None,
            right_subscriber: None,
            _ph: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn tag(&self) -> &Tag {
        self.tag
    }

    #[inline]
    pub(crate) fn set_left_subscriber(&mut self, subscriber: NotifySubscriber<'a>) {
        self.left_subscriber = Some(subscriber)
    }

    #[inline]
    pub(crate) fn set_right_subscriber(&mut self, subscriber: NotifySubscriber<'a>) {
        self.right_subscriber = Some(subscriber)
    }

    #[inline]
    pub fn left_for_each<F>(&mut self, func: F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut DataSet<L>) -> Result<(), JobExecError>,
    {
        let mut session = new_input_session::<L>(&self.inputs[0], self.tag);
        session.for_each_batch(func)
    }

    #[inline]
    pub fn right_for_each<F>(&mut self, func: F) -> Result<(), JobExecError>
    where
        F: FnMut(&mut DataSet<R>) -> Result<(), JobExecError>,
    {
        let mut session = new_input_session::<R>(&self.inputs[1], self.tag);
        session.for_each_batch(func)
    }

    pub fn subscribe_left_notify(&mut self) {
        let tag = self.tag;
        if let Some(ref mut n) = self.left_subscriber {
            n.subscribe(tag);
        }
    }

    pub fn subscribe_right_notify(&mut self) {
        let tag = self.tag;
        if let Some(ref mut n) = self.right_subscriber {
            n.subscribe(tag);
        }
    }

    #[inline]
    pub fn get_left_subscriber(&mut self) -> Option<&mut NotifySubscriber<'a>> {
        self.left_subscriber.as_mut()
    }

    #[inline]
    pub fn get_right_subscriber(&mut self) -> Option<&mut NotifySubscriber<'a>> {
        self.right_subscriber.as_mut()
    }
}

pub trait BinaryNotify<L: Data, R: Data, O: Data>: Send + 'static {
    type NotifyResult: IntoIterator<Item = O>;

    fn on_receive(
        &mut self, input: &mut BinaryInput<L, R>, output: &mut Output<O>,
    ) -> Result<(), JobExecError>;

    fn on_notify(&mut self, n: BinaryNotification) -> Self::NotifyResult;
}

pub trait BinaryState<L: Data, R: Data, O: Data, S: State>: Send + 'static {
    type NotifyResult: IntoIterator<Item = O>;

    fn on_receive(
        &self, input: &mut BinaryInput<L, R>, output: &mut Output<O>, state: &mut S,
    ) -> Result<(), JobExecError>;

    fn on_notify(&self, state: S) -> Self::NotifyResult;
}

pub enum BinaryNotification {
    Left(Tag),
    Right(Tag),
}
