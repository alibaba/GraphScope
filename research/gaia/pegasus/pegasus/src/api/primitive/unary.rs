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
use crate::api::state::{OperatorState, State};
use crate::communication::{Channel, Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::stream::Stream;
use crate::Data;

/// Used to construct operators with one input and one output;
///
/// An unary operator always consumes data from the input stream, call the user-defined program to
/// produce new data into the output stream;
///
pub trait Unary<I: Data> {
    /// TODO: doc
    ///
    /// # Examples:
    /// TODO
    fn unary<O, C, B, F>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        B: FnOnce(&mut OperatorMeta) -> F,
        F: Fn(&mut Input<I>, &mut Output<O>) -> Result<(), JobExecError> + Send + 'static;

    /// TODO: doc
    ///
    /// # Examples:
    /// TODO
    fn unary_with_notify<O, C, B, F>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        B: FnOnce(&mut OperatorMeta) -> F,
        F: UnaryNotify<I, O>;

    /// TODO: doc
    ///
    /// # Examples:
    /// TODO
    fn unary_with_state<O, C, B, F, S>(
        &self, name: &str, channel: C, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        S: State,
        B: FnOnce(&mut OperatorMeta) -> F,
        F: UnaryState<I, O, S>;
}

/// TODO: doc
pub trait UnaryNotify<I: Data, O: Data>: Send + 'static {
    type NotifyResult: IntoIterator<Item = O>;

    fn on_receive(
        &mut self, input: &mut Input<I>, output: &mut Output<O>,
    ) -> Result<(), JobExecError>;

    fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult;
}

/// TODO: doc
pub trait UnaryState<I: Data, O: Data, S: State>: Send + 'static {
    type NotifyResult: IntoIterator<Item = O>;

    fn on_receive(
        &self, input: &mut Input<I>, output: &mut Output<O>, state: &mut OperatorState<S>,
    ) -> Result<(), JobExecError>;

    fn on_notify(&self, state: S) -> Self::NotifyResult;
}

mod lazy {
    use super::*;
    use crate::api::function::FlatMapFunction;

    pub trait LazyUnary<I: Data> {
        fn lazy_unary<O, C, B, F>(
            &self, name: &str, channel: C, construct: B,
        ) -> Result<Stream<O>, BuildJobError>
        where
            O: Data,
            C: Into<Channel<I>>,
            B: FnOnce(&OperatorMeta) -> F,
            F: FlatMapFunction<I, O>;
    }
}

pub use lazy::LazyUnary;
