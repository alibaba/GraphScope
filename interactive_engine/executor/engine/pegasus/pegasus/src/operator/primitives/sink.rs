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

use std::fmt::Debug;

use crate::api::primitive::sink::Sink;
use crate::api::FromStream;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::OutputProxy;
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::OperatorCore;
use crate::stream::{Single, SingleItem, Stream};
use crate::Data;

struct SinkOperator<D, C> {
    collector: C,
    _ph: std::marker::PhantomData<D>,
}

impl<D, C> SinkOperator<D, C> {
    fn new(collector: C) -> Self {
        SinkOperator { collector, _ph: std::marker::PhantomData }
    }
}

impl<D, C> OperatorCore for SinkOperator<D, C>
where
    D: Data,
    C: FromStream<D>,
{
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], _: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0]);
        input.for_each_batch(|dataset| {
            for d in dataset.drain() {
                self.collector.on_next(d)?;
            }
            Ok(())
        })
    }
}

struct SinkSingleOperator<D, C> {
    sender: C,
    _ph: std::marker::PhantomData<D>,
}

impl<D, C> SinkSingleOperator<D, C> {
    fn new(sender: C) -> Self {
        SinkSingleOperator { sender, _ph: std::marker::PhantomData }
    }
}

impl<D, C> OperatorCore for SinkSingleOperator<D, C>
where
    D: Debug + Send + Sync + 'static,
    C: FromStream<D>,
{
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], _: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session::<Single<D>>(&inputs[0]);
        input.for_each_batch(|dataset| {
            for d in dataset.drain() {
                self.sender.on_next(d.0)?;
            }
            Ok(())
        })
    }
}

impl<D: Data> Sink<D> for Stream<D> {
    fn sink_into<C: FromStream<D>>(self, collector: C) -> Result<(), BuildJobError> {
        self.sink_by("sink_stream", |_| SinkOperator::new(collector))
    }
}

impl<D: Debug + Send + Sync + 'static> Sink<D> for SingleItem<D> {
    fn sink_into<C: FromStream<D>>(self, collector: C) -> Result<(), BuildJobError> {
        self.inner
            .sink_by("sink_single", |_| SinkSingleOperator::new(collector))
    }
}
