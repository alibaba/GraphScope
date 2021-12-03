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
use crate::api::Unary;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::communication::{Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::OperatorCore;
use crate::stream::Stream;
use crate::Data;

struct UnaryOperator<I, O, F> {
    func: F,
    _ph: std::marker::PhantomData<(I, O)>,
}

impl<I, O, F> UnaryOperator<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(&mut Input<I>, &Output<O>) -> Result<(), JobExecError> + Send + 'static,
{
    fn new(func: F) -> Self {
        UnaryOperator { func, _ph: std::marker::PhantomData }
    }
}

impl<I, O, F> OperatorCore for UnaryOperator<I, O, F>
where
    I: Data,
    O: Data,
    F: FnMut(&mut Input<I>, &Output<O>) -> Result<(), JobExecError> + Send + 'static,
{
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session::<I>(&inputs[0]);
        let output = new_output::<O>(&outputs[0]);
        (self.func)(&mut input, &output)
    }
}

impl<I: Data> Unary<I> for Stream<I> {
    fn unary<O, B, F>(self, name: &str, construct: B) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        B: FnOnce(&OperatorInfo) -> F,
        F: FnMut(&mut Input<I>, &Output<O>) -> Result<(), JobExecError> + Send + 'static,
    {
        self.transform(name, |info| {
            let func = construct(info);
            Box::new(UnaryOperator::new(func))
        })
    }
}
