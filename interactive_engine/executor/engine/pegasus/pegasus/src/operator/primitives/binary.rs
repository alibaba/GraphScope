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

use std::marker::PhantomData;

use crate::api::meta::OperatorInfo;
use crate::api::Binary;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::communication::{Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::OperatorCore;
use crate::stream::Stream;
use crate::Data;

struct BinaryOperator<L, R, O, F> {
    func: F,
    _ph: PhantomData<(L, R, O)>,
}

impl<L, R, O, F> BinaryOperator<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: FnMut(&mut Input<L>, &mut Input<R>, &Output<O>) -> Result<(), JobExecError> + Send + 'static,
{
    fn new(func: F) -> Self {
        BinaryOperator { func, _ph: std::marker::PhantomData }
    }
}

impl<L, R, O, F> OperatorCore for BinaryOperator<L, R, O, F>
where
    L: Data,
    R: Data,
    O: Data,
    F: FnMut(&mut Input<L>, &mut Input<R>, &Output<O>) -> Result<(), JobExecError> + Send + 'static,
{
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut left = new_input_session::<L>(&inputs[0]);
        let mut right = new_input_session::<R>(&inputs[1]);
        let output = new_output::<O>(&outputs[0]);
        (self.func)(&mut left, &mut right, &output)
    }
}

impl<L: Data> Binary<L> for Stream<L> {
    fn binary<R, O, B, F>(
        self, name: &str, other: Stream<R>, construct: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        B: FnOnce(&OperatorInfo) -> F,
        F: FnMut(&mut Input<L>, &mut Input<R>, &Output<O>) -> Result<(), JobExecError> + Send + 'static,
    {
        self.union_transform(name, other, |info| {
            let func = construct(info);
            BinaryOperator::new(func)
        })
    }
}
