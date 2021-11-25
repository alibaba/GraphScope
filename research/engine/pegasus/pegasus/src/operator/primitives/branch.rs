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
use crate::api::Branch;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output, OutputProxy};
use crate::communication::{Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::OperatorCore;
use crate::stream::Stream;
use crate::Data;

struct BranchOperator<D, L, R, F> {
    func: F,
    _ph: std::marker::PhantomData<(D, L, R)>,
}

impl<D: Data, L: Data, R: Data, F> BranchOperator<D, L, R, F>
where
    F: FnMut(&mut Input<D>, &Output<L>, &Output<R>) -> Result<(), JobExecError> + Send + 'static,
{
    fn new(func: F) -> Self {
        BranchOperator { func, _ph: std::marker::PhantomData }
    }
}

impl<D: Data, L: Data, R: Data, F> OperatorCore for BranchOperator<D, L, R, F>
where
    F: FnMut(&mut Input<D>, &Output<L>, &Output<R>) -> Result<(), JobExecError> + Send + 'static,
{
    fn on_receive(
        &mut self, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<(), JobExecError> {
        let mut input = new_input_session(&inputs[0]);
        let output0 = new_output(&outputs[0]);
        let output1 = new_output(&outputs[1]);
        (self.func)(&mut input, &output0, &output1)
    }
}

impl<D: Data> Branch<D> for Stream<D> {
    fn branch<L, R, B, F>(self, name: &str, construct: B) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        B: FnOnce(&OperatorInfo) -> F,
        F: FnMut(&mut Input<D>, &Output<L>, &Output<R>) -> Result<(), JobExecError> + Send + 'static,
    {
        self.binary_branch(name, |info| {
            let func = construct(info);
            BranchOperator::new(func)
        })
    }
}
