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

use crate::api::meta::OperatorKind;
use crate::api::LeaveScope;
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputDelta, OutputProxy};
use crate::communication::Pipeline;
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};

struct LeaveOperator<D> {
    //scope_depth     : usize,
    _ph: std::marker::PhantomData<D>,
}

impl<D> LeaveOperator<D> {
    pub fn new() -> Self {
        LeaveOperator { _ph: std::marker::PhantomData }
    }
}

impl<D: Data> OperatorCore for LeaveOperator<D> {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0], tag);
        let mut output = new_output_session::<D>(&outputs[0], tag);
        input.for_each_batch(|dataset| {
            output.forward(dataset)?;
            Ok(())
        })?;
        Ok(FiredState::Idle)
    }
}

impl<D: Data> LeaveScope<D> for Stream<D> {
    fn leave(&self) -> Result<Stream<D>, BuildJobError> {
        if self.scope_depth == 0 {
            return BuildJobError::unsupported("can't leave root scope;");
        }
        let after_leave = self.concat("leave", Pipeline, |meta| {
            meta.set_output_delta(OutputDelta::ToParent(1));
            meta.set_kind(OperatorKind::Map);
            Box::new(LeaveOperator::<D>::new())
        })?;
        Ok(after_leave.leave_scope())
    }

    fn owned_leave(self) -> Result<Stream<D>, BuildJobError> {
        if self.scope_depth == 0 {
            return BuildJobError::unsupported("can't leave root scope;");
        }

        let output = self.outputs().clone();
        if output.output_size() == 0 {
            if output.leave() {
                let after_leave = Stream::inherit(&self, output);
                Ok(after_leave.leave_scope())
            } else {
                BuildJobError::unsupported("can't leave root scope;")
            }
        } else {
            BuildJobError::unsupported(
                "owned leave fail because the parent stream has other downstream",
            )
        }
    }
}
