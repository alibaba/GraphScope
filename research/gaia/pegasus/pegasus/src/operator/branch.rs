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
use crate::api::{Branch, Condition, IntoBranch};
use crate::communication::input::{new_input_session, InputProxy};
use crate::communication::output::{new_output_session, OutputProxy};
use crate::communication::Pipeline;
use crate::errors::{BuildJobError, JobExecError};
use crate::operator::{FiredState, OperatorCore};
use crate::stream::Stream;
use crate::{Data, Tag};

struct BranchOperator<D, F> {
    condition: F,
    _ph: std::marker::PhantomData<D>,
}

impl<D: Data, F: Condition<D> + 'static> OperatorCore for BranchOperator<D, F> {
    fn on_receive(
        &mut self, tag: &Tag, inputs: &[Box<dyn InputProxy>], outputs: &[Box<dyn OutputProxy>],
    ) -> Result<FiredState, JobExecError> {
        let mut input = new_input_session::<D>(&inputs[0], tag);
        let mut left = new_output_session::<D>(&outputs[0], tag);
        let mut right = new_output_session::<D>(&outputs[1], tag);
        input.for_each_batch(|dataset| {
            for item in dataset.drain(..) {
                match self.condition.predicate(&item) {
                    Branch::Left => {
                        left.give(item)?;
                    }
                    Branch::Right => {
                        right.give(item)?;
                    }
                }
            }
            Ok(())
        })?;
        Ok(FiredState::Idle)
    }
}

impl<D: Data> IntoBranch<D> for Stream<D> {
    fn branch<F: Condition<D> + 'static>(
        &self, name: &str, func: F,
    ) -> Result<(Stream<D>, Stream<D>), BuildJobError> {
        Stream::make_branch(self, name, Pipeline, |meta| {
            meta.set_kind(OperatorKind::Map);
            Box::new(BranchOperator { condition: func, _ph: std::marker::PhantomData })
        })
    }
}
