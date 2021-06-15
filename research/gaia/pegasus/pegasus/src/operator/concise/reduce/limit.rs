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

use crate::api::concise::reduce::Range;
use crate::api::meta::OperatorKind;
use crate::api::state::OperatorState;
use crate::api::{Limit, Unary, UnaryState};
use crate::communication::{Aggregate, Input, Output, Pipeline};
use crate::errors::JobExecError;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

#[derive(Copy, Clone, Debug)]
struct LimitHandle {
    count: u64,
}

impl LimitHandle {
    pub fn new(count: u64) -> Self {
        LimitHandle { count }
    }
}

impl<D: Data> UnaryState<D, D, u64> for LimitHandle {
    type NotifyResult = Vec<D>;

    fn on_receive(
        &self, input: &mut Input<D>, output: &mut Output<D>, state: &mut OperatorState<u64>,
    ) -> Result<(), JobExecError> {
        input.for_each_batch(|dataset| {
            if **state < self.count {
                for datum in dataset.drain(..) {
                    **state += 1;
                    output.give(datum)?;
                    if **state >= self.count {
                        state.set_final();
                        break;
                    }
                }
            }
            Ok(())
        })
    }

    #[inline(always)]
    fn on_notify(&self, _: u64) -> Self::NotifyResult {
        vec![]
    }
}

impl<D: Data> Limit<D> for Stream<D> {
    fn limit(&self, range: Range, size: u32) -> Result<Stream<D>, BuildJobError> {
        let limit = LimitHandle::new(size as u64);
        match range {
            Range::Global => self
                .unary_with_state("limit_local", Pipeline, |meta| {
                    meta.set_kind(OperatorKind::Clip);
                    limit
                })?
                .unary_with_state("limit", Aggregate(0), |meta| {
                    meta.set_kind(OperatorKind::Clip);
                    limit
                }),
            Range::Local => self.unary_with_state("limit", Pipeline, |meta| {
                meta.set_kind(OperatorKind::Clip);
                limit
            }),
        }
    }
}
