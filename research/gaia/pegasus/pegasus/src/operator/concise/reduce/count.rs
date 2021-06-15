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
use crate::api::{Count, Fold};
use crate::communication::{Aggregate, Pipeline};
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

impl<D: Data> Count<D> for Stream<D> {
    fn count(&self, range: Range) -> Result<Stream<u64>, BuildJobError> {
        match range {
            Range::Local => self.fold(0u64, Pipeline, |s, _| *s += 1),
            Range::Global => {
                self.fold(0u64, Pipeline, |s, _| *s += 1)?.fold(0u64, Aggregate(0), |s, u| *s += u)
            }
        }
    }
}
