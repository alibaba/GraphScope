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
use crate::communication::{Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::stream::Stream;
use crate::Data;

pub trait Branch<D: Data> {
    fn branch<L, R, B, F>(self, name: &str, construct: B) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        B: FnOnce(&OperatorInfo) -> F,
        F: FnMut(&mut Input<D>, &Output<L>, &Output<R>) -> Result<(), JobExecError> + Send + 'static;
}
