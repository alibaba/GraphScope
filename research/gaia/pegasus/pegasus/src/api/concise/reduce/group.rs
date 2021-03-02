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

use crate::api::accum::AccumFactory;
use crate::api::function::{KeyFunction, Pair};
use crate::api::Range;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

pub trait Group<D: Data> {
    fn group_by_with_accum<F, A, O>(
        &self, range: Range, key: F, accum_gen: A,
    ) -> Result<Stream<Pair<F::Target, O>>, BuildJobError>
    where
        O: Data,
        F: KeyFunction<D>,
        A: AccumFactory<D, O> + 'static,
        A::Target: 'static;

    fn group_by<F>(
        &self, range: Range, key: F,
    ) -> Result<Stream<Pair<F::Target, Vec<D>>>, BuildJobError>
    where
        F: KeyFunction<D>;
}
