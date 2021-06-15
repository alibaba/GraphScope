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

use crate::api::function::RouteFunction;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

pub trait Exchange<D: Data> {
    fn exchange<R>(&self, routing: R) -> Result<Stream<D>, BuildJobError>
    where
        R: RouteFunction<D>;

    fn exchange_with_fn<R>(&self, func: R) -> Result<Stream<D>, BuildJobError>
    where
        R: Fn(&D) -> u64 + Send + 'static;
}
