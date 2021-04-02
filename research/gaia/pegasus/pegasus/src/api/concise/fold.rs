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
use crate::api::Range;
use crate::communication::Channel;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

pub trait Fold<I: Data> {
    fn fold<O, C, F>(&self, seed: O, channel: C, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        C: Into<Channel<I>>,
        F: Fn(&mut O, I) + Send + 'static;

    fn fold_with_accum<A>(
        &self, range: Range, accum_factory: A,
    ) -> Result<Stream<A::Target>, BuildJobError>
    where
        A: AccumFactory<I> + 'static,
        A::Target: Data;
}
