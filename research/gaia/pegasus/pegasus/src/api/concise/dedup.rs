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

use crate::api::Range;
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::collections::{CollectionFactory, Set};

pub trait Dedup<D: Data + Eq> {
    fn dedup<S>(&self, range: Range) -> Result<Stream<D>, BuildJobError>
    where
        S: Set<D> + Default + 'static;

    fn dedup_with<S>(&self, range: Range, factory: S) -> Result<Stream<D>, BuildJobError>
    where
        S: CollectionFactory<D> + 'static,
        S::Target: Set<D>;
}
