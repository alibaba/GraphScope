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
use crate::api::function::{KeyFunction, Keyed, Pair, Partition};
use crate::api::Range;
use crate::stream::Stream;
use crate::{BuildJobError, Data};
use pegasus_common::collections::MapFactory;
use pegasus_common::downcast::AsAny;
use std::collections::HashMap;
use std::hash::Hash;

pub trait Group<D: Data + Keyed> {
    fn group_by(
        &self, range: Range,
    ) -> Result<Stream<HashMap<D::Key, Vec<D::Value>>>, BuildJobError>
    where
        D::Key: Data + Hash + Eq + Partition,
        D::Value: Data + AsAny;

    fn group_with_map<F>(
        &self, range: Range, map_factory: F,
    ) -> Result<Stream<F::Target>, BuildJobError>
    where
        D::Key: Send + Eq + Partition,
        D::Value: Send,
        F: MapFactory<D::Key, D::Value> + 'static,
        F::Target: Data;

    fn group_with_accum<A>(
        &self, range: Range, accum_factory: A,
    ) -> Result<Stream<HashMap<D::Key, A::Target>>, BuildJobError>
    where
        A: AccumFactory<D::Value> + 'static,
        A::Target: Data + 'static,
        D::Key: Data + Hash + Eq + Partition;
}

pub trait KeyBy<D: Data> {
    fn key_by<F>(&self, key_selector: F) -> Result<Stream<Pair<F::Key, D>>, BuildJobError>
    where
        F::Key: Data,
        F: KeyFunction<D>;
}
