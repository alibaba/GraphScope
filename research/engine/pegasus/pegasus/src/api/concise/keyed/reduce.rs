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

use std::collections::HashMap;

use crate::api::function::FnResult;
use crate::api::Key;
use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

pub trait ReduceByKey<K: Key + Data, V: Data> {
    /// Analogous to [`reduce()`] but reducing the data according to the key part of the input data.
    ///
    /// [`reduce()`]: crate::api::reduce::Reduce::reduce()
    fn reduce_by_key<B, F>(self, builder: B) -> Result<SingleItem<HashMap<K, V>>, BuildJobError>
    where
        F: FnMut(V, V) -> FnResult<V> + Send + 'static,
        B: Fn() -> F + Send + 'static;
}
