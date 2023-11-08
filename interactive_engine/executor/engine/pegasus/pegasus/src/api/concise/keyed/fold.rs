//
//! Copyright 2022 Alibaba Group Holding Limited.
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
use std::fmt::Debug;

use crate::api::function::FnResult;
use crate::api::Key;
use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

pub trait FoldByKey<K: Data + Key, V: Data> {
    /// Analogous to [`fold()`] but folding the data according to the key part of the input data.
    ///
    /// [`fold()`]: crate::api::fold::Fold::fold()
    fn fold_by_key<I, B, F>(self, init: I, builder: B) -> Result<SingleItem<HashMap<K, I>>, BuildJobError>
    where
        I: Data,
        F: FnMut(I, V) -> FnResult<I> + Send + 'static,
        B: Fn() -> F + Send + 'static;

    fn fold_partition_by_key<I, B, F>(
        self, init: I, builder: B,
    ) -> Result<SingleItem<HashMap<K, I>>, BuildJobError>
    where
        I: Clone + Send + Sync + Debug + 'static,
        F: FnMut(I, V) -> FnResult<I> + Send + 'static,
        B: Fn() -> F + Send + 'static;
}
