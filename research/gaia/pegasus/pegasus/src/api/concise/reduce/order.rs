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

use crate::api::function::CompareFunction;
use crate::api::Range;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OrderDirect {
    Asc,
    Desc,
}

pub trait Order<D: Data + Ord> {
    fn sort(&self, range: Range, order: OrderDirect) -> Result<Stream<D>, BuildJobError>;

    fn top(&self, limit: u32, range: Range, order: OrderDirect)
        -> Result<Stream<D>, BuildJobError>;
}

pub trait OrderBy<D: Data> {
    fn sort_by<F>(&self, range: Range, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: CompareFunction<D> + 'static;

    fn top_by<F>(&self, limit: u32, range: Range, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: CompareFunction<D> + 'static;
}
