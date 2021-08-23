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

use std::cmp::Ordering;

use crate::stream::Stream;
use crate::{BuildJobError, Data};

/// Sort the input data stream.
pub trait Sort<D: Data + Ord> {
    /// Sort the input data stream. In order to do so, the data must be [`Ord`].
    /// Otherwise, please check [`SortBy`]
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`SortBy`]: crate::api::sort::SortBy
    ///
    /// # Example
    /// ```
    /// #     use pegasus::JobConf;
    /// #     use pegasus::api::{Sink, Sort, Collect};
    /// #     let conf = JobConf::new("sort_example");
    ///       let mut results = pegasus::run(conf, || {
    ///         move |input, output| {
    ///                 input.input_from(vec![5_u32, 8, 1, 5, 9].into_iter())?
    ///                      .sort()?
    ///                      .collect::<Vec<u32>>()?
    ///                      .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///
    ///     assert_eq!(results.next().unwrap().unwrap(),[1, 5, 5, 8, 9]);
    /// ```
    fn sort(self) -> Result<Stream<D>, BuildJobError>;
}

/// Sort the input data stream via a user-defined comparator.
pub trait SortBy<D: Data> {
    /// Sort the input data stream via a user-defined comparator `cmp`.
    /// # Example
    /// ```
    /// #     use pegasus::JobConf;
    /// #     use pegasus::api::{Sink, SortBy, Collect};
    /// #     let conf = JobConf::new("sort_by_example");
    ///       let mut results = pegasus::run(conf, || {
    ///         move |input, output| {
    ///                 input.input_from(vec![5_u32, 8, 1, 5, 9].into_iter())?
    ///                      .sort_by(|x, y| x.cmp(y).reverse())?
    ///                      .collect::<Vec<u32>>()?
    ///                      .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///
    ///     assert_eq!(results.next().unwrap().unwrap(),[9, 8, 5, 5, 1]);
    /// ```
    fn sort_by<F>(self, cmp: F) -> Result<Stream<D>, BuildJobError>
    where
        F: Fn(&D, &D) -> Ordering + Send + 'static;
}
