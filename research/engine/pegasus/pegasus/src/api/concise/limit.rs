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

use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;
use std::cmp::Ordering;

/// Produce certain number of data in the output stream
pub trait Limit<D: Data> {
    /// Given a `size` argument, `limit()` produces an output stream that contains up to
    /// `size` number of data. `limit()` is tightly related to an optimization strategy we propose
    /// called early-stop, which aims to terminate the useless computation as early as possible.
    /// It is obvious that all precursor computation is wasteful if there already returns
    /// `size` results.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Limit, Collect};
    ///
    ///   # let mut conf = JobConf::new("reduce_example");
    ///   # conf.plan_print = false;
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 // summing the input data
    ///                 .limit(2)?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///       .expect("build job failure");
    ///
    ///     // The results are guaranteed to have only two elements
    ///     assert_eq!(results.next().unwrap().unwrap().len(), 2);
    ///
    /// ```
    fn limit(self, size: u32) -> Result<Stream<D>, BuildJobError>;

    /// Apply `limit()` per each partition.
    fn limit_partition(self, size: u32) -> Result<Stream<D>, BuildJobError>;
}

/// Similar to `Limit`, but requires the output items are the minimum ones according to the
/// data ordering.
pub trait SortLimit<D: Data + Ord> {
    /// Given a `size` argument, `sort_limit()` produces an output stream that contains up to
    /// `size` number of data, where these data items are the **minimum** ones among all data. The
    /// ordering of data is determined by [`Ord`]. The `sort_limit()` function is semantically
    /// equivalent to calling [`sort()`] and [`limit()`] in combine, but with potential of more
    /// optimized implementation. For example, one may maintain a priority queue of top-`size`
    /// results on each worker, and finally aggregate all workers' top-`size` results to produce
    /// the final results.
    ///
    /// [`Ord`]: std::cmp::Ord
    /// [`sort()`]: crate::api::order::Sort::sort()
    /// [`limit()`]: crate::api::limit::Limit::limit()
    fn sort_limit(self, size: u32) -> Result<Stream<D>, BuildJobError>;
}

/// An alternative of `SortLimit` but requires a comparator of the data.
pub trait SortLimitBy<D: Data> {
    fn sort_limit_by<F>(self, size: u32, cmp: F) -> Result<Stream<D>, BuildJobError>
        where
            F: Fn(&D, &D) -> Ordering + Send + 'static;
}