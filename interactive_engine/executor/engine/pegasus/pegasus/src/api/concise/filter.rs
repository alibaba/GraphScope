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

use crate::api::function::FnResult;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

/// Filter is a [`Unary`] function that offers to filter out required items from the input stream.
///
/// [`Unary`]: crate::api::primitive::unary::Unary
pub trait Filter<D: Data> {
    /// Given a user-defined function `func`, the filter function outputs the data `d` in
    /// the input stream such that `func(d) = Ok(True)`.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Filter, Collect};
    ///
    ///   # let conf = JobConf::new("filter_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 // Filter out the even number
    ///                 .filter(|i| Ok(i % 2 == 0))?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [2, 4, 6, 8]);
    /// ```
    fn filter<F>(self, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: Fn(&D) -> FnResult<bool> + Send + 'static;
}
