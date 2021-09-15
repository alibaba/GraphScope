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

/// Map is a category of [`Unary`] functions that mutate each data of the input stream via a
/// user-defined function (udf) to produce a new data to the output stream.
///
/// [`Unary`]: crate::api::primitive::unary::Unary
pub trait Map<I: Data> {
    /// Apply the user-defined function `func`, that mutates each data of type `I` of the
    /// input stream, and produce the data of type `O` to the output stream.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Map, Sink, Collect};
    ///   # let mut conf = JobConf::new("map_example");
    ///   # conf.plan_print = false;
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 .map(|i| Ok(i*2))?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [2, 4, 6, 8, 10, 12, 14, 16, 18]);
    /// ```
    fn map<O, F>(self, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        F: Fn(I) -> FnResult<O> + Send + 'static;

    /// Similar to [`map`], this function mutates the input data of the type `I` into
    /// the output data of the type `O`. The difference is, [`map`] must produce an output
    /// data item for each input item, while [`filter_map`] allows the udf to return
    /// `Option<O>`, and only those input data items that return `Some` result can present in
    /// the output stream.
    ///
    /// This function is literally a combination of [`map`] and [`filter`].
    ///
    /// [`map`]: crate::api::Map::map
    /// [`filter_map`]: crate::api::Map::filter_map
    /// [`filter`]: crate::api::Filter::filter
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Map, Sink, Collect};
    ///
    ///   # let conf = JobConf::new("filter_map_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 .filter_map(|i| {
    ///                     if i % 2 == 0 {
    ///                         Ok(Some(i * 2 + 1))
    ///                     } else {
    ///                         Ok(None)
    ///                     }
    ///                 })?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [5, 9, 13, 17]);
    /// ```
    fn filter_map<O, F>(self, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        F: Fn(I) -> FnResult<Option<O>> + Send + 'static;

    /// This function can produce zero, one and more than one output data items for each input
    /// item, in which the udf function allows the user to specify the output data as an iterator.
    ///
    /// Note that the engine will adaptively apply flow control for the `flatmap` function, that is
    /// it will monitor the capacity of the output buffer of this operator
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Map, Sink, Collect};
    ///
    ///   # let conf = JobConf::new("flat_map_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 .flat_map(|i| {
    ///                     if i % 2 == 0 {
    ///                         Ok(vec![].into_iter())  // return None
    ///                     } else if i % 3 == 0 {
    ///                         // return more than one
    ///                         Ok(vec![i / 3, i / 3 + 1, i / 3 + 2].into_iter())
    ///                     } else {
    ///                         Ok(vec![i].into_iter())  // return one
    ///                     }
    ///                 })?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [1, 1, 2, 3, 3, 4, 5, 5, 7]);
    /// ```
    fn flat_map<O, R, F>(self, func: F) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        R: Iterator<Item = O> + Send + 'static,
        F: Fn(I) -> FnResult<R> + Send + 'static;
}
