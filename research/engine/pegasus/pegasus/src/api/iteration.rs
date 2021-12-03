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

use crate::api::function::{FilterFunction, FnResult};
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

/// Define the cyclic dataflow
pub trait Iteration<D: Data> {
    /// To iterate through a sub-dataflow given by `func` by at most `max_iters` times.
    /// The input data stream will serve as the input of `func` in the first iteration,
    /// and the output data stream of `func` of next current iteration will automatically be
    /// the input of `func` of the next iteration. In the dataflow, this will introduce a cyclic
    /// (or feedback) edge between the last operator and first operator of `func`, as the name
    /// cyclic dataflow has suggested.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Collect, Iteration, Map};
    ///
    ///   # let conf = JobConf::new("flat_map_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(0..6u32)?
    ///                 // add each value ten times
    ///                 .iterate(10, |input| {
    ///                     input.map(|d| Ok(d + 1))
    ///                 })?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, (10 .. 16u32).collect::<Vec<u32>>());
    /// ```
    fn iterate<F>(self, max_iters: u32, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>;

    /// Similar to `iterate()`, but `iterate_until()` can iterate the sub-dataflow until arriving
    /// at a fixed point given by `IterCondition`.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Collect, Iteration, Map, IterCondition};
    ///
    ///   # let conf = JobConf::new("flat_map_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             let mut until = IterCondition::<u32>::max_iters(10000);
    ///             until.until(|d| Ok(*d == 1));
    ///             input
    ///                 .input_from(vec![10, 100, 1000, 10000])?
    ///                 // The Collatz Conjecture:
    ///                 // Given a number d, do the following:
    ///                 // * if it is an even number, let d = d / 2
    ///                 // * otherwise, let d = d * 3 + 1
    ///                 // For any natural number, the final result will be 1 while continuously
    ///                 // process the above operations.
    ///                 .iterate_until(until, |input| {
    ///                     input.map(|d| {
    ///                         if d % 2 == 0 {
    ///                             Ok(d / 2)
    ///                         } else {
    ///                             Ok(d * 3 + 1)
    ///                         }
    ///                     })
    ///                 })?
    ///                 .collect::<Vec<u32>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let expected = results.next().unwrap().unwrap();
    ///     assert_eq!(expected, [1, 1, 1, 1]);
    /// ```
    fn iterate_until<F>(self, until: IterCondition<D>, func: F) -> Result<Stream<D>, BuildJobError>
    where
        F: FnOnce(Stream<D>) -> Result<Stream<D>, BuildJobError>;
}

/// To define the termination condition for an `iterate()` dataflow.
pub struct IterCondition<D> {
    /// The maximum iterations of `iterate()`. The default value is `u32::max_value()`
    pub max_iters: u32,
    /// The data-dependent termination condition
    until: Option<Box<dyn FilterFunction<D>>>,
}

impl<D: 'static> IterCondition<D> {
    pub fn new() -> Self {
        IterCondition { max_iters: !0u32, until: None }
    }

    pub fn max_iters(max_iters: u32) -> Self {
        IterCondition { max_iters, until: None }
    }

    pub fn set_until(&mut self, until: Box<dyn FilterFunction<D>>) {
        self.until = Some(until);
    }

    #[inline]
    pub fn is_converge(&self, data: &D) -> FnResult<bool> {
        if let Some(cond) = self.until.as_ref() {
            cond.test(data)
        } else {
            Ok(false)
        }
    }

    #[inline]
    pub fn has_until_cond(&self) -> bool {
        self.until.is_some()
    }
}
