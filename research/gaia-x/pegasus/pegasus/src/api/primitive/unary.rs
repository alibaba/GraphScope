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

use crate::api::meta::OperatorInfo;
use crate::communication::{Input, Output};
use crate::errors::{BuildJobError, JobExecError};
use crate::stream::Stream;
use crate::Data;

/// Used to construct operators with one input and one output. An unary operator always
/// consumes data from the input stream, call the user-defined program to produce new data
/// into the output stream.
///
/// `Unary`-shaped operators are the most important category of dataflow operators, which
/// include [`Map`], [`Filter`], [`Fold`], [`Sort`], [`Limit`] etc.
///
/// [`Map`]: crate::api::map::Map
/// [`Filter`]: crate::api::filter::Filter
/// [`Fold`]: crate::api::fold::Fold
/// [`Sort`]: crate::api::order::Sort
/// [`Limit`]: crate::api::limit::Limit
pub trait Unary<I: Data> {
    /// `unary()` takes two arguments: the first argument specifies the name of this operator,
    /// and the second operator is a closure builder that is applies once while building the
    /// dataflow. The closure builder takes an [`OperatorInfo`] as input to specify necessary
    /// metadata information of the operation, such as the operator's name, index and scope level.
    ///
    /// The closure returned by the closure builder specifies the actual execution logic of
    /// the unary operator. It consumes each data from the `Input` of the operator, and can produce
    /// arbitrary number of data to the `Output` of the operator. Additionally,
    /// it implements [`FnMut`] that can capture and mutate any runtime states, which allows
    /// the user to achieve any stateful operation.
    ///
    /// Note: there are two most common errors: [`BuildJobError`] and [`JobExecError`]:
    ///     * [`BuildJobError`] is defined to capture any error from building the dataflow, which
    /// may not be an issue if users are building the dataflow directly from the apis. However,
    /// such error is inevitable if the dataflow is being generated from some third-party codes,
    /// e.g. from parsing a high-level language such as SQL and Gremlin.
    ///     * [`JobExecError`] is defined to capture any runtime error when the engine is running
    /// the operator, typically errors are IO (disk/network)-related errors, OS-related errors, and
    /// engine-related errors. Capturing these errors allow the system to remain robust at service.
    ///
    /// [`OperatorInfo`]: crate::api::meta::OperatorInfo
    /// [`FnMut`]: std::ops::FnMut
    /// [`BuildJobError`]: crate::errors::BuildJobError
    /// [`JobExecError`]: crate::errors::JobExecError;
    ///
    /// # Example
    /// ```
    /// #     use pegasus::{JobConf, Tag};
    /// #     use pegasus::Configuration;
    /// #     use pegasus::api::{Sink, Unary, Collect};
    /// #     use std::collections::{HashSet, HashMap};
    /// #     let conf = JobConf::new("unary_example");
    ///       let mut results = pegasus::run(conf, || {
    ///         // Note that tag is used to differential different scopes
    ///         let mut map: HashMap<Tag, HashSet<u32>> = HashMap::new();
    ///         move |input, output| {
    ///                 input.input_from(vec![1_u32, 1, 2, 2, 3, 3].into_iter())?
    ///                      .repartition(|x| Ok(*x as u64))
    ///                      .unary("unary_operator", |_info| {
    ///                             move |unary_input, unary_output| {
    ///                                 unary_input.for_each_batch(|batch| {
    ///                                     // Each batch of data is bound with a tag to indicate the scope
    ///                                     // it is currently in.
    ///                                     let mut session = unary_output.new_session(batch.tag())?;
    ///                                     let mut exists = map.entry(batch.tag().clone()).or_insert_with(HashSet::new);
    ///                                     for data in batch.drain() {
    ///                                     // the unary operator capture a hashset as states to dedup
    ///                                         if !exists.contains(&data) {
    ///                                             exists.insert(data);
    ///                                             session.give(data)?;
    ///                                         }
    ///                                     }
    ///                                     Ok(())
    ///                                 })
    ///                             }
    ///                     })?
    ///                     .collect::<Vec<u32>>()?
    ///                     .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [1, 2, 3]);
    /// ```
    fn unary<O, B, F>(self, name: &str, builder: B) -> Result<Stream<O>, BuildJobError>
    where
        O: Data,
        B: FnOnce(&OperatorInfo) -> F,
        F: FnMut(&mut Input<I>, &Output<O>) -> Result<(), JobExecError> + Send + 'static;
}
