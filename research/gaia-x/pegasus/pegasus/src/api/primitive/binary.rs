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

/// Used to construct operators with two inputs and one output.
/// `Binary`-shaped operators include [`Merge`], [`Join`], [`Zip`] etc.
///
/// [`Merge`]: crate::api::merge::Merge
/// [`Join`]: crate::api::join::Join
/// [`Zip`]: crate::api::zip::Zip
pub trait Binary<L: Data> {
    /// `binary()` is very similar to [`unary()`] but it takes one further argument to accept
    /// the input from the other data stream, and its closure builder builds a closure
    /// of two (left/right) inputs instead of a single one. The user can specify arbitrary
    /// operation to both the left and right data to produce one single output stream.
    ///
    /// [`unary()`]: crate::api::primitive::unary::Unary::unary()
    ///
    /// # Example
    ///
    /// ```
    /// #     use pegasus::{JobConf, Tag};
    /// #     use pegasus::Configuration;
    /// #     use pegasus::api::{Sink, Binary, Unary, Map, Collect};
    /// #     use std::collections::HashMap;
    ///       
    /// #     #[derive(Default)]
    /// #     struct DataItem { is_end: bool, data: Vec<u32> };
    /// #      #[derive(Default)]    
    /// #      struct DataPair { left: DataItem, right: DataItem };
    ///
    /// #     let mut conf = JobConf::new("binary_example");
    /// #     conf.plan_print = false;
    ///       let mut results = pegasus::run(conf, || {
    ///         let mut hashmap: HashMap<Tag, DataPair> = HashMap::new();
    ///         move |input, output| {
    ///                 let src1 = input.input_from(vec![1_u32, 2, 3].into_iter())?;
    ///                 let (src1, src2) = src1.copied()?;
    ///                 let src2 = src2.map(|d| Ok(d + 1))?.repartition(|x| Ok(*x as u64 - 1));
    ///                 src1.repartition(|x| Ok(*x as u64))
    ///                      .binary("binary_operator", src2, |_info| move |in1, in2, out| {    ///
    ///                         in1.for_each_batch(|batch| {
    ///                             let mut pair = hashmap.entry(batch.tag().clone()).or_insert(DataPair::default());
    ///                             pair.left.data.extend(batch.drain());
    ///                             if batch.is_last() {
    ///                                 pair.left.is_end = true;
    ///                                 if pair.right.is_end {
    ///                                     let mut session = out.new_session(&batch.tag())?;
    ///                                     pair.left.data.sort();
    ///                                     pair.right.data.sort();
    ///                                     for (d1, d2) in pair.left.data.iter().zip(pair.right.data.iter()) {
    ///                                         session.give((*d1, *d2))?;
    ///                                     }       
    ///                                 }
    ///                             }     
    ///                             Ok(())
    ///                         })?;
    ///                         in2.for_each_batch(|batch| {
    ///                             let mut pair = hashmap.entry(batch.tag().clone()).or_insert(DataPair::default());
    ///                             pair.right.data.extend(batch.drain());
    ///                             if batch.is_last() {
    ///                                 pair.right.is_end = true;
    ///                                 if pair.left.is_end {
    ///                                     let mut session = out.new_session(&batch.tag())?;
    ///                                     pair.left.data.sort();
    ///                                     pair.right.data.sort();
    ///                                     for (d1, d2) in pair.left.data.iter().zip(pair.right.data.iter()) {
    ///                                         session.give((*d1, *d2))?;
    ///                                     }       
    ///                                 }
    ///                             }                           
    ///                             Ok(())
    ///                         })
    ///                     })?
    ///                     .collect::<Vec<(u32, u32)>>()?
    ///                     .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [(1, 2), (2, 3), (3, 4)]);
    /// ```
    fn binary<R, O, B, F>(
        self, name: &str, other: Stream<R>, builder: B,
    ) -> Result<Stream<O>, BuildJobError>
    where
        R: Data,
        O: Data,
        B: FnOnce(&OperatorInfo) -> F,
        F: FnMut(&mut Input<L>, &mut Input<R>, &Output<O>) -> Result<(), JobExecError> + Send + 'static;
}
