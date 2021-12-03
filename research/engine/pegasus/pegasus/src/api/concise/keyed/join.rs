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

use crate::api::HasKey;
use crate::errors::BuildJobError;
use crate::stream::Stream;
use crate::Data;

/// Join self and other stream (we treat self stream as left stream, other stream as right stream),
/// the items in both streams are needed to be keyed.
/// [`key_by`] helps items get keyed. It inputs a value and outputs the (key, value) pair.
///
/// Notice that the keys' type of items in left and right streams need to be same,
/// and it must be cloneable and hashable such that it can be maintained and indexed in a hashtable.
///
/// We now implement 6 types of joins, namely [`inner_join`], [`left_outer_join`],
/// [`right_outer_join`], [`full_outer_join`], [`semi_join`] and [`anti_join`].
/// While [`semi_join`] and [`anti_join`] have the variances of left and right, but we only consier
/// the left case, knowing that the right case can be easily achieved by swapping the two streams.
///
/// These joins are for now executed based on the equivalence of the keys of the left and right
/// streams. A general version may require defining any predicate on the key, for example,
/// left.key > right.key, we leave it as a future work.
///
///
/// [`key_by`]: crate::api::KeyBy
/// [`inner_join`]: crate::api::Join::inner_join
/// [`left_outer_join`]: crate::api::Join::left_outer_join
/// [`right_outer_join`]: crate::api::Join::right_outer_join
/// [`full_outer_join`]: crate::api::Join::full_outer_join
/// [`semi_join`]: crate::api::Join::semi_join
/// [`anti_join`]: crate::api::Join::anti_join
/// # Example
/// ```
/// #     use pegasus::api::*;
/// #     use pegasus::JobConf;
/// #     use pegasus::Configuration;
/// #     pegasus_common::logs::init_log();
/// #     let mut conf = JobConf::new("join test");
/// #     conf.set_workers(1);
///     let mut results = pegasus::run(conf, || {
///         let id = pegasus::get_current_worker().index;
///         move |input, output| {
///             // stream {1,2,3}
///             let src1 = if id == 0 { input.input_from(1..4)? } else { input.input_from(vec![])? };
///             let (src1,src2) = src1.copied()?;
///             let src2=src2.map(|x| Ok(x + 1))?; // stream {2,3,4}
///             src1.key_by(|x| Ok((x, x)))? // use item value as key
///                 .inner_join(src2.key_by(|x| Ok((x, x)))?.partition_by_key())? // inner_join two streams
///                 .map(|d| Ok(((d.0.key, d.0.value), (d.1.key, d.1.value))))?
///                 .collect::<Vec<((u32, u32), (u32, u32))>>()?
///                 .sink_into(output)
///             }
///         })
///         .expect("run job failure;");
///
///     let mut expected = results.next().unwrap().unwrap();
///     expected.sort();
///     assert_eq!(expected, [((2, 2), (2, 2)),((3, 3), (3, 3))]);
/// ```
pub trait Join<L: Data + HasKey, R: Data + HasKey<Target = L::Target>> {
    /// Inner join will return a stream containing all pairs of elements whose keys are matched in self and other.
    /// Each pair of elements in inner join will be returned as a tuple `(L, R)`.
    fn inner_join(self, other: Stream<R>) -> Result<Stream<(L, R)>, BuildJobError>;

    /// Beside the output of inner join, left outer join will also return the unmatched elements in the left stream
    /// as `(Some(l), None)`. Each pair of elements in outer join will be returned as a tuple `(Option<L>, Option<R>)`.
    fn left_outer_join(self, other: Stream<R>) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError>;

    /// Similar to left outer join, right outer join will return the unmatched elements in the right stream
    /// as `(None, Some(r))`.
    fn right_outer_join(self, other: Stream<R>) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError>;

    /// Full outer join will return the unmatched elements in both left and right streams.
    fn full_outer_join(self, other: Stream<R>) -> Result<Stream<(Option<L>, Option<R>)>, BuildJobError>;

    /// Semi join will only returns the matched elements in the left stream as `L`.
    fn semi_join(self, other: Stream<R>) -> Result<Stream<L>, BuildJobError>;

    /// Similar to semi join,
    /// but anti join will return the unmatched elements in the left stream as `L`.
    fn anti_join(self, other: Stream<R>) -> Result<Stream<L>, BuildJobError>;
}
