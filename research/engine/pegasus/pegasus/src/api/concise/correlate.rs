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
use crate::stream::{SingleItem, Stream};
use crate::Data;

/// Subtask is a well-known concept in the realm of query service, where an inner query (a part
/// of the main query that itself can be seen as a query standalone), or sub-query, subtask,
/// is called by the outer query. A subtask is called a correlated subtask, if the inner query's
/// evaluation depends on a parameter of the outer query. It can be easily interpreted with
/// function calls as:
///
/// fn subtask();
/// fn correlated_subtask(param);
/// fn main() {
///    // do something;
///    subtask();  // the subtask does not depend on outer parameter
///    // compute an array A;
///    for each t in A {
///        correlated_subtask(t);  // the correlated subtask depends on outer parameter at runtime
///    }
/// }
///
/// As the above function call shows, the correlated subtask often can be viewed as a **for-each**
/// semantics, where a subtask is executed for each input data from the outer query. As the
/// subtask is parameter-dependent, the execution of the subtask must occur for each data.
/// As one can image, the complication of the correlated subtask prevent it from been efficiently
/// executed by most query engines, even those highly commercialized ones. It is even more
/// challenging to consider correlated subtask in a distributed context, for which it actually
/// introduces very **fine-grained** data dependency (each subtask depends on one input data).
/// Such fine-grained data dependency is a killer for efficient implementation for a distributed
/// compute engine. One way to tackle this is via an optimization technique called **de-correlation**,
/// which attempts to remove any correlated task by equivalent transformation. But unfortunately,
/// such transformation itself is not easy to perform on the one hand, on the other hand, no all case
/// can benefit from such a technique.
///
/// Pegasus is a distributed dataflow engine that is calibrated to efficiently perform correlated
/// subtask, after seeing its large number of use cases from graph queries. Conceptually, this is
/// captured via a concept we call as **SCOPE**, where each input data D is tagged a globally-unique
/// SCOPE-ID (S), as <S, D> to get further processed by the subtask. As a result, after seeing the
/// data <S, D> while executing the sub-task, the system immediately knows which context it belongs
/// to so that the runtime data in the context can be correspondingly maintained. For example,
/// suppose the subtask is to count the number of 2-hop neighbors of each input, S can be used
/// a key to a hash table that maintains the partial counting of the execution. In terms of apis,
/// correlated subtask is supported via the apis of `apply()`.
///
pub trait CorrelatedSubTask<D: Data> {
    /// The `apply()` operator is the building block of a correlated subtask in pegasus.
    /// To do so, the user is provided a udf function `F` to specify a subtask, which accepts
    /// a stream of data (that has just one single item `d` from the input), and return a single
    /// value `t` as typed `T`. The output of the `apply()` operator is a stream of `(d, t)`-tuple
    /// where `d` is from the input data, and `t` is from the execution of the subtask for `d`.
    /// In order for the semantics to be sound, the execution of each subtask must return at most
    /// **one** single value. Multiple data items be only be allowed by explicitly collecting them
    /// via [`Collect`].
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, CorrelatedSubTask, Map, Collect};
    ///
    ///   # let conf = JobConf::new("filter_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..5u32)?
    ///                 // apply subtask for each data
    ///                 .apply(|sub| {
    ///                    sub.flat_map(|i| Ok(0 .. i))?
    ///                       .collect::<Vec<u32>>()
    ///                  })?
    ///                 .collect::<Vec<(u32, Vec<u32>)>>()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, vec![
    ///         (1, vec![0]),
    ///         (2, vec![0, 1]),
    ///         (3, vec![0, 1, 2]),
    ///         (4, vec![0, 1, 2, 3]),
    ///     ]);
    /// ```
    /// [`Collect`]: crate::api::collect::Collect
    fn apply<T, F>(self, func: F) -> Result<Stream<(D, T)>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<SingleItem<T>, BuildJobError>;

    /// Similar to `apply()` but with `max_parallel` to control the maximum number of subtasks
    /// that can be executed at the same time. For example, if the input is {1, 2, 3, 4},
    /// and let `max_parallel` be 2, then the subtasks will be first performed on {1, 2} (by random),
    /// and then {3, 4} upon the termination of {1, 2}.
    fn apply_parallel<T, F>(self, max_parallel: u32, func: F) -> Result<Stream<(D, T)>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<D>) -> Result<SingleItem<T>, BuildJobError>;
}
