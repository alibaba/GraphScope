use std::fmt::Debug;

use crate::api::function::FnResult;
use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

/// Fold every data in the input stream into one single value in the first worker.
pub trait Fold<D: Data> {
    /// Folds every data into an accumulator by applying an operation, returning the final accumulator.
    ///
    /// `fold()` takes two arguments: an initial value, a factory that allows the user to produce
    /// a closure. The closure contains two arguments: an 'accumulator', and a data.
    /// The closure returns the value that the accumulator should have for the next batch of data.
    ///
    /// The initial value is the value the accumulator will have on the first call.
    /// After applying this closure to every element of the input stream, `fold()`
    /// returns the accumulator as a [`SingleItem`].
    ///
    /// The `fold()` is very similar to that in the standard [`Iterator`], with two major
    /// differences:
    ///   * Instead of directly exposing the closure as argument to the user, we rather let the user
    /// specify a factory that produces the closure. This design can often avoid frequently
    /// cloning the closure.
    ///   * As the order of data in the input stream in the distributed context is unpredictable by
    /// nature, the closure must be **associative**, otherwise the results can be unpredictable.
    ///
    /// Note: [`reduce()`] can be adopted to use the first element as the initial value, if the
    /// accumulator type and data type is the same.
    ///
    /// If the closure is both **associative** and **commutative**, it is recommended using
    /// [`fold_partition()'] + [`reduce()`], where [`fold_partition()'] can locally fold the
    /// data into one accumulator in each partition in order to reduce communication cost. The
    /// concept is similar to the Combiner of MapReduce.
    ///
    /// [`SingleItem`]: crate::stream::SingleItem
    /// [`Reduce`]: crate::api::Reduce::reduce()
    /// [`fold_partition()`]: crate::api::fold::Fold::fold_partition()
    /// [`Iterator`]: std::iter::Iterator
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Fold, Collect};
    ///
    ///   # let conf = JobConf::new("fold_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 // summing the input data
    ///                 .fold(0, || |a, b| Ok(a + b))?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///
    ///     assert_eq!(results.next().unwrap().unwrap(), 45);
    /// ```
    fn fold<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static;

    /// Similar to `fold()`, but `fold_partition()` only happens locally for partition of the
    /// input stream.
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Fold, Collect};
    ///
    ///   # let mut conf = JobConf::new("fold_partition_example");
    ///   # conf.set_workers(2);
    ///     let mut results = pegasus::run(conf, || {
    ///         let id = pegasus::get_current_worker().index;
    ///         move |input, output| {
    ///         let src = if id == 0 {
    ///             input.input_from(1..10_u32)?
    ///         } else {
    ///             input.input_from(vec![].into_iter())?
    ///         };
    ///         // prepare two partitions:
    ///         // partition 1: {1, 3, 5, 7, 9}
    ///         // partition 2: {2, 4, 6, 8}
    ///         src.repartition(|x| Ok((*x % 2) as u64))
    ///              // summing the input data of this partition
    ///              .fold_partition(0, || |a, b| Ok(a + b))?
    ///              .into_stream()?
    ///              .collect::<Vec<u32>>()?
    ///              .sink_into(output)   
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///     
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected, [20, 25]);
    /// ```
    fn fold_partition<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static;
}
