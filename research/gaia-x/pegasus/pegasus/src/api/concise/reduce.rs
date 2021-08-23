use crate::macros::map::FnResult;
use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

/// Reduce the data stream into one single value.
pub trait Reduce<D: Data> {
    /// Reduce is in de facto [`fold()`] with the ``first'' data as the initial value, and as a result,
    /// the accumulator and the data must have the same type. In order for the results to be
    /// correct, the closure provided by the `builder` must be both **associative** and
    /// **commutative**.
    ///
    /// [`fold()`]: crate::api::fold::Fold::fold
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Reduce};
    ///
    ///   # let conf = JobConf::new("reduce_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 // summing the input data
    ///                 .reduce(|| |a, b| Ok(a + b))?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///       .expect("build job failure");
    ///
    ///     assert_eq!(results.next().unwrap().unwrap(), 45);
    /// ```
    fn reduce<B, F>(self, builder: B) -> Result<SingleItem<D>, BuildJobError>
    where
        F: FnMut(D, D) -> FnResult<D> + Send + 'static,
        B: Fn() -> F + Send + Sync + 'static;

    /// `reduce_partition()` is in de facto [`fold_partition()`] with the ``first'' data as the
    /// initial value, and as a result, the accumulator and the data must have the same type.
    /// In order for the results to be correct, the closure provided by the `builder` must be both
    /// **associative** and **commutative**.
    ///
    /// [`fold()`]: crate::api::fold::Fold::fold_partition
    ///
    /// # Example
    /// ```
    ///   # use pegasus::{JobConf};
    ///   # use pegasus::api::{Sink, Reduce};
    ///
    ///   # let mut conf = JobConf::new("reduce_example");
    ///   # conf.set_workers(2);
    ///     let mut results = pegasus::run(conf, || {
    ///         let id = pegasus::get_current_worker().index;
    ///         move |input, output| {
    ///             let src = if id == 0 {
    ///                 input.input_from(1..10u32)?
    ///             } else {
    ///                 input.input_from(vec![].into_iter())?
    ///             };                  
    ///             src
    ///                 .repartition(|x| Ok(*x as u64 % 2))
    ///                 // summing the input data
    ///                 .reduce_partition(|| |a, b| Ok(a + b))?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///       .expect("build job failure")
    ///       .map(|x| x.unwrap())
    ///       .collect::<Vec<u32>>();
    ///
    ///     results.sort();
    ///     assert_eq!(results, [20, 25]);
    /// ```
    fn reduce_partition<B, F>(self, builder: B) -> Result<SingleItem<D>, BuildJobError>
    where
        F: FnMut(D, D) -> FnResult<D> + Send + 'static,
        B: Fn() -> F + Send + Sync + 'static;
}
