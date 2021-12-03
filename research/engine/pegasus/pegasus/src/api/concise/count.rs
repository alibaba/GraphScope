use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

/// Count is applied to count the number of items in the input stream.
pub trait Count<D: Data> {
    /// The count function returns a [`SingleItem`] of `u64` indicating the number of items
    /// in the input stream.
    ///
    /// [`SingleItem`]: crate::stream::SingleItem
    ///
    /// # Example
    /// ```
    ///   # use pegasus::JobConf;
    ///   # use pegasus::api::{Sink, Count};
    ///   # let conf = JobConf::new("count_example");
    ///     let mut results = pegasus::run(conf, || {
    ///         |input, output| {
    ///             input
    ///                 .input_from(1..10u32)?
    ///                 .count()?
    ///                 .sink_into(output)
    ///         }
    ///     })
    ///     .expect("build job failure");
    ///
    ///     assert_eq!(results.next().unwrap().unwrap(), 9);
    /// ```
    fn count(self) -> Result<SingleItem<u64>, BuildJobError>;
}
