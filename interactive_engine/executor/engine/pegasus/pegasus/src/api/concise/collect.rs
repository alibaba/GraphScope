use crate::api::primitive::sink::FromStream;
use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

/// Collect the data stream into a container
pub trait Collect<D: Data> {
    /// Collect the input data stream into a container `C`, where `C` is a container that
    /// implements [`FromStream`]. After the collection, all data will present in the container
    /// that is locally assigned in the first worker. `collect()` is extremely useful while
    /// debugging/checking the results of a computation.
    ///
    /// # Example
    /// ```
    /// #     use pegasus::JobConf;
    /// #     use pegasus::api::{Sink, Collect};
    /// #     let conf = JobConf::new("collect_example");
    ///       let mut results = pegasus::run(conf, || {
    ///         move |input, output| {
    ///                 input.input_from(1_u32..4)?
    ///                      .collect::<Vec<u32>>()?
    ///                      .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///
    ///     // Due to collection, results is now an iterator that contains only one element,
    ///     // which is `Ok(Vec<u32>>`
    ///     assert_eq!(results.next().unwrap().unwrap(), [1, 2, 3]);
    /// ```
    fn collect<C: FromStream<D> + Default + Data>(self) -> Result<SingleItem<C>, BuildJobError>;
}
