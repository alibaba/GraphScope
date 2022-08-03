use crate::api::HasKey;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

/// Deduplicate the input data stream.
pub trait Dedup<D: Data + HasKey> {
    /// To apply deduplication on the input data stream. In order to do so, the input data must be
    /// [`Key`], so that a data structure like [`HashSet`] can be leveraged to eliminate duplication.
    ///
    /// [`Key`]: crate::api::keyed::Key
    /// [`HashSet`]: std::collections::HashSet
    ///
    /// # Example
    /// ```
    /// #     use pegasus::JobConf;
    /// #     use pegasus::api::{Sink, Dedup, Collect};
    /// #     let conf = JobConf::new("dedup_example");
    ///       let mut results = pegasus::run(conf, || {
    ///         move |input, output| {
    ///                 input.input_from(vec![1_u32, 1, 2, 2, 3, 3].into_iter())?
    ///                      .dedup()?
    ///                      .collect::<Vec<u32>>()?
    ///                      .sink_into(output)
    ///             }
    ///         })
    ///         .expect("run job failure;");
    ///
    ///     let mut expected = results.next().unwrap().unwrap();
    ///     expected.sort();
    ///     assert_eq!(expected,[1, 2, 3]);
    /// ```
    fn dedup(self) -> Result<Stream<D>, BuildJobError>;
}
