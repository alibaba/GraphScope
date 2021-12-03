use crate::stream::Stream;
use crate::{BuildJobError, Data};

pub trait Zip<D: Data> {
    /// 'Zips up' two streams into a single stream of pairs.
    fn zip<T: Data>(self, other: Stream<T>) -> Result<Stream<(D, T)>, BuildJobError>;
}
