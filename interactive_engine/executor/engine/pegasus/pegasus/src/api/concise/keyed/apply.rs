use crate::api::{Key, Pair};
use crate::stream::{SingleItem, Stream};
use crate::{BuildJobError, Data};

pub trait ScopeByKey<K: Data + Key, V: Data> {
    fn segment_apply<F, T>(self, task: F) -> Result<Stream<Pair<K, T>>, BuildJobError>
    where
        T: Data,
        F: FnOnce(Stream<V>) -> Result<SingleItem<T>, BuildJobError>;
}
