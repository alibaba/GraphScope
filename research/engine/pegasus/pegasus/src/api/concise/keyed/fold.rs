use std::collections::HashMap;

use crate::api::function::FnResult;
use crate::api::Key;
use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

pub trait FoldByKey<K: Data + Key, V: Data> {
    /// Analogous to [`fold()`] but folding the data according to the key part of the input data.
    ///
    /// [`fold()`]: crate::api::fold::Fold::fold()
    fn fold_by_key<I, B, F>(self, init: I, builder: B) -> Result<SingleItem<HashMap<K, I>>, BuildJobError>
    where
        I: Data,
        F: FnMut(I, V) -> FnResult<I> + Send + 'static,
        B: Fn() -> F + Send + 'static;
}
