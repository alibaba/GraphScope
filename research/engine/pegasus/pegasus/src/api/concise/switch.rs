use crate::macros::map::FnResult;
use crate::stream::Stream;
use crate::{BuildJobError, Data};

pub trait SwitchFn<D: Data> {
    fn switch<L, R, F>(self, func: F) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        F: Fn(&D) -> FnResult<bool> + Send + 'static;

    fn switch_mut<L, R, F, B>(self, builder: B) -> Result<(Stream<L>, Stream<R>), BuildJobError>
    where
        L: Data,
        R: Data,
        F: FnMut(&D) -> FnResult<bool> + Send + 'static,
        B: Fn() -> F + Send + 'static;
}
