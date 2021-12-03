use crate::api::{Collect, Fold, FromStream};
use crate::stream::{SingleItem, Stream};
use crate::{BuildJobError, Data};

impl<D: Data> Collect<D> for Stream<D> {
    fn collect<C: FromStream<D> + Default + Data>(self) -> Result<SingleItem<C>, BuildJobError> {
        self.fold(C::default(), || {
            |mut collect, next| {
                collect.on_next(next)?;
                Ok(collect)
            }
        })
    }
}
