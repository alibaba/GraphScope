use crate::stream::SingleItem;
use crate::{BuildJobError, Data};

pub trait HasAny<D: Data> {
    fn any(self) -> Result<SingleItem<bool>, BuildJobError>;
}
