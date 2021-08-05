use crate::api::function::DynError;
use std::error::Error;

pub trait ToDynError {
    fn into_error(self) -> DynError;
}

impl<E: Into<Box<dyn Error + Send + Sync>>> ToDynError for E {
    fn into_error(self) -> DynError {
        let error = self.into();
        error as DynError
    }
}
