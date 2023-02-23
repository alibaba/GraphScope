use std::any::Any;
use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};

use dyn_type::CastError;

pub type GDBResult<T> = Result<T, GDBError>;

#[derive(Debug)]
pub enum GDBError {
    ModifyReadOnlyError,
    BincodeError(std::boxed::Box<bincode::ErrorKind>),
    JsonError(serde_json::Error),
    IOError(std::io::Error),
    DynError(Box<dyn Any + Send>),
    CastError(CastError),
    DBNotFoundError,
    LruZeroCapacity,
    JsonObjectFieldError,
    BooleanExpressionError,
    StringExpressionError,
    NumberExpressionError,
    EdgeNotFoundError,
    VertexNotFoundError,
    UnknownError,
    CrossComparisonError,
    OutOfBoundError,
    ParseError,
    InvalidFunctionCallError,
    InvalidTypeError,
    FieldNotExistError,
}

impl From<std::io::Error> for GDBError {
    fn from(error: Error) -> Self {
        GDBError::IOError(error)
    }
}

impl From<std::num::ParseIntError> for GDBError {
    fn from(_error: ParseIntError) -> Self {
        GDBError::ParseError
    }
}

impl From<std::num::ParseFloatError> for GDBError {
    fn from(_error: ParseFloatError) -> Self {
        GDBError::ParseError
    }
}

impl From<serde_json::Error> for GDBError {
    fn from(error: serde_json::Error) -> Self {
        GDBError::JsonError(error)
    }
}

impl From<Box<bincode::ErrorKind>> for GDBError {
    fn from(error: Box<bincode::ErrorKind>) -> Self {
        GDBError::BincodeError(error)
    }
}

impl From<()> for GDBError {
    fn from(_error: ()) -> Self {
        GDBError::UnknownError
    }
}

impl From<Box<dyn Any + Send>> for GDBError {
    fn from(error: Box<dyn Any + Send>) -> Self {
        GDBError::DynError(error)
    }
}
