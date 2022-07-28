//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use dyn_type::CastError;
use std::any::Any;
use std::io::Error;
use std::num::{ParseFloatError, ParseIntError};

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
