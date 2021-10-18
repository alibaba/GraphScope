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

pub type ParsePbResult<T> = Result<T, ParsePbError>;

#[derive(Debug, PartialEq)]
pub enum ParsePbError {
    InvalidPb(String),
}

impl std::fmt::Display for ParsePbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParsePbError::InvalidPb(s) => write!(f, "invalid protobuf: {}", s),
        }
    }
}

impl std::error::Error for ParsePbError {}

impl From<&str> for ParsePbError {
    fn from(e: &str) -> Self {
        ParsePbError::InvalidPb(e.into())
    }
}

pub type DynError = Box<dyn std::error::Error + Send>;
pub type DynResult<T> = Result<T, Box<dyn std::error::Error + Send>>;
pub type DynIter<T> = Box<dyn Iterator<Item = T> + Send>;

impl From<ParsePbError> for DynError {
    fn from(e: ParsePbError) -> Self {
        let err: Box<dyn std::error::Error + Send + Sync> = e.into();
        err
    }
}

/// A tricky bypassing of Rust's compiler. It is useful to simplify throwing a `DynError`
/// from a `&str` as `Err(str_to_dyn_err('some str'))`
pub fn str_to_dyn_error(str: &str) -> DynError {
    let err: Box<dyn std::error::Error + Send + Sync> = str.into();
    err
}
