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

/// Errors that occur when parse a pb struct
#[derive(Clone, Debug, PartialEq)]
pub enum ParsePbError {
    /// Parse pb structure error
    ParseError(String),
    /// Caused by any transformation for the pb from `serde_json`
    SerdeError(String),
    /// Empty pb fields error
    EmptyFieldError(String),
    /// Not supported
    Unsupported(String),
}

impl std::fmt::Display for ParsePbError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParsePbError::ParseError(e) => {
                write!(f, "invalid protobuf: {}", e)
            }
            ParsePbError::SerdeError(e) => {
                write!(f, "serde error: {}", e)
            }
            ParsePbError::EmptyFieldError(e) => {
                write!(f, "empty protobuf field: {}", e)
            }
            ParsePbError::Unsupported(e) => {
                write!(f, "Not supported: {}", e)
            }
        }
    }
}

impl std::error::Error for ParsePbError {}

impl From<String> for ParsePbError {
    fn from(desc: String) -> Self {
        ParsePbError::ParseError(desc)
    }
}

impl From<&str> for ParsePbError {
    fn from(desc: &str) -> Self {
        desc.to_string().into()
    }
}
