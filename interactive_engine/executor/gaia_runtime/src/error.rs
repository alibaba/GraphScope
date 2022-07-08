//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::error::Error;

use pegasus::StartupError;

pub type StartServerResult<T> = Result<T, StartServerError>;

/// Errors that occur when generating a udf in Runtime
#[derive(Debug)]
pub enum StartServerError {
    ParseError(String),
    EmptyConfigError(String),
    ServerError(StartupError),
    Others(String),
}

impl std::fmt::Display for StartServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartServerError::ParseError(e) => write!(f, "Parse error in starting server {}", e),
            StartServerError::EmptyConfigError(e) => write!(f, "Config is missing: {}", e),
            StartServerError::ServerError(e) => write!(f, "Start pegasus error {}", e),
            StartServerError::Others(e) => write!(f, "Other start server errors: {}", e),
        }
    }
}

impl StartServerError {
    pub fn parse_error(e: &str) -> Self {
        StartServerError::ParseError(e.to_string())
    }
    pub fn empty_config_error(e: &str) -> Self {
        StartServerError::EmptyConfigError(e.to_string())
    }
    pub fn other_error(e: &str) -> Self {
        StartServerError::Others(e.to_string())
    }
}

impl Error for StartServerError {}

impl From<StartupError> for StartServerError {
    fn from(e: StartupError) -> Self {
        StartServerError::ServerError(e)
    }
}
