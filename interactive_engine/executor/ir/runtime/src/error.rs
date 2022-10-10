//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use graph_proxy::utils::expr::ExprEvalError;
use graph_proxy::GraphProxyError;
use ir_common::error::ParsePbError;
use pegasus::api::function::DynError;
use pegasus::BuildJobError;
use prost::DecodeError;

pub type FnGenResult<T> = Result<T, FnGenError>;

/// Errors that occur when generating a udf in Runtime
#[derive(Debug)]
pub enum FnGenError {
    /// Decode pb structure error
    DecodeOpError(DecodeError),
    /// Parse pb structure error
    ParseError(ParsePbError),
    /// Null storage error
    NullGraphError,
    /// Query storage error
    StoreError(GraphProxyError),
    /// Not supported error
    UnSupported(String),
}

impl FnGenError {
    pub fn unsupported_error(e: &str) -> Self {
        FnGenError::UnSupported(e.to_string())
    }
}

impl std::fmt::Display for FnGenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FnGenError::DecodeOpError(e) => write!(f, "Decode pb error in fn gen {}", e),
            FnGenError::ParseError(e) => write!(f, "Parse pb error in fn gen {}", e),
            FnGenError::NullGraphError => write!(f, "Null graph store error in fn gen",),
            FnGenError::StoreError(e) => write!(f, "Query store error in fn gen {}", e),
            FnGenError::UnSupported(e) => write!(f, "Unsupported error in fn gen  {}", e),
        }
    }
}

impl std::error::Error for FnGenError {}

impl From<ParsePbError> for FnGenError {
    fn from(e: ParsePbError) -> Self {
        FnGenError::ParseError(e)
    }
}

impl From<DecodeError> for FnGenError {
    fn from(e: DecodeError) -> Self {
        FnGenError::DecodeOpError(e)
    }
}

impl From<GraphProxyError> for FnGenError {
    fn from(e: GraphProxyError) -> Self {
        FnGenError::StoreError(e)
    }
}

impl From<FnGenError> for BuildJobError {
    fn from(e: FnGenError) -> Self {
        match e {
            FnGenError::DecodeOpError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                BuildJobError::UserError(err)
            }
            FnGenError::ParseError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                BuildJobError::UserError(err)
            }
            FnGenError::NullGraphError => {
                let err: Box<dyn std::error::Error + Send + Sync> = "Null graph error".into();
                BuildJobError::UserError(err)
            }
            FnGenError::StoreError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                BuildJobError::UserError(err)
            }
            FnGenError::UnSupported(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                BuildJobError::UserError(err)
            }
        }
    }
}

pub type FnExecResult<T> = Result<T, FnExecError>;

/// Errors that occur when execute a udf in Runtime
#[derive(Debug)]
pub enum FnExecError {
    /// Null storage error
    NullGraphError,
    /// Query storage error
    StoreError(GraphProxyError),
    /// Keyed error
    GetTagError(String),
    /// Evaluating expressions error
    ExprEvalError(ExprEvalError),
    /// Unexpected data type error
    UnExpectedData(String),
    /// Accumulate error
    AccumError(String),
    /// Not supported error
    UnSupported(String),
}

impl FnExecError {
    pub fn get_tag_error(e: &str) -> Self {
        FnExecError::GetTagError(e.to_string())
    }

    pub fn unexpected_data_error(e: &str) -> Self {
        FnExecError::UnExpectedData(e.to_string())
    }

    pub fn accum_error(e: &str) -> Self {
        FnExecError::AccumError(e.to_string())
    }

    pub fn unsupported_error(e: &str) -> Self {
        FnExecError::UnSupported(e.to_string())
    }
}

impl std::fmt::Display for FnExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FnExecError::NullGraphError => write!(f, "Null graph store error in fn exec",),
            FnExecError::StoreError(e) => write!(f, "Query store error in exec {}", e),
            FnExecError::GetTagError(e) => write!(f, "Get tag error in exec {}", e),
            FnExecError::ExprEvalError(e) => write!(f, "Eval expression error in exec {}", e),
            FnExecError::UnExpectedData(e) => write!(f, "Unexpected data type in exec {}", e),
            FnExecError::AccumError(e) => write!(f, "Accum error in exec {}", e),
            FnExecError::UnSupported(e) => write!(f, "Op not supported error in exec {}", e),
        }
    }
}

impl std::error::Error for FnExecError {}

impl From<ExprEvalError> for FnExecError {
    fn from(e: ExprEvalError) -> Self {
        FnExecError::ExprEvalError(e)
    }
}

impl From<GraphProxyError> for FnExecError {
    fn from(e: GraphProxyError) -> Self {
        FnExecError::StoreError(e)
    }
}

impl From<FnExecError> for DynError {
    fn from(e: FnExecError) -> Self {
        let err: Box<dyn std::error::Error + Send + Sync> = e.into();
        err
    }
}
