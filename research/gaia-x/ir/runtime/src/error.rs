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

use crate::expr::error::ExprError;
use crate::process::operator::KeyedError;
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
    /// Empty storage error
    EmptyGraphError,
    /// Query storage error
    QueryStoreError(DynError),
    /// Not supported error
    UnSupported(String),
}

impl std::fmt::Display for FnGenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FnGenError::DecodeOpError(e) => write!(f, "Decode pb error in fn gen {}", e),
            FnGenError::ParseError(e) => write!(f, "Parse pb error in fn gen {}", e),
            FnGenError::EmptyGraphError => write!(f, "Empty graph store error in fn gen",),
            FnGenError::QueryStoreError(e) => write!(f, "Query store error in fn gen {}", e),
            FnGenError::UnSupported(e) => write!(f, "Op not supported error in fn gen  {}", e),
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

impl From<DynError> for FnGenError {
    fn from(e: DynError) -> Self {
        FnGenError::QueryStoreError(e)
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
            FnGenError::EmptyGraphError => {
                let err: Box<dyn std::error::Error + Send + Sync> = "Empty graph error".into();
                BuildJobError::UserError(err)
            }
            FnGenError::QueryStoreError(e) => BuildJobError::UserError(e),
            FnGenError::UnSupported(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                BuildJobError::UserError(err)
            }
        }
    }
}

/// Errors that occur when execute a udf in Runtime
#[derive(Debug)]
pub enum FnExecError {
    /// Empty storage error
    EmptyGraphError,
    /// Query storage error
    QueryStoreError(String),
    /// Keyed error
    GetTagError(KeyedError),
    /// Evaluating expressions error
    ExprEvalError(ExprError),
    /// Unexpected data type error
    UnExpectedDataType(String),
    /// Not supported error
    UnSupported(String),
}

impl std::fmt::Display for FnExecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FnExecError::EmptyGraphError => write!(f, "Empty graph store error in fn exec",),
            FnExecError::QueryStoreError(e) => write!(f, "Query store error in exec {}", e),
            FnExecError::GetTagError(e) => write!(f, "Get tag error in exec {}", e),
            FnExecError::ExprEvalError(e) => write!(f, "Eval expression error in exec {}", e),
            FnExecError::UnExpectedDataType(e) => write!(f, "Unexpected data type in exec {}", e),
            FnExecError::UnSupported(e) => write!(f, "Op not supported error in exec {}", e),
        }
    }
}

impl std::error::Error for FnExecError {}

impl From<KeyedError> for FnExecError {
    fn from(e: KeyedError) -> Self {
        FnExecError::GetTagError(e)
    }
}

impl From<ExprError> for FnExecError {
    fn from(e: ExprError) -> Self {
        FnExecError::ExprEvalError(e)
    }
}

impl From<FnExecError> for DynError {
    fn from(e: FnExecError) -> Self {
        match e {
            FnExecError::EmptyGraphError => {
                let err: Box<dyn std::error::Error + Send + Sync> = "Empty graph error".into();
                err
            }
            FnExecError::QueryStoreError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
            FnExecError::GetTagError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
            FnExecError::ExprEvalError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
            FnExecError::UnExpectedDataType(_) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
            FnExecError::UnSupported(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
        }
    }
}
