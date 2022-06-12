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

use pegasus::api::function::DynError;

pub type GraphProxyResult<T> = Result<T, GraphProxyError>;

/// Errors that occur when querying or writing in graph proxy
#[derive(Debug)]
pub enum GraphProxyError {
    /// Query storage error
    QueryStoreError(String),
    /// Query storage error
    WriteGraphError(String),
    /// Not supported error
    UnSupported(String),
}

impl GraphProxyError {
    pub fn query_store_error(e: &str) -> Self {
        GraphProxyError::QueryStoreError(e.to_string())
    }

    pub fn write_graph_error(e: &str) -> Self {
        GraphProxyError::WriteGraphError(e.to_string())
    }

    pub fn unsupported_error(e: &str) -> Self {
        GraphProxyError::UnSupported(e.to_string())
    }
}

impl std::fmt::Display for GraphProxyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphProxyError::QueryStoreError(e) => write!(f, "Query store error in exec {}", e),
            GraphProxyError::WriteGraphError(e) => write!(f, "Write graph error in exec {}", e),
            GraphProxyError::UnSupported(e) => write!(f, "Op not supported error in exec {}", e),
        }
    }
}

impl std::error::Error for GraphProxyError {}

impl From<GraphProxyError> for DynError {
    fn from(e: GraphProxyError) -> Self {
        match e {
            GraphProxyError::QueryStoreError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
            GraphProxyError::WriteGraphError(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
            GraphProxyError::UnSupported(e) => {
                let err: Box<dyn std::error::Error + Send + Sync> = e.into();
                err
            }
        }
    }
}
