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

#[macro_use]
mod error;
pub mod api;
pub mod db;
pub mod config;
#[allow(dead_code)]
#[allow(unused_variables)]
pub mod schema;
pub mod test;

#[macro_use]
extern crate log;
extern crate maxgraph_common;
#[allow(unused_imports)]
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

#[derive(Debug)]
pub struct GraphError {
    kind: GraphErrorKind,
    msg: String,
}

impl GraphError {
    pub fn invalid_data(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::InvalidData,
            msg,
        }
    }

    pub fn invalid_condition(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::InvalidCondition,
            msg,
        }
    }

    pub fn invalid_operation(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::InvalidOperation,
            msg,
        }
    }

    pub fn not_supported(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::NotSupported,
            msg,
        }
    }

    pub fn storage_error(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::ExternalStorageError,
            msg,
        }
    }

    pub fn system_error(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::SystemError,
            msg,
        }
    }

    pub fn index_not_found(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::IndexNotFound,
            msg,
        }
    }

    pub fn internal_data_error(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::InternalDataError,
            msg,
        }
    }

    pub fn other(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::Other,
            msg,
        }
    }

    pub fn index_engine_not_found(msg: String) -> Self {
        GraphError {
            kind: GraphErrorKind::IndexEngineNotFound,
            msg,
        }
    }
}

#[derive(Debug)]
pub enum GraphErrorKind {
    InvalidData,
    InvalidCondition,
    InvalidOperation,
    NotSupported,
    ExternalStorageError,
    SystemError,
    IndexNotFound,
    IndexEngineNotFound,
    // data in store error, it's a fatal error
    InternalDataError,
    Other,
}

pub type GraphResult<T> = Result<T, GraphError>;
