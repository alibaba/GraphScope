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

use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum GraphError {
    Internal(String),
    Rocksdb(String),
    InvalidArgument(String),
    TooManyVersions(usize),
}

impl Display for GraphError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GraphError::Internal(s) | GraphError::Rocksdb(s) | GraphError::InvalidArgument(s) => {
                write!(f, "{}", s)
            }
            GraphError::TooManyVersions(limit) => write!(f, "version count exceed limit {}", limit)
        }
    }
}

impl std::error::Error for GraphError {}

impl GraphError {
    pub fn what(&self) -> &String {
        return match &self {
            GraphError::Internal(msg) => {
                msg
            }
            _ => {
                // should use Display trait
                unimplemented!()
            }
        }
    }
}

impl From<crate::db::api::GraphError> for GraphError {
    fn from(err: crate::db::api::GraphError) -> Self {
        GraphError::Internal(format!("{:?}", err))
    }
}
