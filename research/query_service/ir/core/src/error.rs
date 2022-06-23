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
//!

use std::fmt;

use ir_common::error::ParsePbError;
use ir_common::expr_parse::error::ExprError;
use ir_common::NameOrId;
use prost::EncodeError;

/// Record any error while transforming ir to a pegasus physical plan
#[derive(Debug, Clone)]
pub enum IrError {
    // Logical Errors
    TableNotExist(NameOrId),
    ColumnNotExist(NameOrId),
    ParentNodeNotExist(u32),
    TagNotExist(NameOrId),
    ParsePbError(ParsePbError),
    ParseExprError(ExprError),
    InvalidPattern(String),

    // Physical Errors
    PbEncodeError(EncodeError),
    MissingData(String),
    InvalidRange(i32, i32),

    // Common Errors
    Unsupported(String),
}

pub type IrResult<T> = Result<T, IrError>;

impl fmt::Display for IrError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IrError::TableNotExist(table) => {
                write!(f, "the given table(label): {:?} does not exist", table)
            }
            IrError::ColumnNotExist(col) => {
                write!(f, "the given column(property): {:?} does not exist", col)
            }
            IrError::TagNotExist(tag) => write!(f, "the given tag: {:?} does not exist", tag),
            IrError::ParentNodeNotExist(node) => {
                write!(f, "the given parent node: {:?} does not exist", node)
            }
            IrError::ParsePbError(err) => write!(f, "parse pb error: {:?}", err),
            IrError::ParseExprError(err) => write!(f, "parse expression error: {:?}", err),
            IrError::InvalidPattern(s) => write!(f, "invalid pattern: {:?}", s),
            IrError::PbEncodeError(err) => write!(f, "encoding protobuf error: {:?}", err),
            IrError::MissingData(s) => write!(f, "missing required data: {:?}", s),
            IrError::InvalidRange(lo, up) => {
                write!(f, "invalid range ({:?}, {:?})", lo, up)
            }
            IrError::Unsupported(s) => write!(f, "{:?}: is not supported", s),
        }
    }
}

impl std::error::Error for IrError {}

impl From<ParsePbError> for IrError {
    fn from(err: ParsePbError) -> Self {
        Self::ParsePbError(err)
    }
}

impl From<EncodeError> for IrError {
    fn from(err: EncodeError) -> Self {
        Self::PbEncodeError(err)
    }
}

impl From<ExprError> for IrError {
    fn from(err: ExprError) -> Self {
        Self::ParseExprError(err)
    }
}
