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

use std::fmt::Display;

use dyn_type::CastError;

use crate::utils::expr::eval::OperatorDesc;

pub mod eval;
pub mod eval_pred;

pub type ExprEvalResult<T> = Result<T, ExprEvalError>;

/// The error cases while parsing and evaluating expressions
#[derive(Debug, PartialEq)]
pub enum ExprEvalError {
    /// The error while casting from different data types enabled by `dyn_type::object::Object`
    CastError(CastError),
    /// Missing context for the certain variable,
    MissingContext(OperatorDesc),
    /// The error of missing required operands in an arithmetic or logical expression.
    /// e.g., the plus expression requires two operands, and it is an error if less than two provided.
    MissingOperands(OperatorDesc),
    /// An error where an empty expression is to be evaluated
    EmptyExpression,
    /// Try to evaluate a const value or a variable but obtain `None` value
    NoneOperand(OperatorDesc),
    /// Meant to evaluate a certain operator, but obtain a different one
    UnmatchedOperator(OperatorDesc),
    /// Meant to evaluate a variable, but the data type is unexpected (e.g., not a graph-element)
    UnexpectedDataType(OperatorDesc),
    /// Get ``None` from `Context`
    GetNoneFromContext,
    /// Unsupported
    Unsupported(String),
    /// Other unknown errors that is converted from a error description
    OtherErr(String),
}

impl Display for ExprEvalError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use self::ExprEvalError::*;
        match self {
            CastError(e) => write!(f, "casting error {:?}", e),
            MissingContext(var) => write!(f, "missing context for {:?}", var),
            MissingOperands(opr) => write!(f, "missing operands for {:?}", opr),
            EmptyExpression => write!(f, "try to evaluate an empty expression"),
            NoneOperand(opr) => write!(f, "try to evaluate {:?} but obtain `None` value", opr),
            UnmatchedOperator(opr) => {
                write!(f, "meant to evaluate a certain operator, but obtain a different one: {:?}", opr)
            }
            UnexpectedDataType(opr) => {
                write!(f, "meant to evaluate a variable, but with unexpected data type: {:?}", opr)
            }
            GetNoneFromContext => write!(f, "get `None` from `Context`"),
            Unsupported(e) => write!(f, "unsupported: {}", e),
            OtherErr(e) => write!(f, "parse error {}", e),
        }
    }
}

impl std::error::Error for ExprEvalError {}

impl From<CastError> for ExprEvalError {
    fn from(error: CastError) -> Self {
        Self::CastError(error)
    }
}

impl From<&str> for ExprEvalError {
    fn from(str: &str) -> Self {
        Self::OtherErr(str.to_string())
    }
}
