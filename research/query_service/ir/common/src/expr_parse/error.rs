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

use std::fmt::Display;

use crate::error::ParsePbError;
use crate::expr_parse::token::PartialToken;

pub type ExprResult<T> = Result<T, ExprError>;

/// The error cases while parsing and evaluating expressions
#[derive(Clone, Debug, PartialEq)]
pub enum ExprError {
    /// The left brace may not be closed by a right brace
    UnmatchedLRBraces,
    /// The left bracket may not be closed by a right braket
    UnmatchedLRBrackets,
    /// An escape sequence within a string literal is illegal.
    IllegalEscapeSequence(String),
    /// A `PartialToken` is unmatched, such that it cannot be combined into a full `Token`.
    /// For example, '&' is a partial token, and it can be a full token if there is another
    /// '&&' that represents logical and, same applies to '|' ('||'), '=' ('>=', '<=', '==').  
    UnmatchedPartialToken {
        /// The unmatched partial token.
        first: PartialToken,
        /// The token that follows the unmatched partial token and that cannot be matched to the
        /// partial token, or `None`, if `first` is the last partial token in the stream.
        second: Option<PartialToken>,
    },
    /// Parse from protobuf error
    ParsePbError(ParsePbError),
    /// Unsupported
    Unsupported(String),
    /// Other unknown errors that is converted from a error description
    OtherErr(String),
}

impl Display for ExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use super::ExprError::*;
        match self {
            UnmatchedLRBraces => write!(f, "the left and right braces may not be matched"),
            UnmatchedLRBrackets => write!(f, "the left and right brackets may not be matched"),
            IllegalEscapeSequence(s) => write!(f, "illegal escape sequence {:?}", s),
            UnmatchedPartialToken { first: s1, second: s2 } => {
                write!(f, "partial token {:?} cannot be completed by {:?}", s1, s2)
            }
            ParsePbError(e) => {
                write!(f, "parse from pb error {:?}", e)
            }
            Unsupported(e) => write!(f, "unsupported: {}", e),
            OtherErr(e) => write!(f, "parse error {}", e),
        }
    }
}

impl std::error::Error for ExprError {}

impl ExprError {
    pub fn unmatched_partial_token(first: PartialToken, second: Option<PartialToken>) -> Self {
        Self::UnmatchedPartialToken { first, second }
    }

    pub fn unsupported(string: String) -> Self {
        Self::Unsupported(string)
    }
}

impl From<ParsePbError> for ExprError {
    fn from(error: ParsePbError) -> Self {
        Self::ParsePbError(error)
    }
}

impl From<&str> for ExprError {
    fn from(e: &str) -> Self {
        Self::OtherErr(e.into())
    }
}
