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

pub mod error;
pub mod eval;
pub mod token;

use std::convert::{TryFrom, TryInto};

use ir_common::generated::common as pb;
use ir_common::VAR_PREFIX;

use crate::expr::error::{ExprError, ExprResult};
use crate::expr::token::{tokenize, Token};

impl TryFrom<Token> for pb::ExprOpr {
    type Error = ExprError;

    fn try_from(token: Token) -> ExprResult<Self> {
        match token {
            Token::Plus => Ok(pb::Arithmetic::Add.into()),
            Token::Minus => Ok(pb::Arithmetic::Sub.into()),
            Token::Star => Ok(pb::Arithmetic::Mul.into()),
            Token::Slash => Ok(pb::Arithmetic::Div.into()),
            Token::Percent => Ok(pb::Arithmetic::Mod.into()),
            Token::Hat => Ok(pb::Arithmetic::Exp.into()),
            Token::Eq => Ok(pb::Logical::Eq.into()),
            Token::Ne => Ok(pb::Logical::Ne.into()),
            Token::Gt => Ok(pb::Logical::Gt.into()),
            Token::Lt => Ok(pb::Logical::Lt.into()),
            Token::Ge => Ok(pb::Logical::Ge.into()),
            Token::Le => Ok(pb::Logical::Le.into()),
            Token::And => Ok(pb::Logical::And.into()),
            Token::Or => Ok(pb::Logical::Or.into()),
            Token::Not => Ok(pb::Logical::Not.into()),
            Token::Within => Ok(pb::Logical::Within.into()),
            Token::Without => Ok(pb::Logical::Without.into()),
            Token::Boolean(b) => Ok(pb::Const { value: Some(b.into()) }.into()),
            Token::Int(i) => Ok(pb::Const { value: Some(i.into()) }.into()),
            Token::Float(f) => Ok(pb::Const { value: Some(f.into()) }.into()),
            Token::String(s) => Ok(pb::Const { value: Some(s.into()) }.into()),
            Token::IntArray(v) => Ok(pb::Const { value: Some(v.into()) }.into()),
            Token::FloatArray(v) => Ok(pb::Const { value: Some(v.into()) }.into()),
            Token::StrArray(v) => Ok(pb::Const { value: Some(v.into()) }.into()),
            Token::Identifier(ident) => {
                if !ident.starts_with(VAR_PREFIX) {
                    Err(format!("invalid variable token: {:?}, a variable must start with \"@\"", ident)
                        .as_str()
                        .into())
                } else {
                    let var: pb::Variable = ident.into();
                    Ok(var.into())
                }
            }
            _ => Err(format!("invalid token {:?}", token)
                .as_str()
                .into()),
        }
    }
}

/// Turn a sequence of tokens with bracket, into a suffix order
fn to_suffix_tokens(tokens: Vec<Token>) -> ExprResult<Vec<Token>> {
    let mut stack: Vec<Token> = Vec::with_capacity(tokens.len());
    let mut results: Vec<Token> = Vec::with_capacity(tokens.len());

    for token in tokens {
        if token.is_operand() {
            results.push(token);
        } else if token.is_left_brace() {
            stack.push(token);
        } else if token.is_right_brace() {
            let mut is_left_brace = false;
            while !stack.is_empty() {
                let recent = stack.pop().unwrap();
                if recent.is_left_brace() {
                    is_left_brace = true;
                    break;
                } else {
                    results.push(recent);
                }
            }
            if !is_left_brace {
                return Err(ExprError::UnmatchedLRBraces);
            }
        } else {
            // the operator
            if stack.is_empty() {
                stack.push(token);
            } else {
                while !stack.is_empty() && stack.last().unwrap().precedence() >= token.precedence() {
                    results.push(stack.pop().unwrap());
                }
                stack.push(token);
            }
        }
    }

    while !stack.is_empty() {
        results.push(stack.pop().unwrap());
    }
    Ok(results)
}

pub fn to_suffix_expr_pb(tokens: Vec<Token>) -> ExprResult<pb::SuffixExpr> {
    let tokens = to_suffix_tokens(tokens)?;
    let mut operators = Vec::<pb::ExprOpr>::with_capacity(tokens.len());

    for token in tokens {
        operators.push(token.try_into()?);
    }

    Ok(pb::SuffixExpr { operators })
}

pub fn str_to_expr_pb(expr_str: String) -> ExprResult<pb::SuffixExpr> {
    to_suffix_expr_pb(tokenize(&expr_str)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::token::tokenize;

    #[test]
    fn test_to_suffix_tokens() {
        // 1 + 2
        let case1 = tokenize("1 + 2").unwrap();
        let expected_case1 = vec![Token::Int(1), Token::Int(2), Token::Plus];
        assert_eq!(to_suffix_tokens(case1).unwrap(), expected_case1);

        // 1 + 2 == 3
        let case2 = tokenize("1 + 2 == 3").unwrap();
        let expected_case2 = vec![Token::Int(1), Token::Int(2), Token::Plus, Token::Int(3), Token::Eq];
        assert_eq!(to_suffix_tokens(case2).unwrap(), expected_case2);

        // 1 + 2 * 3
        let case3 = tokenize("1 + 2 * 3").unwrap();
        let expected_case3 = vec![Token::Int(1), Token::Int(2), Token::Int(3), Token::Star, Token::Plus];
        assert_eq!(to_suffix_tokens(case3).unwrap(), expected_case3);

        // (1 + 2) * 3
        let case4 = tokenize("(1 + 2) * 3").unwrap();
        let expected_case4 = vec![Token::Int(1), Token::Int(2), Token::Plus, Token::Int(3), Token::Star];
        assert_eq!(to_suffix_tokens(case4).unwrap(), expected_case4);

        // 1 + 2 ^ 3 > 6
        let case5 = tokenize(" 1 + 2 ^ 3 > 6").unwrap();
        let expected_case5 = vec![
            Token::Int(1),
            Token::Int(2),
            Token::Int(3),
            Token::Hat,
            Token::Plus,
            Token::Int(6),
            Token::Gt,
        ];
        assert_eq!(to_suffix_tokens(case5).unwrap(), expected_case5);

        // (1 + 2) ^ 3 > 6
        let case6 = tokenize("(1 + 2) ^ 3 > 6").unwrap();
        let expected_case6 = vec![
            Token::Int(1),
            Token::Int(2),
            Token::Plus,
            Token::Int(3),
            Token::Hat,
            Token::Int(6),
            Token::Gt,
        ];
        assert_eq!(to_suffix_tokens(case6).unwrap(), expected_case6);

        // ((1 + 2) * 2) ^ 3 == 6 ^ 3
        let case7 = tokenize("((1 + 1e-3) * 2) ^ 3 == 6 ^ 3").unwrap();
        let expected_case7 = vec![
            Token::Int(1),
            Token::Float(0.001),
            Token::Plus,
            Token::Int(2),
            Token::Star,
            Token::Int(3),
            Token::Hat,
            Token::Int(6),
            Token::Int(3),
            Token::Hat,
            Token::Eq,
        ];
        assert_eq!(to_suffix_tokens(case7).unwrap(), expected_case7);
    }
}
