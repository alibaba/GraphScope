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
pub mod token;

use std::convert::{TryFrom, TryInto};

use crate::expr_parse::error::{ExprError, ExprResult};
use crate::expr_parse::token::{tokenize, Token};
use crate::generated::common as pb;
use crate::VAR_PREFIX;

fn idents_to_vars(idents: Vec<String>) -> ExprResult<pb::VariableKeys> {
    let mut vars = Vec::with_capacity(idents.len());
    for ident in idents {
        if !ident.starts_with(VAR_PREFIX) {
            return Err(format!("invalid variable token: {:?}, a variable must start with \"@\"", ident)
                .as_str()
                .into());
        } else {
            let var: pb::Variable = ident.into();
            vars.push(var)
        }
    }

    Ok(pb::VariableKeys { keys: vars })
}

impl TryFrom<Token> for pb::ExprOpr {
    type Error = ExprError;

    fn try_from(token: Token) -> ExprResult<Self> {
        match token {
            Token::Plus => Ok(pb::Arithmetic::Add.into()),
            Token::Minus => Ok(pb::Arithmetic::Sub.into()),
            Token::Star => Ok(pb::Arithmetic::Mul.into()),
            Token::Slash => Ok(pb::Arithmetic::Div.into()),
            Token::Percent => Ok(pb::Arithmetic::Mod.into()),
            Token::Power => Ok(pb::Arithmetic::Exp.into()),
            Token::BitAnd => Ok(pb::Arithmetic::Bitand.into()),
            Token::BitOr => Ok(pb::Arithmetic::Bitor.into()),
            Token::BitXor => Ok(pb::Arithmetic::Bitxor.into()),
            Token::BitLShift => Ok(pb::Arithmetic::Bitlshift.into()),
            Token::BitRShift => Ok(pb::Arithmetic::Bitrshift.into()),
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
            Token::StartsWith => Ok(pb::Logical::Startswith.into()),
            Token::EndsWith => Ok(pb::Logical::Endswith.into()),
            Token::Boolean(b) => Ok(pb::Value::from(b).into()),
            Token::Int(i) => Ok(pb::Value::from(i).into()),
            Token::Float(f) => Ok(pb::Value::from(f).into()),
            Token::String(s) => Ok(pb::Value::from(s).into()),
            Token::IntArray(v) => Ok(pb::Value::from(v).into()),
            Token::FloatArray(v) => Ok(pb::Value::from(v).into()),
            Token::StrArray(v) => Ok(pb::Value::from(v).into()),
            Token::LBrace => Ok(pb::ExprOpr { item: Some(pb::expr_opr::Item::Brace(0)) }),
            Token::RBrace => Ok(pb::ExprOpr { item: Some(pb::expr_opr::Item::Brace(1)) }),
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
            Token::IdentArray(idents) => Ok((idents_to_vars(idents)?, false).into()),
            Token::IdentMap(idents) => Ok((idents_to_vars(idents)?, true).into()),
        }
    }
}

pub trait ExprToken {
    fn is_operand(&self) -> bool;

    fn is_left_brace(&self) -> bool;

    fn is_right_brace(&self) -> bool;

    /// Returns the precedence of the operator.
    /// A high precedence means that the operator has larger priority to get operated
    fn precedence(&self) -> i32;
}

impl ExprToken for pb::ExprOpr {
    fn is_operand(&self) -> bool {
        match self.item {
            Some(pb::expr_opr::Item::Const(_)) => true,
            Some(pb::expr_opr::Item::Var(_)) => true,
            _ => false,
        }
    }

    fn is_left_brace(&self) -> bool {
        match self.item {
            Some(pb::expr_opr::Item::Brace(i)) => i == 0,
            _ => false,
        }
    }

    fn is_right_brace(&self) -> bool {
        match self.item {
            Some(pb::expr_opr::Item::Brace(i)) => i == 1,
            _ => false,
        }
    }

    fn precedence(&self) -> i32 {
        use pb::expr_opr::Item::*;
        if self.item.is_some() {
            match self.item.as_ref().unwrap() {
                &Arith(a) => {
                    let arith = unsafe { std::mem::transmute::<i32, pb::Arithmetic>(a) };
                    match arith {
                        pb::Arithmetic::Add | pb::Arithmetic::Sub => 95,
                        pb::Arithmetic::Mul | pb::Arithmetic::Div | pb::Arithmetic::Mod => 100,
                        pb::Arithmetic::Exp => 120,
                        pb::Arithmetic::Bitlshift | pb::Arithmetic::Bitrshift => 130,
                        pb::Arithmetic::Bitand | pb::Arithmetic::Bitor | pb::Arithmetic::Bitxor => 140,
                    }
                }
                &Logical(l) => {
                    let logical = unsafe { std::mem::transmute::<i32, pb::Logical>(l) };
                    match logical {
                        pb::Logical::Eq
                        | pb::Logical::Ne
                        | pb::Logical::Lt
                        | pb::Logical::Le
                        | pb::Logical::Gt
                        | pb::Logical::Ge
                        | pb::Logical::Within
                        | pb::Logical::Without
                        | pb::Logical::Startswith
                        | pb::Logical::Endswith => 80,
                        pb::Logical::And => 75,
                        pb::Logical::Or => 70,
                        pb::Logical::Not => 110,
                    }
                }
                &Brace(_) => 0,
                _ => 200,
            }
        } else {
            -1
        }
    }
}

#[allow(dead_code)]
/// Turn a sequence of tokens with bracket, into a suffix order
pub fn to_suffix_expr<E: ExprToken + std::fmt::Debug>(expr: Vec<E>) -> ExprResult<Vec<E>> {
    let mut stack: Vec<E> = Vec::with_capacity(expr.len());
    let mut results: Vec<E> = Vec::with_capacity(expr.len());

    for token in expr {
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

pub fn str_to_expr_pb(expr_str: String) -> ExprResult<pb::Expression> {
    let mut operators = vec![];
    for token in tokenize(&expr_str)? {
        operators.push(token.try_into()?);
    }

    Ok(pb::Expression { operators })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr_parse::token::tokenize;

    #[test]
    fn test_to_suffix_expr() {
        // 1 + 2
        let case1 = tokenize("1 + 2").unwrap();
        let expected_case1 = vec![Token::Int(1), Token::Int(2), Token::Plus];
        assert_eq!(to_suffix_expr(case1).unwrap(), expected_case1);

        // 1 + 2 == 3
        let case2 = tokenize("1 + 2 == 3").unwrap();
        let expected_case2 = vec![Token::Int(1), Token::Int(2), Token::Plus, Token::Int(3), Token::Eq];
        assert_eq!(to_suffix_expr(case2).unwrap(), expected_case2);

        // 1 + 2 * 3
        let case3 = tokenize("1 + 2 * 3").unwrap();
        let expected_case3 = vec![Token::Int(1), Token::Int(2), Token::Int(3), Token::Star, Token::Plus];
        assert_eq!(to_suffix_expr(case3).unwrap(), expected_case3);

        // (1 + 2) * 3
        let case4 = tokenize("(1 + 2) * 3").unwrap();
        let expected_case4 = vec![Token::Int(1), Token::Int(2), Token::Plus, Token::Int(3), Token::Star];
        assert_eq!(to_suffix_expr(case4).unwrap(), expected_case4);

        // 1 + 2 ^ 3 > 6
        let case5 = tokenize(" 1 + 2 ^^ 3 > 6").unwrap();
        let expected_case5 = vec![
            Token::Int(1),
            Token::Int(2),
            Token::Int(3),
            Token::Power,
            Token::Plus,
            Token::Int(6),
            Token::Gt,
        ];
        assert_eq!(to_suffix_expr(case5).unwrap(), expected_case5);

        // (1 + 2) ^ 3 > 6
        let case6 = tokenize("(1 + 2) ^^ 3 > 6").unwrap();
        let expected_case6 = vec![
            Token::Int(1),
            Token::Int(2),
            Token::Plus,
            Token::Int(3),
            Token::Power,
            Token::Int(6),
            Token::Gt,
        ];
        assert_eq!(to_suffix_expr(case6).unwrap(), expected_case6);

        // ((1 + 2) * 2) ^ 3 == 6 ^ 3
        let case7 = tokenize("((1 + 1e-3) * 2) ^^ 3 == 6 ^^ 3").unwrap();
        let expected_case7 = vec![
            Token::Int(1),
            Token::Float(0.001),
            Token::Plus,
            Token::Int(2),
            Token::Star,
            Token::Int(3),
            Token::Power,
            Token::Int(6),
            Token::Int(3),
            Token::Power,
            Token::Eq,
        ];
        assert_eq!(to_suffix_expr(case7).unwrap(), expected_case7);
    }
}
