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

use crate::expr::to_suffix_expr_pb;
use crate::expr::token::{tokenize, Token};
use crate::generated::common as pb;

#[no_mangle]
pub extern "C" fn tokenize_unchecked(str: &str) -> Vec<Token> {
    let rst = tokenize(str);
    if let Ok(tokens) = rst {
        tokens
    } else {
        panic!(
            "Parsing expression into tokens encounters error: {:?}",
            rst.err()
        )
    }
}

/// Initiate for the caller to build a tokenized expression from scratch. For example,
/// an expression like: "1 + 2 < 3" can be built as follows:
/// `std::vec<_> expr = init_expr();`
/// `add_integer(1, &expr);`
/// `add_plus(&expr);`
/// `add_integer(2, &expr);`
/// `add_lt(&expr);`
/// `add_integer(3, &expr);`
///
/// Possible tokens and their functions are listed in the following.
#[no_mangle]
pub extern "C" fn init_expr() -> Vec<Token> {
    Vec::<Token>::new()
}

#[no_mangle]
pub extern "C" fn add_integer(val: i64, tokens: &mut Vec<Token>) {
    tokens.push(Token::Int(val))
}

#[no_mangle]
pub extern "C" fn add_double(val: f64, tokens: &mut Vec<Token>) {
    tokens.push(Token::Float(val))
}

#[no_mangle]
pub extern "C" fn add_bool(b: bool, tokens: &mut Vec<Token>) {
    tokens.push(Token::Boolean(b))
}

#[no_mangle]
pub extern "C" fn add_string(str: String, tokens: &mut Vec<Token>) {
    tokens.push(Token::String(str))
}

/// Add a variable that must start with '@'. There are a couple of variances
/// 1. @i, where i is a number inidicating the position index in an array
///  1.1 @i.j, where j is an integer that encodes a given property's name
///  1.2 @i.x, where x is a string-typed name that indicates a given property's name
/// 2. @x, where x is a string-typed name, indicating a tag name in a key-value-like structure
///  2.1 @x.j, where j is an integer that encodes a given property's name
///  2.2 @x.y where y is a string-typed name that indicates a given property's name
#[no_mangle]
pub extern "C" fn add_variable(str: String, tokens: &mut Vec<Token>) {
    tokens.push(Token::Identifier(str))
}

/// '+'
pub extern "C" fn add_plus(tokens: &mut Vec<Token>) {
    tokens.push(Token::Plus)
}

/// '-'
pub extern "C" fn add_minus(tokens: &mut Vec<Token>) {
    tokens.push(Token::Minus)
}

/// '*'
pub extern "C" fn add_star(tokens: &mut Vec<Token>) {
    tokens.push(Token::Star)
}

/// '/'
pub extern "C" fn add_slash(tokens: &mut Vec<Token>) {
    tokens.push(Token::Slash)
}

/// '^', exponential
pub extern "C" fn add_hat(tokens: &mut Vec<Token>) {
    tokens.push(Token::Hat)
}

/// '%', modulo
pub extern "C" fn add_percent(tokens: &mut Vec<Token>) {
    tokens.push(Token::Percent)
}

/// '('
pub extern "C" fn add_lbrace(tokens: &mut Vec<Token>) {
    tokens.push(Token::LBrace)
}

/// ')'
pub extern "C" fn add_rbrace(tokens: &mut Vec<Token>) {
    tokens.push(Token::RBrace)
}

/// Logical and
pub extern "C" fn add_land(tokens: &mut Vec<Token>) {
    tokens.push(Token::And)
}

/// Logical or
pub extern "C" fn add_lor(tokens: &mut Vec<Token>) {
    tokens.push(Token::Or)
}

/// Logical not
pub extern "C" fn add_not(tokens: &mut Vec<Token>) {
    tokens.push(Token::Not)
}

/// Greater than
pub extern "C" fn add_gt(tokens: &mut Vec<Token>) {
    tokens.push(Token::Gt)
}

/// Greater than and equal
pub extern "C" fn add_ge(tokens: &mut Vec<Token>) {
    tokens.push(Token::Ge)
}

/// Less than
pub extern "C" fn add_lt(tokens: &mut Vec<Token>) {
    tokens.push(Token::lt)
}

/// Less than and equal
pub extern "C" fn add_le(tokens: &mut Vec<Token>) {
    tokens.push(Token::le)
}

/// Equal
pub extern "C" fn add_eq(tokens: &mut Vec<Token>) {
    tokens.push(Token::Eq)
}

/// Not equal
pub extern "C" fn add_ne(tokens: &mut Vec<Token>) {
    tokens.push(Token::Ne)
}

#[no_mangle]
pub extern "C" fn tokens_to_suffix_expr_pb(tokens: Vec<Token>) -> Vec<pb::ExprUnit> {
    let rst = to_suffix_expr_pb(tokens);
    if let Ok(suffix_expr) = rst {
        suffix_expr
    } else {
        panic!(
            "Parsing tokens into suffix tree encounters error: {:?}",
            rst.err()
        )
    }
}
