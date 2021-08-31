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

use crate::expr::error::{ExprError, ExprResult};

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // Arithmetic
    Plus,    // +
    Minus,   // -
    Star,    // *
    Slash,   // /
    Percent, // %
    Hat,     // ^

    // Logical
    Eq,  // ==
    Ne,  // !=
    Gt,  // >
    Lt,  // <
    Ge,  // >=
    Le,  // <=
    And, // &&
    Or,  // ||
    Not, // !

    // Precedence
    LBrace, // (
    RBrace, // )

    // Values and Variables
    Identifier(String), // a string-identifier
    Float(f64),         // a float value
    Int(i64),           // an integer value
    Boolean(bool),      // a boolean value
    String(String),     // a string value
}

impl Token {
    pub fn is_operand(&self) -> bool {
        use crate::expr::token::Token::*;
        match self {
            Identifier(_) | Float(_) | Int(_) | Boolean(_) | String(_) => true,
            _ => false,
        }
    }

    pub fn is_left_brace(&self) -> bool {
        self == &Token::LBrace
    }

    pub fn is_right_brace(&self) -> bool {
        self == &Token::RBrace
    }

    /// Returns the precedence of the operator.
    /// A high precedence means that the operator has larger priority to get operated
    pub fn precedence(&self) -> i32 {
        use crate::expr::token::Token::*;
        match self {
            Plus | Minus => 95,
            Star | Slash | Percent => 100,
            Hat => 120,

            Eq | Ne | Gt | Lt | Ge | Le => 80,
            And => 75,
            Or => 70,
            Not => 110,

            LBrace | RBrace => 0,
            _ => 200,
        }
    }
}

/// A partial token is an input character whose meaning depends on the characters around it.
#[derive(Clone, Debug, PartialEq)]
pub enum PartialToken {
    /// A partial token that unambiguously maps to a single token.
    Token(Token),
    /// A partial token that is a literal.
    Literal(String),
    /// A whitespace character, e.g. ' '.
    Whitespace,
    /// An equal-to character '='.
    Eq,
    /// An exclamation mark character '!'.
    ExclamationMark,
    /// A greater-than character '>'.
    Gt,
    /// A lower-than character '<'.
    Lt,
    /// An ampersand character '&'.
    Ampersand,
    /// A vertical bar character '|'.
    VerticalBar,
}

// Make this a const fn as soon as is_whitespace and to_string get stable (issue #57563)
fn char_to_partial_token(c: char) -> PartialToken {
    match c {
        '=' => PartialToken::Eq,
        '!' => PartialToken::ExclamationMark,
        '>' => PartialToken::Gt,
        '<' => PartialToken::Lt,
        '&' => PartialToken::Ampersand,
        '|' => PartialToken::VerticalBar,

        '+' => PartialToken::Token(Token::Plus),
        '-' => PartialToken::Token(Token::Minus),
        '*' => PartialToken::Token(Token::Star),
        '/' => PartialToken::Token(Token::Slash),
        '%' => PartialToken::Token(Token::Percent),
        '^' => PartialToken::Token(Token::Hat),
        '(' => PartialToken::Token(Token::LBrace),
        ')' => PartialToken::Token(Token::RBrace),
        c => {
            if c.is_whitespace() {
                PartialToken::Whitespace
            } else {
                PartialToken::Literal(c.to_string())
            }
        }
    }
}

/// Parses an escape sequence within a string literal.
fn parse_escape_sequence<Iter: Iterator<Item = char>>(iter: &mut Iter) -> ExprResult<char> {
    match iter.next() {
        Some('"') => Ok('"'),
        Some('\\') => Ok('\\'),
        Some(c) => Err(ExprError::IllegalEscapeSequence(format!("\\{}", c))),
        None => Err(ExprError::IllegalEscapeSequence("\\".to_string())),
    }
}

/// Parses a string value from the given character iterator.
///
/// The first character from the iterator is interpreted as first character of the string.
/// The string is terminated by a double quote `"`.
/// Occurrences of `"` within the string can be escaped with `\`.
/// The backslash needs to be escaped with another backslash `\`.
fn parse_string_literal<Iter: Iterator<Item = char>>(
    mut iter: &mut Iter,
) -> ExprResult<PartialToken> {
    let mut result = String::new();

    while let Some(c) = iter.next() {
        match c {
            '"' => break,
            '\\' => result.push(parse_escape_sequence(&mut iter)?),
            c => result.push(c),
        }
    }

    Ok(PartialToken::Token(Token::String(result)))
}

/// Converts a string to a vector of partial tokens.
fn str_to_partial_tokens(string: &str) -> ExprResult<Vec<PartialToken>> {
    let mut result = Vec::new();
    let mut iter = string.chars().peekable();

    while let Some(c) = iter.next() {
        if c == '"' {
            result.push(parse_string_literal(&mut iter)?);
        } else {
            let partial_token = char_to_partial_token(c);

            let if_let_successful =
                if let (Some(PartialToken::Literal(last)), PartialToken::Literal(literal)) =
                    (result.last_mut(), &partial_token)
                {
                    last.push_str(literal);
                    true
                } else {
                    false
                };

            if !if_let_successful {
                result.push(partial_token);
            }
        }
    }
    Ok(result)
}

/// Resolves all partial tokens by converting them to complex tokens.
fn partial_tokens_to_tokens(mut tokens: &[PartialToken]) -> ExprResult<Vec<Token>> {
    let mut result = Vec::new();
    while !tokens.is_empty() {
        let first = tokens[0].clone();
        let second = tokens.get(1).cloned();
        let third = tokens.get(2).cloned();
        let mut cutoff = 2;

        result.extend(
            match first {
                PartialToken::Token(token) => {
                    cutoff = 1;
                    Some(token)
                }
                PartialToken::Literal(literal) => {
                    cutoff = 1;
                    if let Ok(number) = literal.parse::<i64>() {
                        Some(Token::Int(number))
                    } else if let Ok(number) = literal.parse::<f64>() {
                        Some(Token::Float(number))
                    } else if let Ok(boolean) = literal.parse::<bool>() {
                        Some(Token::Boolean(boolean))
                    } else {
                        // To parse the float of the form `<coefficient>e{+,-}<exponent>`,
                        // for example [Literal("10e"), Minus, Literal("3")] => "1e-3".parse().
                        match (second, third) {
                            (Some(second), Some(third))
                                if second == PartialToken::Token(Token::Minus)
                                    || second == PartialToken::Token(Token::Plus) =>
                            {
                                let second_sign = match second {
                                    PartialToken::Token(Token::Minus) => "-",
                                    _ => "+",
                                };
                                let third_num = match third {
                                    PartialToken::Literal(s) => s,
                                    _ => "".to_string(),
                                };
                                if let Ok(number) =
                                    format!("{}{}{}", literal, second_sign, third_num)
                                        .parse::<f64>()
                                {
                                    cutoff = 3;
                                    Some(Token::Float(number))
                                } else {
                                    Some(Token::Identifier(literal.to_string()))
                                }
                            }
                            _ => Some(Token::Identifier(literal.to_string())),
                        }
                    }
                }
                PartialToken::Whitespace => {
                    cutoff = 1;
                    None
                }
                PartialToken::Eq => match second {
                    Some(PartialToken::Eq) => Some(Token::Eq),
                    _ => {
                        return Err(ExprError::unmatched_partial_token(first, second));
                    }
                },
                PartialToken::ExclamationMark => match second {
                    Some(PartialToken::Eq) => Some(Token::Ne),
                    _ => {
                        cutoff = 1;
                        Some(Token::Not)
                    }
                },
                PartialToken::Gt => match second {
                    Some(PartialToken::Eq) => Some(Token::Ge),
                    _ => {
                        cutoff = 1;
                        Some(Token::Gt)
                    }
                },
                PartialToken::Lt => match second {
                    Some(PartialToken::Eq) => Some(Token::Le),
                    _ => {
                        cutoff = 1;
                        Some(Token::Lt)
                    }
                },
                PartialToken::Ampersand => match second {
                    Some(PartialToken::Ampersand) => Some(Token::And),
                    _ => return Err(ExprError::unmatched_partial_token(first, second)),
                },
                PartialToken::VerticalBar => match second {
                    Some(PartialToken::VerticalBar) => Some(Token::Or),
                    _ => return Err(ExprError::unmatched_partial_token(first, second)),
                },
            }
            .into_iter(),
        );

        tokens = &tokens[cutoff..];
    }
    Ok(result)
}

pub fn tokenize(string: &str) -> ExprResult<Vec<Token>> {
    partial_tokens_to_tokens(&str_to_partial_tokens(string)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        // ((1 + 2) * 2) ^ 3 == 6 ^ 3
        let case1 = tokenize("((1 + 1e-3) * 2) ^ 3 == 6 ^ 3").unwrap();
        let expected_case1 = vec![
            Token::LBrace,
            Token::LBrace,
            Token::Int(1),
            Token::Plus,
            Token::Float(0.001),
            Token::RBrace,
            Token::Star,
            Token::Int(2),
            Token::RBrace,
            Token::Hat,
            Token::Int(3),
            Token::Eq,
            Token::Int(6),
            Token::Hat,
            Token::Int(3),
        ];

        assert_eq!(case1, expected_case1);
    }

    #[test]
    fn test_errors() {
        // 1 = 1, the partial = must be completed by another =
        let case1 = tokenize("1 = 1");
        assert_eq!(
            case1.err().unwrap(),
            ExprError::unmatched_partial_token(PartialToken::Eq, Some(PartialToken::Whitespace))
        );

        let case2 = tokenize("1 & 2");
        assert_eq!(
            case2.err().unwrap(),
            ExprError::unmatched_partial_token(
                PartialToken::Ampersand,
                Some(PartialToken::Whitespace)
            )
        );

        let case3 = tokenize("1 | 2");
        assert_eq!(
            case3.err().unwrap(),
            ExprError::unmatched_partial_token(
                PartialToken::VerticalBar,
                Some(PartialToken::Whitespace)
            )
        );
    }
}
