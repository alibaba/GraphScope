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

use crate::expr_parse::error::{ExprError, ExprResult};
use crate::expr_parse::ExprToken;

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // Arithmetic
    Plus,      // +
    Minus,     // -
    Star,      // *
    Slash,     // /
    Percent,   // %
    Power,     // ^^
    BitAnd,    // &
    BitOr,     // |
    BitXor,    // ^
    BitLShift, // <<
    BitRShift, // >>

    // Logical
    Eq,         // ==
    Ne,         // !=
    Gt,         // >
    Lt,         // <
    Ge,         // >=
    Le,         // <=
    And,        // &&
    Or,         // ||
    Not,        // !
    Within,     // Within
    Without,    // Without
    StartsWith, // String StartsWith
    EndsWith,   // String EndsWith

    // Precedence
    LBrace, // (
    RBrace, // )

    // Values and Variables
    Identifier(String),      // a string-identifier
    Boolean(bool),           // a boolean value
    Int(i64),                // an integer value
    Float(f64),              // a float value
    String(String),          // a string value
    IntArray(Vec<i64>),      // a integer array
    FloatArray(Vec<f64>),    // a float array
    StrArray(Vec<String>),   // a string array
    IdentArray(Vec<String>), // an identifier array
    IdentMap(Vec<String>),   // an identifier map
}

impl ExprToken for Token {
    #[inline]
    fn is_operand(&self) -> bool {
        use crate::expr_parse::token::Token::*;
        match self {
            Identifier(_) | Float(_) | Int(_) | Boolean(_) | String(_) | IntArray(_) | FloatArray(_)
            | StrArray(_) | IdentArray(_) | IdentMap(_) => true,
            _ => false,
        }
    }

    #[inline]
    fn is_left_brace(&self) -> bool {
        self == &Token::LBrace
    }

    #[inline]
    fn is_right_brace(&self) -> bool {
        self == &Token::RBrace
    }

    #[inline]
    fn precedence(&self) -> i32 {
        use crate::expr_parse::token::Token::*;
        match self {
            Plus | Minus => 95,
            Star | Slash | Percent => 100,
            Power => 120,

            Eq | Ne | Gt | Lt | Ge | Le | Within | Without | StartsWith | EndsWith => 80,
            And => 75,
            Or => 70,
            Not => 110,
            BitLShift | BitRShift => 130,
            BitAnd | BitOr => 140,

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
    /// A partial token that is a minus, which may be a Sub, or a Negative sign.
    Minus,
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
    /// To initiate an array, '['
    LBracket,
    /// To terminate an array, ']'
    RBracket,
    /// To initiate a map, '{'
    LCBracket,
    /// To terminate a map, '}'
    RCBracket,
    /// A '^' character
    Hat,
}

#[inline]
fn char_to_partial_token(c: char) -> PartialToken {
    match c {
        '=' => PartialToken::Eq,
        '!' => PartialToken::ExclamationMark,
        '>' => PartialToken::Gt,
        '<' => PartialToken::Lt,
        '&' => PartialToken::Ampersand,
        '|' => PartialToken::VerticalBar,
        '-' => PartialToken::Minus,
        '[' => PartialToken::LBracket,
        ']' => PartialToken::RBracket,
        '{' => PartialToken::LCBracket,
        '}' => PartialToken::RCBracket,
        '^' => PartialToken::Hat,
        '+' => PartialToken::Token(Token::Plus),
        '*' => PartialToken::Token(Token::Star),
        '/' => PartialToken::Token(Token::Slash),
        '%' => PartialToken::Token(Token::Percent),
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
/// Either in the case of an array, in which the whole literal is enclosed by a pair of brackets [],
/// or it is a typical string literal in a quote, in this case:
///   * The first character from the iterator is interpreted as first character of the string.
///   * The string is terminated by a double quote `"`.
///   * Occurrences of `"` within the string can be escaped with `\`.
///   * The backslash needs to be escaped with another backslash `\`.
fn parse_string_literal<Iter: Iterator<Item = char>>(
    mut iter: &mut Iter, is_in_bracket: bool,
) -> ExprResult<PartialToken> {
    let mut result = String::new();
    if !is_in_bracket {
        while let Some(c) = iter.next() {
            match c {
                '"' => break,
                '\\' => result.push(parse_escape_sequence(&mut iter)?),
                c => result.push(c),
            }
        }
    } else {
        let mut has_right_bracket = false;
        // Treat everything as a string
        while let Some(c) = iter.next() {
            if c == ']' || c == '}' {
                has_right_bracket = true;
                break;
            } else if c == '[' || c == '{' {
                return Err(ExprError::unsupported("nested array is not supported".to_string()));
            } else {
                result.push(c);
            }
        }

        if !has_right_bracket {
            return Err(ExprError::UnmatchedLRBrackets);
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
            result.push(parse_string_literal(&mut iter, false)?);
        } else if c == '[' {
            result.push(PartialToken::LBracket);
            result.push(parse_string_literal(&mut iter, true)?);
            // must have right bracket to escape from `parse_string_literal()`
            result.push(PartialToken::RBracket);
        } else if c == '{' {
            result.push(PartialToken::LCBracket);
            result.push(parse_string_literal(&mut iter, true)?);
            // must have right bracket to escape from `parse_string_literal()`
            result.push(PartialToken::RCBracket);
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

fn token_array_to_token(token_array: Vec<Token>) -> ExprResult<Token> {
    if token_array.is_empty() {
        Ok(Token::IdentArray(vec![]))
    } else {
        // use pivot to regulate the type of all elements
        let pivot = token_array.first().unwrap();
        match pivot {
            Token::Int(_) => {
                let mut vec = Vec::with_capacity(token_array.len());
                for t in token_array {
                    match t {
                        Token::Int(i) => vec.push(i),
                        _ => {
                            return Err(ExprError::unsupported(
                                "array of various type unsupported".to_string(),
                            ))
                        }
                    }
                }
                Ok(Token::IntArray(vec))
            }
            Token::Float(_) => {
                let mut vec = Vec::with_capacity(token_array.len());
                for t in token_array {
                    match t {
                        Token::Float(f) => vec.push(f),
                        _ => {
                            return Err(ExprError::unsupported(
                                "array of various type unsupported".to_string(),
                            ))
                        }
                    }
                }
                Ok(Token::FloatArray(vec))
            }
            Token::String(_) => {
                let mut vec = Vec::with_capacity(token_array.len());
                for t in token_array {
                    match t {
                        Token::String(s) => vec.push(s),
                        _ => {
                            return Err(ExprError::unsupported(
                                "array of various type unsupported".to_string(),
                            ))
                        }
                    }
                }
                Ok(Token::StrArray(vec))
            }
            Token::Identifier(_) => {
                let mut vec = Vec::with_capacity(token_array.len());
                for t in token_array {
                    match t {
                        Token::Identifier(s) => vec.push(s),
                        _ => {
                            return Err(ExprError::unsupported(
                                "array of various type unsupported".to_string(),
                            ))
                        }
                    }
                }
                Ok(Token::IdentArray(vec))
            }
            _ => Err(ExprError::unsupported(format!("array of this type: {:?} is not supported", pivot))),
        }
    }
}

/// Resolves all partial tokens by converting them to complex tokens.
fn partial_tokens_to_tokens(mut tokens: &[PartialToken]) -> ExprResult<Vec<Token>> {
    let mut result = Vec::new();
    let mut recent_token: Option<Token> = None;
    while !tokens.is_empty() {
        let first = tokens[0].clone();
        let second = tokens.get(1).cloned();
        let third = tokens.get(2).cloned();
        let mut cutoff = 2;

        let curr_token = match first {
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
                } else if literal.to_lowercase().as_str() == "within" {
                    Some(Token::Within)
                } else if literal.to_lowercase().as_str() == "without" {
                    Some(Token::Without)
                } else if literal.to_lowercase().as_str() == "startswith" {
                    Some(Token::StartsWith)
                } else if literal.to_lowercase().as_str() == "endswith" {
                    Some(Token::EndsWith)
                } else {
                    // To parse the float of the form `<coefficient>e{+,-}<exponent>`,
                    // for example [Literal("10e"), Minus, Literal("3")] => "1e-3".parse().
                    match (second, third) {
                        (Some(second), Some(third))
                            if second == PartialToken::Minus
                                || second == PartialToken::Token(Token::Plus) =>
                        {
                            let second_sign = match second {
                                PartialToken::Minus => "-",
                                _ => "+",
                            };
                            let third_num = match third {
                                PartialToken::Literal(s) => s,
                                _ => "".to_string(),
                            };
                            if let Ok(number) =
                                format!("{}{}{}", literal, second_sign, third_num).parse::<f64>()
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
            PartialToken::Minus => {
                // Should we consider minus as a negative sign
                let is_negative_sign =
                    { recent_token.is_none() || !recent_token.as_ref().unwrap().is_operand() };
                if is_negative_sign {
                    match &second {
                        // Be aware that minus can represent both subtraction or negative sign
                        // if it is a negative sign, it must be directly trailed by a number.
                        // However, we can not actually tell whether the case "x -y", is actually
                        // subtracting x by y, or -y must be treated as a number.
                        Some(PartialToken::Literal(literal)) => {
                            // Must check whether previous is what
                            if let Ok(number) = literal.parse::<i64>() {
                                Some(Token::Int(-number))
                            } else if let Ok(number) = literal.parse::<f64>() {
                                Some(Token::Float(-number))
                            } else {
                                return Err(ExprError::unmatched_partial_token(first, second));
                            }
                        }
                        _ => {
                            cutoff = 1;
                            Some(Token::Minus)
                        }
                    }
                } else {
                    cutoff = 1;
                    Some(Token::Minus)
                }
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
                Some(PartialToken::Gt) => Some(Token::BitRShift), // >>
                _ => {
                    cutoff = 1;
                    Some(Token::Gt)
                }
            },
            PartialToken::Lt => match second {
                Some(PartialToken::Eq) => Some(Token::Le),
                Some(PartialToken::Lt) => Some(Token::BitLShift), // <<
                _ => {
                    cutoff = 1;
                    Some(Token::Lt)
                }
            },
            PartialToken::Ampersand => match second {
                Some(PartialToken::Ampersand) => Some(Token::And),
                // _ => return Err(ExprError::unmatched_partial_token(first, second)),
                _ => {
                    cutoff = 1;
                    Some(Token::BitAnd)
                }
            },
            PartialToken::VerticalBar => match second {
                Some(PartialToken::VerticalBar) => Some(Token::Or),
                // _ => return Err(ExprError::unmatched_partial_token(first, second)),
                _ => {
                    cutoff = 1;
                    Some(Token::BitOr)
                }
            },
            PartialToken::LBracket | PartialToken::LCBracket => {
                let is_bracket = first == PartialToken::LBracket;
                cutoff = 3;
                if (is_bracket && third != Some(PartialToken::RBracket))
                    || (!is_bracket && third != Some(PartialToken::RCBracket))
                {
                    return Err(ExprError::UnmatchedLRBrackets);
                } else {
                    let mut token_array: Vec<Token> = Vec::new();
                    match second {
                        Some(PartialToken::Token(Token::String(ref s))) => {
                            let elements = s.split(",");
                            for e in elements {
                                let t = partial_tokens_to_tokens(&str_to_partial_tokens(e)?)?;
                                if t.is_empty() {
                                    // do nothing
                                } else if t.len() == 1 {
                                    token_array.push(t[0].clone())
                                } else {
                                    return Err(format!("invalid token: {:?}", second)
                                        .as_str()
                                        .into());
                                }
                            }
                        }
                        _ => {
                            return Err(format!("invalid token: {:?}", second)
                                .as_str()
                                .into())
                        }
                    }
                    let result = token_array_to_token(token_array)?;
                    if is_bracket {
                        Some(result)
                    } else {
                        if let Token::IdentArray(vec) = result {
                            Some(Token::IdentMap(vec))
                        } else {
                            unreachable!()
                        }
                    }
                }
            }
            PartialToken::Hat => match second {
                Some(PartialToken::Hat) => Some(Token::Power),
                _ => {
                    cutoff = 1;
                    Some(Token::BitXor)
                }
            },
            _ => {
                return Err(format!("invalid token: {:?}", first)
                    .as_str()
                    .into());
            }
        };

        if let Some(token) = curr_token.clone() {
            result.push(token);
            recent_token = curr_token.clone();
        }

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
        let case1 = tokenize("((1 + 1e-3) * 2) ^^ 3 == 6 ^^ 3").unwrap();
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
            Token::Power,
            Token::Int(3),
            Token::Eq,
            Token::Int(6),
            Token::Power,
            Token::Int(3),
        ];

        assert_eq!(case1, expected_case1);

        let case2 = tokenize("1 - 2").unwrap();
        let expected_case2 = vec![Token::Int(1), Token::Minus, Token::Int(2)];
        assert_eq!(case2, expected_case2);

        let case3 = tokenize("1 + (-2)").unwrap();
        let expected_case3 = vec![Token::Int(1), Token::Plus, Token::LBrace, Token::Int(-2), Token::RBrace];
        assert_eq!(case3, expected_case3);

        let case4 = tokenize("1 + -2 + 2").unwrap();
        let expected_case4 = vec![Token::Int(1), Token::Plus, Token::Int(-2), Token::Plus, Token::Int(2)];
        assert_eq!(case4, expected_case4);
    }

    #[test]
    fn test_tokenize_array() {
        let case1 = tokenize("[1, 2, 3, 4]").unwrap();
        let expected_case1 = vec![Token::IntArray(vec![1, 2, 3, 4])];
        assert_eq!(case1, expected_case1);

        let case2 = tokenize("1 within [1, 2, 3, 4]").unwrap();
        let expected_case2 = vec![Token::Int(1), Token::Within, Token::IntArray(vec![1, 2, 3, 4])];
        assert_eq!(case2, expected_case2);

        let case3 = tokenize("[1.0] without [1.0, 2.0, 3.0, 4.0]").unwrap();
        let expected_case3 =
            vec![Token::FloatArray(vec![1.0]), Token::Without, Token::FloatArray(vec![1.0, 2.0, 3.0, 4.0])];
        assert_eq!(case3, expected_case3);

        let case4 = tokenize("\"a\" within [\"a\", \"b\"]").unwrap();
        let expected_case4 = vec![
            Token::String("a".to_string()),
            Token::Within,
            Token::StrArray(vec!["a".to_string(), "b".to_string()]),
        ];
        assert_eq!(case4, expected_case4);

        let case5 = tokenize("[1, -2, 3, -4]").unwrap();
        let expected_case5 = vec![Token::IntArray(vec![1, -2, 3, -4])];
        assert_eq!(case5, expected_case5);

        let case6 = tokenize("-4.0 within [1.0, -2.0, 3.0, -4.0]").unwrap();
        let expected_case6 =
            vec![Token::Float(-4.0), Token::Within, Token::FloatArray(vec![1.0, -2.0, 3.0, -4.0])];
        assert_eq!(case6, expected_case6);

        let case7 = tokenize("[@a, @a.name, @.age]");
        let expected_case7 =
            vec![Token::IdentArray(vec!["@a".to_string(), "@a.name".to_string(), "@.age".to_string()])];
        assert_eq!(case7.unwrap(), expected_case7,);

        let case8 = tokenize("{@a, @a.name, @.age}");
        let expected_case8 =
            vec![Token::IdentMap(vec!["@a".to_string(), "@a.name".to_string(), "@.age".to_string()])];
        assert_eq!(case8.unwrap(), expected_case8,);

        let case9 = tokenize("[]");
        assert_eq!(case9.unwrap(), vec![Token::IdentArray(vec![])]);

        let case10 = tokenize("\"John\" StartsWith \"Jo\"");
        let expected_case10 =
            vec![Token::String("John".to_string()), Token::StartsWith, Token::String("Jo".to_string())];
        assert_eq!(case10.unwrap(), expected_case10);

        let case11 = tokenize("\"John\" EndsWith \"hn\"");
        let expected_case11 =
            vec![Token::String("John".to_string()), Token::EndsWith, Token::String("hn".to_string())];
        assert_eq!(case11.unwrap(), expected_case11);

        let case12 = tokenize("15 & 32");
        let expected_case12 = vec![Token::Int(15), Token::BitAnd, Token::Int(32)];
        assert_eq!(case12.unwrap(), expected_case12);

        let case13 = tokenize("15 | 32");
        let expected_case13 = vec![Token::Int(15), Token::BitOr, Token::Int(32)];
        assert_eq!(case13.unwrap(), expected_case13);

        let case14 = tokenize("15 << 2");
        let expected_case14 = vec![Token::Int(15), Token::BitLShift, Token::Int(2)];
        assert_eq!(case14.unwrap(), expected_case14);

        let case15 = tokenize("15 >> 2");
        let expected_case15 = vec![Token::Int(15), Token::BitRShift, Token::Int(2)];
        assert_eq!(case15.unwrap(), expected_case15);
    }

    #[test]
    fn test_tokenize_errors() {
        // 1 = 1, the partial = must be completed by another =
        let case1 = tokenize("1 = 1");
        assert_eq!(
            case1.err().unwrap(),
            ExprError::unmatched_partial_token(PartialToken::Eq, Some(PartialToken::Whitespace))
        );

        /*
        let case2 = tokenize("1 & 2");
        assert_eq!(
            case2.err().unwrap(),
            ExprError::unmatched_partial_token(PartialToken::Ampersand, Some(PartialToken::Whitespace))
        );

        let case3 = tokenize("1 | 2");
        assert_eq!(
            case3.err().unwrap(),
            ExprError::unmatched_partial_token(PartialToken::VerticalBar, Some(PartialToken::Whitespace))
        );
         */

        let case4 = tokenize("-a");
        assert_eq!(
            case4.err().unwrap(),
            ExprError::unmatched_partial_token(
                PartialToken::Minus,
                Some(PartialToken::Literal("a".to_string()))
            )
        );

        let case5 = tokenize("[1, -2, 3, -4");
        assert_eq!(case5.err().unwrap(), ExprError::UnmatchedLRBrackets);

        let case6 = tokenize("[1, -2, [3], -4]");
        assert_eq!(
            case6.err().unwrap(),
            ExprError::Unsupported("nested array is not supported".to_string())
        );

        let case7 = tokenize("[1, 0.5, -4]");
        assert_eq!(
            case7.err().unwrap(),
            ExprError::unsupported("array of various type unsupported".to_string())
        );
    }
}
