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
    Eq,      // ==
    Ne,      // !=
    Gt,      // >
    Lt,      // <
    Ge,      // >=
    Le,      // <=
    And,     // &&
    Or,      // ||
    Not,     // !
    Within,  // Within
    Without, // Without

    // Precedence
    LBrace, // (
    RBrace, // )

    // Values and Variables
    Identifier(String),    // a string-identifier
    Boolean(bool),         // a boolean value
    Int(i64),              // an integer value
    Float(f64),            // a float value
    String(String),        // a string value
    IntArray(Vec<i64>),    // a integer array
    FloatArray(Vec<f64>),  // a float array
    StrArray(Vec<String>), // a string array
}

impl Token {
    #[inline]
    pub fn is_operand(&self) -> bool {
        use crate::expr::token::Token::*;
        match self {
            Identifier(_) | Float(_) | Int(_) | Boolean(_) | String(_) | IntArray(_) | FloatArray(_)
            | StrArray(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_left_brace(&self) -> bool {
        self == &Token::LBrace
    }

    #[inline]
    pub fn is_right_brace(&self) -> bool {
        self == &Token::RBrace
    }

    #[inline]
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
        '+' => PartialToken::Token(Token::Plus),
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
    mut iter: &mut Iter, is_in_bracket: bool,
) -> ExprResult<(PartialToken, Option<char>)> {
    let mut result = String::new();
    let mut last_char = None;

    if !is_in_bracket {
        while let Some(c) = iter.next() {
            match c {
                '"' => break,
                '\\' => result.push(parse_escape_sequence(&mut iter)?),
                c => result.push(c),
            }
        }
    } else {
        // Treat everything as a string
        while let Some(c) = iter.next() {
            if c == ']' {
                last_char = Some(c);
                break;
            } else if c == '[' {
                return Err(ExprError::unsupported("nested array is not supported".to_string()));
            } else {
                result.push(c);
            }
        }
    }

    Ok((PartialToken::Token(Token::String(result)), last_char))
}

/// Converts a string to a vector of partial tokens.
fn str_to_partial_tokens(string: &str) -> ExprResult<Vec<PartialToken>> {
    let mut result = Vec::new();
    let mut iter = string.chars().peekable();
    while let Some(c) = iter.next() {
        if c == '"' {
            result.push(parse_string_literal(&mut iter, false)?.0);
        } else if c == '[' {
            result.push(PartialToken::LBracket);
            let (literal, last_char) = parse_string_literal(&mut iter, true)?;
            result.push(literal);
            if last_char.is_some() {
                result.push(PartialToken::RBracket);
            }
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
    // use pivot to regulate the type of all elements
    let pivot = token_array.first().unwrap();
    match pivot {
        Token::Int(_) => {
            let mut vec = Vec::with_capacity(token_array.len());
            for t in token_array {
                match t {
                    Token::Int(i) => vec.push(i),
                    _ => {
                        return Err(ExprError::unsupported("array of various type unsupported".to_string()))
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
                        return Err(ExprError::unsupported("array of various type unsupported".to_string()))
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
                        return Err(ExprError::unsupported("array of various type unsupported".to_string()))
                    }
                }
            }
            Ok(Token::StrArray(vec))
        }
        _ => Err(ExprError::unsupported(format!("array of this type: {:?} is not supported", pivot))),
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
            PartialToken::LBracket => {
                cutoff = 3;
                if third != Some(PartialToken::RBracket) {
                    return Err(ExprError::UnmatchedLRBrackets);
                } else {
                    let mut token_array: Vec<Token> = Vec::new();
                    match second {
                        Some(PartialToken::Token(Token::String(ref s))) => {
                            let elements = s.split(",");
                            for e in elements {
                                let t = partial_tokens_to_tokens(&str_to_partial_tokens(e)?)?;
                                if t.len() != 1 {
                                    return Err(format!("invalid token: {:?}", second)
                                        .as_str()
                                        .into());
                                } else {
                                    token_array.push(t[0].clone())
                                }
                            }
                        }
                        _ => {
                            return Err(format!("invalid token: {:?}", second)
                                .as_str()
                                .into())
                        }
                    }
                    if token_array.is_empty() {
                        return Err("empty array is given".into());
                    } else {
                        Some(token_array_to_token(token_array)?)
                    }
                }
            }
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

        let case7 = tokenize("[1, -2, 3, -4");
        assert_eq!(case7.err().unwrap(), ExprError::UnmatchedLRBrackets);

        let case8 = tokenize("[1, -2, [3], -4]");
        assert_eq!(
            case8.err().unwrap(),
            ExprError::Unsupported("nested array is not supported".to_string())
        );

        let case9 = tokenize("[1, 0.5, -4]");
        assert_eq!(
            case9.err().unwrap(),
            ExprError::unsupported("array of various type unsupported".to_string())
        );

        let case10 = tokenize("[@a]");
        assert_eq!(
            case10.err().unwrap(),
            ExprError::unsupported(format!(
                "array of this type: {:?} is not supported",
                Token::Identifier("@a".to_string())
            ))
        );

        let case11 = tokenize("[]");
        assert_eq!(
            case11.err().unwrap(),
            format!("invalid token: {:?}", Some(PartialToken::Token(Token::String("".to_string()))))
                .as_str()
                .into()
        );
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
            ExprError::unmatched_partial_token(PartialToken::Ampersand, Some(PartialToken::Whitespace))
        );

        let case3 = tokenize("1 | 2");
        assert_eq!(
            case3.err().unwrap(),
            ExprError::unmatched_partial_token(PartialToken::VerticalBar, Some(PartialToken::Whitespace))
        );

        let case4 = tokenize("-a");
        assert_eq!(
            case4.err().unwrap(),
            ExprError::unmatched_partial_token(
                PartialToken::Minus,
                Some(PartialToken::Literal("a".to_string()))
            )
        );
    }
}
