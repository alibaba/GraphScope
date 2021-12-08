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

use std::cell::RefCell;
use std::convert::TryFrom;

use dyn_type::arith::Exp;
use dyn_type::{BorrowObject, Object};
use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::common as pb;
use ir_common::NameOrId;

use crate::expr::error::{ExprError, ExprResult};
use crate::graph::element::Element;
use crate::graph::property::{Details, PropKey};

unsafe impl Sync for Evaluator {}

#[derive(Debug)]
pub struct Evaluator {
    /// A suffix-tree-based expression for evaluating
    suffix_tree: Vec<InnerOpr>,
    /// A stack for evaluating the suffix-tree-based expression
    /// Wrap it in a `RefCell` to avoid conflict mutable reference
    stack: RefCell<Vec<Object>>,
}

/// An inner representation of `pb::ExprOpr` for one-shot translation of `pb::ExprOpr`.
#[derive(Debug, Clone)]
pub(crate) enum InnerOpr {
    Logical(pb::Logical),
    Arith(pb::Arithmetic),
    Const(Option<Object>),
    Var { tag: Option<NameOrId>, prop_key: Option<PropKey> },
}

impl ToString for InnerOpr {
    fn to_string(&self) -> String {
        match self {
            InnerOpr::Logical(logical) => format!("{:?}", logical),
            InnerOpr::Arith(arith) => format!("{:?}", arith),
            InnerOpr::Const(c) => format!("Const({:?})", c),
            InnerOpr::Var { tag, prop_key } => {
                if let Some(p) = prop_key {
                    format!("Var (tag: {:?}, prop_key: {:?})", tag, p)
                } else {
                    format!("Var (tag: {:?})", tag)
                }
            }
        }
    }
}

/// A string representation of `InnerOpr`
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorDesc(String);

impl From<InnerOpr> for OperatorDesc {
    fn from(inner: InnerOpr) -> Self {
        Self::from(&inner)
    }
}

impl From<&InnerOpr> for OperatorDesc {
    fn from(inner: &InnerOpr) -> Self {
        Self(inner.to_string())
    }
}

/// A `Context` gives the behavior of obtaining a certain tag from the runtime
/// for evaluating variables in an expression.
pub trait Context<E: Element> {
    fn get(&self, _tag: Option<&NameOrId>) -> Option<&E> {
        None
    }
}

pub struct NoneContext {}

impl Context<()> for NoneContext {}

impl TryFrom<pb::SuffixExpr> for Evaluator {
    type Error = ParsePbError;

    fn try_from(suffix_tree: pb::SuffixExpr) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        let mut inner_tree: Vec<InnerOpr> = Vec::with_capacity(suffix_tree.operators.len());
        for unit in suffix_tree.operators {
            inner_tree.push(InnerOpr::try_from(unit)?);
        }
        Ok(Self { suffix_tree: inner_tree, stack: RefCell::new(vec![]) })
    }
}

fn apply_arith<'a>(
    arith: &pb::Arithmetic, first: Option<BorrowObject<'a>>, second: Option<BorrowObject<'a>>,
) -> ExprResult<Object> {
    use pb::Arithmetic::*;
    if first.is_some() && second.is_some() {
        let a = first.unwrap();
        let b = second.unwrap();
        Ok(match arith {
            Add => Object::Primitive(a.as_primitive()? + b.as_primitive()?),
            Sub => Object::Primitive(a.as_primitive()? - b.as_primitive()?),
            Mul => Object::Primitive(a.as_primitive()? * b.as_primitive()?),
            Div => Object::Primitive(a.as_primitive()? / b.as_primitive()?),
            Mod => Object::Primitive(a.as_primitive()? % b.as_primitive()?),
            Exp => Object::Primitive(a.as_primitive()?.exp(b.as_primitive()?)),
        })
    } else {
        Err(ExprError::MissingOperands(InnerOpr::Arith(*arith).into()))
    }
}

fn apply_logical<'a>(
    logical: &pb::Logical, first: Option<BorrowObject<'a>>, second: Option<BorrowObject<'a>>,
) -> ExprResult<Object> {
    use pb::Logical::*;
    if logical == &Not {
        if let Some(a) = first {
            return Ok((!a.as_bool()?).into());
        }
    } else {
        if first.is_some() && second.is_some() {
            let a = first.unwrap();
            let b = second.unwrap();
            let rst = match logical {
                Eq => Ok((a == b).into()),
                Ne => Ok((a != b).into()),
                Lt => Ok((a < b).into()),
                Le => Ok((a <= b).into()),
                Gt => Ok((a > b).into()),
                Ge => Ok((a >= b).into()),
                And => Ok((a.as_bool()? && b.as_bool()?).into()),
                Or => Ok((a.as_bool()? || b.as_bool()?).into()),
                Within => Ok(b.contains(&a).into()),
                Without => Ok((!b.contains(&a)).into()),
                Not => unreachable!(),
            };

            return rst;
        }
    }

    Err(ExprError::MissingOperands(InnerOpr::Logical(*logical).into()))
}

// Private api
impl Evaluator {
    /// Evaluate simple expression that contains less than three operators
    /// without using the stack.
    fn eval_without_stack<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprResult<Object> {
        assert!(self.suffix_tree.len() <= 3);
        let _first = self.suffix_tree.get(0);
        let _second = self.suffix_tree.get(1);
        let _third = self.suffix_tree.get(2);
        if self.suffix_tree.is_empty() {
            Err(ExprError::EmptyExpression)
        } else if self.suffix_tree.len() == 1 {
            let first = _first.unwrap();
            Ok(first
                .eval_as_borrow_object(context)?
                .ok_or(ExprError::NoneOperand(first.into()))?
                .into())
        } else if self.suffix_tree.len() == 2 {
            let first = _first.unwrap();
            let second = _second.unwrap();
            // must be not
            if let InnerOpr::Logical(logical) = second {
                apply_logical(logical, first.eval_as_borrow_object(context)?, None)
            } else {
                if !second.is_operand() {
                    Err(ExprError::MissingOperands(second.into()))
                } else {
                    Err(ExprError::OtherErr("invalid expression".to_string()))
                }
            }
        } else {
            let first = _first.unwrap();
            let second = _second.unwrap();
            let third = _third.unwrap();

            if let InnerOpr::Logical(logical) = third {
                apply_logical(
                    logical,
                    first.eval_as_borrow_object(context)?,
                    second.eval_as_borrow_object(context)?,
                )
            } else if let InnerOpr::Arith(arith) = third {
                apply_arith(
                    arith,
                    first.eval_as_borrow_object(context)?,
                    second.eval_as_borrow_object(context)?,
                )
            } else {
                Err(ExprError::OtherErr("invalid expression".to_string()))
            }
        }
    }
}

impl Evaluator {
    /// Reset the status of the evaluator for further evaluation
    pub fn reset(&self) {
        self.stack.borrow_mut().clear();
    }

    /// Evaluate an expression with an optional context. The context must implement the
    /// provided trait `[Context]`, that can get an `[crate::graph::element::Element]`
    /// using a given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dyn_type::Object;
    /// # use ir_common::NameOrId;
    /// # use runtime::expr::eval::{Context, Evaluator};
    /// # use runtime::expr::token::tokenize;
    /// # use runtime::expr::to_suffix_expr_pb;
    /// # use runtime::graph::element::Vertex;
    /// # use runtime::graph::property::{DefaultDetails, DynDetails};
    /// # use std::collections::HashMap;
    /// # use std::convert::TryFrom;
    ///
    /// struct Vertices {
    ///     vec: Vec<Vertex>,
    /// }
    ///
    /// impl Context<Vertex> for Vertices {
    ///     fn get(&self, key: Option<&NameOrId>) -> Option<&Vertex> {
    ///        match key {
    ///             Some(NameOrId::Id(i)) => self.vec.get(*i as usize),
    ///             _ => None
    ///         }
    ///     }
    /// }
    ///
    /// let map: HashMap<NameOrId, Object> = vec![
    ///         (NameOrId::from("age".to_string()), 12.into()),
    ///         (NameOrId::from("birthday".to_string()), 19900416.into()),
    ///     ]
    ///     .into_iter()
    ///     .collect();
    ///
    ///     let ctxt = Vertices {
    ///         vec: vec![
    ///             Vertex::new(DynDetails::new(DefaultDetails::with_property(
    ///                 1,
    ///                 NameOrId::from(1),
    ///                 map.clone(),
    ///             ))),
    ///             Vertex::new(DynDetails::new(DefaultDetails::with_property(
    ///                 2,
    ///                 NameOrId::from(2),
    ///                 map.clone(),
    ///             ))),
    ///         ],
    ///     };
    ///
    /// let tokens = tokenize("@0.age == @1.age").unwrap();
    /// let suffix_tree = to_suffix_expr_pb(tokens).unwrap();
    /// let eval = Evaluator::try_from(suffix_tree).unwrap();
    ///
    /// assert!(eval.eval::<_, _>(Some(&ctxt)).unwrap().as_bool().unwrap())
    /// ```
    pub fn eval<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprResult<Object> {
        let mut stack = self.stack.borrow_mut();
        if self.suffix_tree.len() <= 3 {
            return self.eval_without_stack(context);
        }
        stack.clear();
        for opr in &self.suffix_tree {
            if opr.is_operand() {
                if let Some(obj) = opr.eval_as_borrow_object(context)? {
                    stack.push(obj.into());
                } else {
                    return Err(ExprError::NoneOperand(opr.into()));
                }
            } else {
                let first = stack.pop();
                let first_borrow = first.as_ref().map(|obj| obj.as_borrow());
                let rst = match opr {
                    InnerOpr::Logical(logical) => {
                        if logical == &pb::Logical::Not {
                            apply_logical(logical, first_borrow, None)?
                        } else {
                            let second = stack.pop();
                            let second_borrow = second.as_ref().map(|obj| obj.as_borrow());
                            apply_logical(logical, second_borrow, first_borrow)?
                        }
                    }
                    InnerOpr::Arith(arith) => {
                        let second = stack.pop();
                        let second_borrow = second.as_ref().map(|obj| obj.as_borrow());
                        apply_arith(arith, second_borrow, first_borrow)?
                    }
                    _ => unreachable!(),
                };
                stack.push(rst.into());
            }
        }

        if stack.len() == 1 {
            Ok(stack.pop().unwrap())
        } else {
            Err("invalid expression".into())
        }
    }

    pub fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprResult<bool> {
        Ok(self.eval(context)?.as_bool()?)
    }
}

impl TryFrom<pb::ExprOpr> for InnerOpr {
    type Error = ParsePbError;

    fn try_from(unit: pb::ExprOpr) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use pb::expr_opr::Item::*;
        if let Some(item) = unit.item {
            let result = match item {
                Logical(logical) => {
                    Self::Logical(unsafe { std::mem::transmute::<_, pb::Logical>(logical) })
                }
                Arith(arith) => Self::Arith(unsafe { std::mem::transmute::<_, pb::Arithmetic>(arith) }),
                Const(c) => Self::Const(c.into_object()?),
                Var(var) => {
                    let (_tag, _property) = (var.tag, var.property);
                    let tag = if let Some(tag) = _tag { Some(NameOrId::try_from(tag)?) } else { None };
                    if let Some(property) = _property {
                        Self::Var { tag, prop_key: Some(PropKey::try_from(property)?) }
                    } else {
                        Self::Var { tag, prop_key: None }
                    }
                }
            };
            Ok(result)
        } else {
            Err(ParsePbError::from("empty value provided"))
        }
    }
}

impl InnerOpr {
    pub fn eval_as_borrow_object<'a, E: Element + 'a, C: Context<E> + 'a>(
        &'a self, context: Option<&'a C>,
    ) -> ExprResult<Option<BorrowObject<'a>>> {
        match self {
            Self::Const(c_opt) => Ok(if let Some(opt) = c_opt { Some(opt.as_borrow()) } else { None }),
            Self::Var { tag, prop_key } => {
                if let Some(ctxt) = context {
                    let mut result = Ok(None);
                    if let Some(element) = ctxt.get(tag.as_ref()) {
                        if let Some(property) = prop_key {
                            if let Some(details) = element.details() {
                                result = Ok(details.get(property))
                            }
                        } else {
                            result = Ok(Some(element.as_borrow_object()))
                        }
                    }

                    result
                } else {
                    Err(ExprError::MissingContext(self.into()))
                }
            }
            _ => Err(ExprError::UnmatchedOperator(self.into())),
        }
    }

    pub fn is_operand(&self) -> bool {
        match self {
            InnerOpr::Const(_) | InnerOpr::Var { .. } => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::expr::to_suffix_expr_pb;
    use crate::expr::token::tokenize;
    use crate::expr::token::Token;
    use crate::graph::element::Vertex;
    use crate::graph::property::{DefaultDetails, DynDetails};

    struct Vertices {
        vec: Vec<Vertex>,
    }
    impl Context<Vertex> for Vertices {
        fn get(&self, key: Option<&NameOrId>) -> Option<&Vertex> {
            match key {
                Some(NameOrId::Id(i)) => self.vec.get(*i as usize),
                _ => None,
            }
        }
    }

    fn prepare_context() -> Vertices {
        let map1: HashMap<NameOrId, Object> = vec![
            (NameOrId::from("age".to_string()), 31.into()),
            (NameOrId::from("birthday".to_string()), 19900416.into()),
            (NameOrId::from("name".to_string()), "John".to_string().into()),
        ]
        .into_iter()
        .collect();
        let map2: HashMap<NameOrId, Object> = vec![
            (NameOrId::from("age".to_string()), 26.into()),
            (NameOrId::from("birthday".to_string()), 19950816.into()),
            (NameOrId::from("name".to_string()), "Nancy".to_string().into()),
        ]
        .into_iter()
        .collect();
        Vertices {
            vec: vec![
                Vertex::new(DynDetails::new(DefaultDetails::with_property(1, NameOrId::from(9), map1))),
                Vertex::new(DynDetails::new(DefaultDetails::with_property(2, NameOrId::from(11), map2))),
            ],
        }
    }

    #[test]
    fn test_eval_simple() {
        let cases: Vec<&str> = vec![
            "7 + 3",          // 10
            "7.0 + 3",        // 10.0
            "7 * 3",          // 21
            "7 / 3",          // 2
            "7 ^ 3",          // 343
            "7 ^ -3",         // 1 / 343
            "7 % 3",          // 1
            "7 -3",           // 4
            "-3 + 7",         // 4
            "-3",             // -3
            "-3.0",           // -3.0
            "false",          // false
            "!true",          // false
            "!10",            // false
            "!0",             // true
            "true || true",   // true
            "true || false",  // true
            "false || false", // false
            "true && true",   // true
            "true && false",  // false
            "1 > 2",          // false
            "1 < 2",          // true
            "1 >= 2",         // false
            "2 <= 2",         // true,
            "2 == 2",         // true
            "1.0 > 2.0",      // false
        ];

        let expected: Vec<Object> = vec![
            Object::from(10),
            Object::from(10.0),
            Object::from(21),
            Object::from(2),
            Object::from(343),
            Object::from(1.0 / 343.0),
            Object::from(1),
            Object::from(4),
            Object::from(4),
            Object::from(-3),
            Object::from(-3.0),
            Object::from(false),
            Object::from(false),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(true),
            Object::from(false),
            Object::from(true),
            Object::from(false),
            Object::from(false),
            Object::from(true),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(false),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_contains() {
        let tokens = vec![Token::Int(10), Token::Within, Token::IntArray(vec![10, 9, 8, 7])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![Token::IntArray(vec![10, 7]), Token::Within, Token::IntArray(vec![10, 9, 8, 7])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![Token::IntArray(vec![10, 6]), Token::Within, Token::IntArray(vec![10, 9, 8, 7])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(!eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![Token::IntArray(vec![10, 6]), Token::Without, Token::IntArray(vec![10, 9, 8, 7])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![Token::Float(10.0), Token::Within, Token::FloatArray(vec![10.0, 9.0, 8.0, 7.0])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![
            Token::FloatArray(vec![10.0, 7.0]),
            Token::Within,
            Token::FloatArray(vec![10.0, 9.0, 8.0, 7.0]),
        ];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![Token::Float(11.0), Token::Within, Token::FloatArray(vec![10.0, 9.0, 8.0, 7.0])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(!eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![Token::Float(11.0), Token::Without, Token::FloatArray(vec![10.0, 9.0, 8.0, 7.0])];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![
            Token::String("a".to_string()),
            Token::Within,
            Token::StrArray(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![
            Token::StrArray(vec!["a".to_string()]),
            Token::Within,
            Token::StrArray(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![
            Token::StrArray(vec!["d".to_string()]),
            Token::Within,
            Token::StrArray(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(!eval.eval_bool::<(), NoneContext>(None).unwrap());

        let tokens = vec![
            Token::StrArray(vec!["d".to_string()]),
            Token::Without,
            Token::StrArray(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
        ];
        let eval = Evaluator::try_from(to_suffix_expr_pb(tokens).unwrap()).unwrap();
        assert!(eval.eval_bool::<(), NoneContext>(None).unwrap());
    }

    #[test]
    fn test_eval_complex() {
        let cases: Vec<&str> = vec![
            "(-10)",                                 // -10
            "2 * 2 - 3",                             // 1
            "2 * (2 - 3)",                           // -2
            "6 / 2 - 3",                             // 0
            "6 / (2 - 3)",                           // -6
            "2 * 1e-3",                              // 0.002
            "1 > 2 && 1 < 3",                        // false
            "1 > 2 || 1 < 3",                        // true
            "2 ^ 10 > 10",                           // true
            "2 / 5 ^ 2",                             // 0
            "2.0 / 5 ^ 2",                           // 2.0 / 25
            "((1 + 2) * 3) / (7 * 8) + 12.5 / 10.1", // 1.2376237623762376
            "((1 + 2) * 3) / 7 * 8 + 12.5 / 10.1",   // 9.237623762376238
            "((1 + 2) * 3) / 7 * 8 + 12.5 / 10.1 \
                == ((1 + 2) * 3) / (7 * 8) + 12.5 / 10.1", // false
        ];

        let expected: Vec<Object> = vec![
            Object::from(-10),
            Object::from(1),
            Object::from(-2),
            Object::from(0),
            Object::from(-6),
            Object::from(0.002),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(0),
            Object::from(2.0 / 25.0),
            Object::from(1.2376237623762376),
            Object::from(9.237623762376238),
            Object::from(false),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_variable() {
        // [v0: id = 1, label = 9, age = 31, birthday = 19900416]
        // [v1: id = 2, label = 11, age = 26, birthday = 19950816]
        let ctxt = prepare_context();
        let cases: Vec<&str> = vec![
            "@0.~id",                                      // 1
            "@1.~label",                                   // 1
            "@0.~id < @1.~id",                             // true
            "@0.birthday > @1.birthday",                   // false
            "@0.~label == @1.~label",                      // false
            "@0.name != @1.name",                          // true
            "@0.name == \"John\"",                         // true
            "@0.name == \"John\" && @1.name == \"Jimmy\"", // false
            "@0.age + @0.birthday / 10000 == \
                @1.age + @1.birthday / 10000", // true
        ];

        let expected: Vec<Object> = vec![
            Object::from(1),
            Object::from(11),
            Object::from(true),
            Object::from(false),
            Object::from(false),
            Object::from(true),
            Object::from(true),
            Object::from(false),
            Object::from(true),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_errors() {
        let cases: Vec<&str> = vec![
            "@2",
            "@2",
            "@1.nonexistent",
            "+",
            "1 + ",
            "1 1",
            "1 1 2",
            "1 1 + 2",
            "1 + @1.age * 1 1 - 1 - 5",
        ];
        let ctxt = prepare_context();

        let expected: Vec<ExprError> = vec![
            // Evaluate variable without providing the context
            ExprError::MissingContext(InnerOpr::Var { tag: Some(2.into()), prop_key: None }.into()),
            // obtain non-value from the context
            ExprError::NoneOperand(InnerOpr::Var { tag: Some(2.into()), prop_key: None }.into()),
            // obtain non-value from the context
            ExprError::NoneOperand(
                InnerOpr::Var {
                    tag: Some(1.into()),
                    prop_key: Some(PropKey::Key("nonexistent".to_string().into())),
                }
                .into(),
            ),
            // try to evaluate neither a variable nor a const
            ExprError::UnmatchedOperator(InnerOpr::Arith(pb::Arithmetic::Add).into()),
            ExprError::MissingOperands(InnerOpr::Arith(pb::Arithmetic::Add).into()),
            ExprError::OtherErr("invalid expression".to_string()),
            ExprError::OtherErr("invalid expression".to_string()),
            ExprError::OtherErr("invalid expression".to_string()),
            ExprError::OtherErr("invalid expression".to_string()),
        ];

        let mut is_context = false;
        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(to_suffix_expr_pb(tokenize(case).unwrap()).unwrap()).unwrap();
            assert_eq!(
                if is_context {
                    eval.eval::<_, Vertices>(Some(&ctxt))
                        .err()
                        .unwrap()
                } else {
                    eval.eval::<_, NoneContext>(None).err().unwrap()
                },
                expected
            );
            is_context = true;
        }
    }
}
