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
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};

use dyn_type::arith::{BitOperand, Exp};
use dyn_type::object;
use dyn_type::{BorrowObject, Object};
use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::expr_parse::to_suffix_expr;
use ir_common::generated::common as common_pb;
use ir_common::{NameOrId, ALL_KEY, ID_KEY, LABEL_KEY, LENGTH_KEY};

use crate::apis::{Details, Element, PropKey};
use crate::utils::expr::eval_pred::EvalPred;
use crate::utils::expr::{ExprEvalError, ExprEvalResult};

/// The trait to define evaluating an expression
pub trait Evaluate {
    fn eval<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<Object>;
}

#[derive(Debug)]
pub struct Evaluator {
    /// A suffix-tree-based expression for evaluating
    suffix_tree: Vec<InnerOpr>,
    /// A stack for evaluating the suffix-tree-based expression
    /// Wrap it in a `RefCell` to avoid conflict mutable reference
    stack: RefCell<Vec<Object>>,
}

unsafe impl Sync for Evaluator {}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Const(Object),
    Var { tag: Option<NameOrId>, prop_key: Option<PropKey> },
    Vars(Vec<Operand>),
    VarMap(Vec<Operand>),
}

/// An inner representation of `common_pb::ExprOpr` for one-shot translation of `common_pb::ExprOpr`.
#[derive(Debug, Clone)]
pub(crate) enum InnerOpr {
    Logical(common_pb::Logical),
    Arith(common_pb::Arithmetic),
    Operand(Operand),
}

impl ToString for InnerOpr {
    fn to_string(&self) -> String {
        match self {
            InnerOpr::Logical(logical) => format!("{:?}", logical),
            InnerOpr::Arith(arith) => format!("{:?}", arith),
            InnerOpr::Operand(item) => format!("{:?}", item),
        }
    }
}

impl ToString for Operand {
    fn to_string(&self) -> String {
        format!("{:?}", self)
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

impl From<Operand> for OperatorDesc {
    fn from(opr: Operand) -> Self {
        Self(opr.to_string())
    }
}

impl From<&Operand> for OperatorDesc {
    fn from(opr: &Operand) -> Self {
        Self(opr.to_string())
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

impl TryFrom<common_pb::Expression> for Evaluator {
    type Error = ParsePbError;

    fn try_from(suffix_tree: common_pb::Expression) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        let mut inner_tree: Vec<InnerOpr> = Vec::with_capacity(suffix_tree.operators.len());
        let suffix_oprs = to_suffix_expr(suffix_tree.operators)
            .map_err(|err| ParsePbError::ParseError(format!("{:?}", err)))?;
        for unit in suffix_oprs {
            inner_tree.push(InnerOpr::try_from(unit)?);
        }
        Ok(Self { suffix_tree: inner_tree, stack: RefCell::new(vec![]) })
    }
}

fn apply_arith<'a>(
    arith: &common_pb::Arithmetic, a: BorrowObject<'a>, b: BorrowObject<'a>,
) -> ExprEvalResult<Object> {
    use common_pb::Arithmetic::*;
    Ok(match arith {
        Add => Object::Primitive(a.as_primitive()? + b.as_primitive()?),
        Sub => Object::Primitive(a.as_primitive()? - b.as_primitive()?),
        Mul => Object::Primitive(a.as_primitive()? * b.as_primitive()?),
        Div => Object::Primitive(a.as_primitive()? / b.as_primitive()?),
        Mod => Object::Primitive(a.as_primitive()? % b.as_primitive()?),
        Exp => Object::Primitive(a.as_primitive()?.exp(b.as_primitive()?)),
        Bitand => Object::Primitive(a.as_primitive()?.bit_and(b.as_primitive()?)),
        Bitor => Object::Primitive(a.as_primitive()?.bit_or(b.as_primitive()?)),
        Bitxor => Object::Primitive(a.as_primitive()?.bit_xor(b.as_primitive()?)),
        Bitlshift => Object::Primitive(
            a.as_primitive()?
                .bit_left_shift(b.as_primitive()?),
        ),
        Bitrshift => Object::Primitive(
            a.as_primitive()?
                .bit_right_shift(b.as_primitive()?),
        ),
    })
}

pub(crate) fn apply_logical<'a>(
    logical: &common_pb::Logical, a: BorrowObject<'a>, b_opt: Option<BorrowObject<'a>>,
) -> ExprEvalResult<Object> {
    use common_pb::Logical::*;
    if logical == &Not {
        return Ok((!a.eval_bool::<(), NoneContext>(None)?).into());
    } else {
        if b_opt.is_some() {
            let b = b_opt.unwrap();
            match logical {
                Eq => Ok((a == b).into()),
                Ne => Ok((a != b).into()),
                Lt => Ok((a < b).into()),
                Le => Ok((a <= b).into()),
                Gt => Ok((a > b).into()),
                Ge => Ok((a >= b).into()),
                And => Ok((a.eval_bool::<(), NoneContext>(None)?
                    && b.eval_bool::<(), NoneContext>(None)?)
                .into()),
                Or => Ok((a.eval_bool::<(), NoneContext>(None)?
                    || b.eval_bool::<(), NoneContext>(None)?)
                .into()),
                Within => Ok(b.contains(&a).into()),
                Without => Ok((!b.contains(&a)).into()),
                Startswith => Ok(a
                    .as_str()?
                    .starts_with(b.as_str()?.as_ref())
                    .into()),
                Endswith => Ok(a
                    .as_str()?
                    .ends_with(b.as_str()?.as_ref())
                    .into()),
                Not => unreachable!(),
            }
        } else {
            Err(ExprEvalError::MissingOperands(InnerOpr::Logical(*logical).into()))
        }
    }
}

// Private api
impl Evaluator {
    /// Evaluate simple expression that contains less than three operators
    /// without using the stack.
    fn eval_without_stack<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<Object> {
        assert!(self.suffix_tree.len() <= 3);
        let _first = self.suffix_tree.get(0);
        let _second = self.suffix_tree.get(1);
        let _third = self.suffix_tree.get(2);
        if self.suffix_tree.is_empty() {
            Err(ExprEvalError::EmptyExpression)
        } else if self.suffix_tree.len() == 1 {
            _first.unwrap().eval(context)
        } else if self.suffix_tree.len() == 2 {
            let first = _first.unwrap();
            let second = _second.unwrap();
            if let InnerOpr::Logical(logical) = second {
                Ok(apply_logical(logical, first.eval(context)?.as_borrow(), None)?)
            } else {
                if !second.is_operand() {
                    Err(ExprEvalError::MissingOperands(second.into()))
                } else {
                    Err(ExprEvalError::OtherErr("invalid expression".to_string()))
                }
            }
        } else {
            let first = _first.unwrap();
            let second = _second.unwrap();
            let third = _third.unwrap();

            if let InnerOpr::Logical(logical) = third {
                let a = first.eval(context)?;
                let b = second.eval(context)?;
                Ok(apply_logical(logical, a.as_borrow(), Some(b.as_borrow()))?)
            } else if let InnerOpr::Arith(arith) = third {
                let a = first.eval(context)?;
                let b = second.eval(context)?;

                Ok(apply_arith(arith, a.as_borrow(), b.as_borrow())?)
            } else {
                Err(ExprEvalError::OtherErr("invalid expression".to_string()))
            }
        }
    }
}

impl Evaluate for Evaluator {
    /// Evaluate an expression with an optional context. The context must implement the
    /// provided trait `[Context]`, that can get an `[crate::graph::element::Element]`
    /// using a given key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dyn_type::Object;
    /// # use ir_common::NameOrId;
    /// # use graph_proxy::utils::expr::eval::{Context, Evaluator, Evaluate};
    /// # use graph_proxy::apis::{Vertex, DynDetails};
    /// # use ahash::HashMap;
    /// # use std::convert::TryFrom;
    /// # use ir_common::expr_parse::str_to_expr_pb;
    /// # use graph_proxy::utils::expr::eval_pred::EvalPred;
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
    ///             Vertex::new(1, Some(1), DynDetails::new(
    ///                 map.clone(),
    ///             )),
    ///             Vertex::new(2, Some(2), DynDetails::new(
    ///                 map.clone(),
    ///             )),
    ///         ],
    ///     };
    ///
    /// let suffix_tree = str_to_expr_pb("@0.age == @1.age".to_string()).unwrap();
    /// let eval = Evaluator::try_from(suffix_tree).unwrap();
    ///
    /// assert!(eval.eval_bool::<_, _>(Some(&ctxt)).unwrap())
    /// ```
    fn eval<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<Object> {
        let mut stack = self.stack.borrow_mut();
        if self.suffix_tree.len() <= 3 {
            return self.eval_without_stack(context);
        }
        stack.clear();
        for opr in &self.suffix_tree {
            if opr.is_operand() {
                stack.push(opr.eval(context)?);
            } else {
                if let Some(first) = stack.pop() {
                    let first_borrow = first.as_borrow();
                    let rst = match opr {
                        InnerOpr::Logical(logical) => {
                            if logical == &common_pb::Logical::Not {
                                apply_logical(logical, first_borrow, None)
                            } else {
                                if let Some(second) = stack.pop() {
                                    apply_logical(logical, second.as_borrow(), Some(first_borrow))
                                } else {
                                    Err(ExprEvalError::OtherErr("invalid expression".to_string()))
                                }
                            }
                        }
                        InnerOpr::Arith(arith) => {
                            if let Some(second) = stack.pop() {
                                apply_arith(arith, second.as_borrow(), first_borrow)
                            } else {
                                Err(ExprEvalError::OtherErr("invalid expression".to_string()))
                            }
                        }
                        _ => unreachable!(),
                    };
                    stack.push((rst?).into());
                }
            }
        }

        if stack.len() == 1 {
            Ok(stack.pop().unwrap())
        } else {
            Err("invalid expression".into())
        }
    }
}

fn get_object(obj_result: ExprEvalResult<Object>) -> ExprEvalResult<Object> {
    match obj_result {
        Ok(obj) => Ok(obj),
        Err(err) => match err {
            // Capture a special error case that returns `Object::None`
            ExprEvalError::GetNoneFromContext => Ok(Object::None),
            _ => Err(err),
        },
    }
}

impl EvalPred for Evaluator {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        get_object(self.eval(context))?.eval_bool(context)
    }
}

impl Evaluator {
    /// Reset the status of the evaluator for further evaluation
    pub fn reset(&self) {
        self.stack.borrow_mut().clear();
    }
}

impl TryFrom<common_pb::Value> for Operand {
    type Error = ParsePbError;

    fn try_from(value: common_pb::Value) -> Result<Self, Self::Error> {
        Ok(Self::Const(value.try_into()?))
    }
}

impl TryFrom<common_pb::Property> for Operand {
    type Error = ParsePbError;

    fn try_from(property: common_pb::Property) -> Result<Self, Self::Error> {
        Ok(Self::Var { tag: None, prop_key: Some(PropKey::try_from(property)?) })
    }
}

impl TryFrom<common_pb::Variable> for Operand {
    type Error = ParsePbError;

    fn try_from(var: common_pb::Variable) -> Result<Self, Self::Error> {
        let (_tag, _property) = (var.tag, var.property);
        let tag = if let Some(tag) = _tag { Some(NameOrId::try_from(tag)?) } else { None };
        if let Some(property) = _property {
            Ok(Self::Var { tag, prop_key: Some(PropKey::try_from(property)?) })
        } else {
            Ok(Self::Var { tag, prop_key: None })
        }
    }
}

impl TryFrom<common_pb::ExprOpr> for Operand {
    type Error = ParsePbError;

    fn try_from(unit: common_pb::ExprOpr) -> Result<Self, Self::Error> {
        use common_pb::expr_opr::Item::*;
        if let Some(item) = unit.item {
            match item {
                Const(c) => c.try_into(),
                Var(var) => var.try_into(),
                Vars(vars) => {
                    let mut vec = Vec::with_capacity(vars.keys.len());
                    for var in vars.keys {
                        vec.push(var.try_into()?);
                    }
                    Ok(Self::Vars(vec))
                }
                VarMap(vars) => {
                    let mut vec = Vec::with_capacity(vars.keys.len());
                    for var in vars.keys {
                        vec.push(var.try_into()?);
                    }
                    Ok(Self::VarMap(vec))
                }
                _ => Err(ParsePbError::ParseError("invalid operators for an Operand".to_string())),
            }
        } else {
            Err(ParsePbError::from("empty value provided"))
        }
    }
}

impl TryFrom<common_pb::ExprOpr> for InnerOpr {
    type Error = ParsePbError;

    fn try_from(unit: common_pb::ExprOpr) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use common_pb::expr_opr::Item::*;
        if let Some(item) = &unit.item {
            match item {
                Logical(logical) => {
                    Ok(Self::Logical(unsafe { std::mem::transmute::<_, common_pb::Logical>(*logical) }))
                }
                Arith(arith) => {
                    Ok(Self::Arith(unsafe { std::mem::transmute::<_, common_pb::Arithmetic>(*arith) }))
                }
                _ => Ok(Self::Operand(unit.clone().try_into()?)),
            }
        } else {
            Err(ParsePbError::from("empty value provided"))
        }
    }
}

impl Evaluate for Operand {
    fn eval<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<Object> {
        match self {
            Self::Const(obj) => Ok(obj.clone()),
            Self::Var { tag, prop_key } => {
                if let Some(ctxt) = context {
                    if let Some(element) = ctxt.get(tag.as_ref()) {
                        let result = if let Some(property) = prop_key {
                            let graph_element = element
                                .as_graph_element()
                                .ok_or(ExprEvalError::UnexpectedDataType(self.into()))?;
                            match property {
                                PropKey::Id => graph_element.id().into(),
                                PropKey::Label => graph_element
                                    .label()
                                    .map(|label| label.into())
                                    .ok_or(ExprEvalError::GetNoneFromContext)?,
                                PropKey::Len => graph_element.len().into(),
                                PropKey::All => graph_element
                                    .details()
                                    .ok_or(ExprEvalError::UnexpectedDataType(self.into()))?
                                    .get_all_properties()
                                    .map(|obj| {
                                        obj.into_iter()
                                            .map(|(key, value)| {
                                                let obj_key: Object = match key {
                                                    NameOrId::Str(str) => str.into(),
                                                    NameOrId::Id(id) => id.into(),
                                                };
                                                (obj_key, value)
                                            })
                                            .collect::<Vec<(Object, Object)>>()
                                            .into()
                                    })
                                    .ok_or(ExprEvalError::GetNoneFromContext)?,
                                PropKey::Key(key) => graph_element
                                    .details()
                                    .ok_or(ExprEvalError::UnexpectedDataType(self.into()))?
                                    .get_property(key)
                                    .ok_or(ExprEvalError::GetNoneFromContext)?
                                    .try_to_owned()
                                    .ok_or(ExprEvalError::OtherErr(
                                        "cannot get `Object` from `BorrowObject`".to_string(),
                                    ))?,
                            }
                        } else {
                            element
                                .as_borrow_object()
                                .try_to_owned()
                                .ok_or(ExprEvalError::OtherErr(
                                    "cannot get `Object` from `BorrowObject`".to_string(),
                                ))?
                        };

                        Ok(result)
                    } else {
                        Err(ExprEvalError::GetNoneFromContext)
                    }
                } else {
                    Err(ExprEvalError::MissingContext(InnerOpr::Operand(self.clone()).into()))
                }
            }
            Self::Vars(vars) => {
                let mut vec = Vec::with_capacity(vars.len());
                for var in vars {
                    vec.push(get_object(var.eval(context))?);
                }
                Ok(Object::Vector(vec))
            }
            Self::VarMap(vars) => {
                let mut map = BTreeMap::new();
                for var in vars {
                    let obj_key = match var {
                        Operand::Var { tag, prop_key } => {
                            let mut obj1 = Object::None;
                            let mut obj2 = Object::None;
                            if let Some(t) = tag {
                                match t {
                                    NameOrId::Str(str) => obj1 = object!(str.as_str()),
                                    NameOrId::Id(id) => obj1 = object!(*id),
                                }
                            }
                            if let Some(prop) = prop_key {
                                match prop {
                                    PropKey::Id => obj2 = object!(ID_KEY),
                                    PropKey::Label => obj2 = object!(LABEL_KEY),
                                    PropKey::Len => obj2 = object!(LENGTH_KEY),
                                    PropKey::All => obj2 = object!(ALL_KEY),
                                    PropKey::Key(key) => match key {
                                        NameOrId::Str(str) => obj2 = object!(str.as_str()),
                                        NameOrId::Id(id) => obj2 = object!(*id),
                                    },
                                }
                            }
                            Ok(object!(vec![obj1, obj2]))
                        }
                        _ => Err(ExprEvalError::Unsupported(
                            "evaluating `valueMap` on non-vars is not supported.".to_string(),
                        )),
                    }?;
                    map.insert(obj_key, get_object(var.eval(context))?);
                }
                Ok(Object::KV(map))
            }
        }
    }
}

impl Evaluate for InnerOpr {
    fn eval<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<Object> {
        match self {
            Self::Operand(item) => item.eval(context),
            _ => Err(ExprEvalError::UnmatchedOperator(self.into())),
        }
    }
}

impl InnerOpr {
    pub fn is_operand(&self) -> bool {
        match self {
            InnerOpr::Operand(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use ir_common::expr_parse::str_to_expr_pb;

    use super::*;
    use crate::apis::{DynDetails, Vertex};

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
            (
                NameOrId::from("hobbies".to_string()),
                vec!["football".to_string(), "guitar".to_string()].into(),
            ),
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
                Vertex::new(1, Some(9.into()), DynDetails::new(map1)),
                Vertex::new(2, Some(11.into()), DynDetails::new(map2)),
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
            "7 ^^ 3",         // 343
            "7 ^^ -3",        // 1 / 343
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
            "1 & 2",          // 0
            "1 | 2",          // 3
            "1 ^ 2",          // 3
            "1 << 2",         // 4
            "4 >> 2",         // 1
            "232 & 64 != 0",  // true
        ];

        let expected: Vec<Object> = vec![
            object!(10),
            object!(10.0),
            object!(21),
            object!(2),
            object!(343),
            object!(1.0 / 343.0),
            object!(1),
            object!(4),
            object!(4),
            object!(-3),
            object!(-3.0),
            object!(false),
            object!(false),
            object!(false),
            object!(true),
            object!(true),
            object!(true),
            object!(false),
            object!(true),
            object!(false),
            object!(false),
            object!(true),
            object!(false),
            object!(true),
            object!(true),
            object!(false),
            object!(0),
            object!(3),
            object!(3),
            object!(4),
            object!(1),
            object!(true),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_contains() {
        let cases: Vec<&str> = vec![
            "10 within [10, 9, 8, 7]",                                                // true
            "10 within []",                                                           // false
            "10 without []",                                                          // true
            "[10, 7] within [10, 9, 8, 7]",                                           // true
            "[10, 6] within [10, 9, 8, 7]",                                           // false
            "[10, 6] without [10, 9, 8, 7]",                                          // true
            "10.0 within [10.0, 9.0, 8.0, 7.0]",                                      // true
            "[10.0, 7.0] within [10.0, 9.0, 8.0, 7.0]",                               // true
            "[10.0, 6.0] within [10.0, 9.0, 8.0, 7.0]",                               // false
            "[10.0, 6.0] without [10.0, 9.0, 8.0, 7.0]",                              // true
            "\"a\" within [\"a\", \"b\", \"c\", \"d\"]",                              // true
            "[\"f\"] within [\"a\", \"b\", \"c\", \"d\"]",                            // false
            "[\"f\"] without [\"a\", \"b\", \"c\", \"d\"]",                           // true
            "10 within [10, 9, 8, 7] && [\"f\"] within [\"a\", \"b\", \"c\", \"d\"]", // false
            "(3 + 7) within [10, 9, 8, 7] || [\"f\"] within [\"a\", \"b\", \"c\", \"d\"]", // true
            "10 within [\"a\", \"b\", \"c\", \"d\"]",                                 // false
            "10 without [\"a\", \"b\", \"c\", \"d\"]",                                // true
        ];

        let expected: Vec<Object> = vec![
            object!(true),
            object!(false),
            object!(true),
            object!(true),
            object!(false),
            object!(true),
            object!(true),
            object!(true),
            object!(false),
            object!(true),
            object!(true),
            object!(false),
            object!(true),
            object!(false),
            object!(true),
            object!(false),
            object!(true),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
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
            "2 ^^ 10 > 10",                          // true
            "2 / 5 ^^ 2",                            // 0
            "2.0 / 5 ^^ 2",                          // 2.0 / 25
            "((1 + 2) * 3) / (7 * 8) + 12.5 / 10.1", // 1.2376237623762376
            "((1 + 2) * 3) / 7 * 8 + 12.5 / 10.1",   // 9.237623762376238
            "((1 + 2) * 3) / 7 * 8 + 12.5 / 10.1 \
                == ((1 + 2) * 3) / (7 * 8) + 12.5 / 10.1", // false
        ];

        let expected: Vec<Object> = vec![
            object!(-10),
            object!(1),
            object!(-2),
            object!(0),
            object!(-6),
            object!(0.002),
            object!(false),
            object!(true),
            object!(true),
            object!(0),
            object!(2.0 / 25.0),
            object!(1.2376237623762376),
            object!(9.237623762376238),
            object!(false),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_variable() {
        // [v0: id = 1, label = 9, age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]
        // [v1: id = 2, label = 11, age = 26, name = Jimmy, birthday = 19950816]
        let ctxt = prepare_context();
        let cases: Vec<&str> = vec![
            "@0.~id",                                      // 1
            "@1.~label",                                   // 11
            "@1.~label && @1.~label == 11",                // true
            "@0.~id < @1.~id",                             // true
            "@0.birthday > @1.birthday",                   // false
            "@0.~label == @1.~label",                      // false
            "@0.name != @1.name",                          // true
            "@0.name == \"John\"",                         // true
            "@0.name == \"John\" && @1.name == \"Jimmy\"", // false
            "@0.age + @0.birthday / 10000 == \
                            @1.age + @1.birthday / 10000", // true
            "@0.hobbies within [\"football\", \"guitar\", \"chess\"]", // true
            "[@0.name, @0.age]",                           // [\"John\"", 31]
            "{@0.name, @0.age}",                           // {"name": "John", "age": 31}
            "@0.~all", // {age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]}
            "{@0.~all}", // {~all, {age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]}}
        ];

        let expected: Vec<Object> = vec![
            object!(1),
            object!(11),
            object!(true),
            object!(true),
            object!(false),
            object!(false),
            object!(true),
            object!(true),
            object!(false),
            object!(true),
            object!(true),
            object!(vec![object!("John"), object!(31)]),
            Object::KV(
                vec![
                    (object!(vec![object!(0), object!("age")]), object!(31)),
                    (object!(vec![object!(0), object!("name")]), object!("John")),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![
                    (object!("age"), object!(31)),
                    (object!("name"), object!("John")),
                    (object!("birthday"), object!(19900416)),
                    (object!("hobbies"), object!(vec!["football", "guitar"])),
                ]
                .into_iter()
                .collect(),
            ),
            Object::KV(
                vec![(
                    object!(vec![object!(0), object!("~all")]),
                    Object::KV(
                        vec![
                            (object!("age"), object!(31)),
                            (object!("name"), object!("John")),
                            (object!("birthday"), object!(19900416)),
                            (object!("hobbies"), object!(vec!["football", "guitar"])),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                )]
                .into_iter()
                .collect(),
            ),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_errors() {
        let cases: Vec<&str> = vec![
            "@2",
            "+",
            "1 + ",
            "1 1",
            "1 1 2",
            "1 1 + 2",
            "1 + @1.age * 1 1 - 1 - 5",
            "@2",
            "@0.not_exist",
        ];
        let ctxt = prepare_context();

        let expected: Vec<ExprEvalError> = vec![
            // Evaluate variable without providing the context
            ExprEvalError::MissingContext(
                InnerOpr::Operand(Operand::Var { tag: Some(2.into()), prop_key: None }).into(),
            ),
            // try to evaluate neither a variable nor a const
            ExprEvalError::UnmatchedOperator(InnerOpr::Arith(common_pb::Arithmetic::Add).into()),
            ExprEvalError::MissingOperands(InnerOpr::Arith(common_pb::Arithmetic::Add).into()),
            ExprEvalError::OtherErr("invalid expression".to_string()),
            ExprEvalError::OtherErr("invalid expression".to_string()),
            ExprEvalError::OtherErr("invalid expression".to_string()),
            ExprEvalError::OtherErr("invalid expression".to_string()),
            ExprEvalError::GetNoneFromContext,
            ExprEvalError::GetNoneFromContext,
        ];

        let mut is_context = false;
        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
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
