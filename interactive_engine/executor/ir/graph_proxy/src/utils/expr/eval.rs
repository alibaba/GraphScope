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

use super::eval_pred::PEvaluator;
use crate::apis::{Element, PropKey};
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
    stack: RefCell<Vec<ExprEvalResult<Object>>>,
}

unsafe impl Sync for Evaluator {}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Const(Object),
    Var { tag: Option<NameOrId>, prop_key: Option<PropKey> },
    Vars(Vec<Operand>),
    VarMap(Vec<Operand>),
    // TODO: this is the new definition of VarMap. Will replace VarMap soon.
    Map(Vec<(Object, Operand)>),
    // this is to concat multiple fields (refer to paths, or Strings) into one
    Concat(Vec<Operand>),
}

#[derive(Debug, Clone)]
pub enum Function {
    Extract(common_pb::extract::Interval),
}

#[derive(Debug)]
pub struct CaseWhen {
    when_then_evals: Vec<(PEvaluator, Evaluator)>,
    else_eval: Evaluator,
}

impl TryFrom<common_pb::Case> for CaseWhen {
    type Error = ParsePbError;

    fn try_from(case: common_pb::Case) -> Result<Self, Self::Error> {
        let mut when_then_evals = Vec::with_capacity(case.when_then_expressions.len());
        for when_then in &case.when_then_expressions {
            let when = when_then
                .when_expression
                .as_ref()
                .ok_or(ParsePbError::EmptyFieldError(format!("missing when expression {:?}", case)))?;
            let then = when_then
                .then_result_expression
                .as_ref()
                .ok_or(ParsePbError::EmptyFieldError(format!("missing then expression: {:?}", case)))?;
            when_then_evals.push((PEvaluator::try_from(when.clone())?, Evaluator::try_from(then.clone())?));
        }
        let else_result_expression = case
            .else_result_expression
            .as_ref()
            .ok_or(ParsePbError::EmptyFieldError(format!("missing else expression: {:?}", case)))?;
        let else_eval = Evaluator::try_from(else_result_expression.clone())?;
        Ok(Self { when_then_evals, else_eval })
    }
}

/// A conditional expression for evaluating a casewhen. More conditional expressions can be added in the future, e.g., COALESCE()，NULLIF() etc.
#[derive(Debug)]
pub enum Conditional {
    Case(CaseWhen),
}

/// An inner representation of `common_pb::ExprOpr` for one-shot translation of `common_pb::ExprOpr`.
#[derive(Debug)]
pub(crate) enum InnerOpr {
    Logical(common_pb::Logical),
    Arith(common_pb::Arithmetic),
    Function(Function),
    Operand(Operand),
    Conditional(Conditional),
}

impl ToString for InnerOpr {
    fn to_string(&self) -> String {
        match self {
            InnerOpr::Logical(logical) => format!("{:?}", logical),
            InnerOpr::Arith(arith) => format!("{:?}", arith),
            InnerOpr::Operand(item) => format!("{:?}", item),
            InnerOpr::Function(func) => format!("{:?}", func),
            InnerOpr::Conditional(conditional) => format!("{:?}", conditional),
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

impl From<&PropKey> for OperatorDesc {
    fn from(prop: &PropKey) -> Self {
        Self(format!("{:?}", prop))
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

    fn try_from(expression: common_pb::Expression) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        let suffix_oprs = to_suffix_expr(expression.operators)
            .map_err(|err| ParsePbError::ParseError(format!("{:?}", err)))?;
        suffix_oprs.try_into()
    }
}

impl TryFrom<Vec<common_pb::ExprOpr>> for Evaluator {
    type Error = ParsePbError;

    fn try_from(suffix_oprs: Vec<common_pb::ExprOpr>) -> ParsePbResult<Self> {
        let mut inner_tree: Vec<InnerOpr> = Vec::with_capacity(suffix_oprs.len());
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
    if a.eq(&Object::None) || b.eq(&Object::None) {
        return Ok(Object::None);
    }
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

pub(crate) fn apply_function<'a>(
    function: &Function, a: BorrowObject<'a>, _b_opt: Option<BorrowObject<'a>>,
) -> ExprEvalResult<Object> {
    use common_pb::extract::Interval;
    match function {
        Function::Extract(interval) => match interval {
            Interval::Year => Ok(a
                .as_date_format()?
                .year()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                .into()),
            Interval::Month => Ok((a
                .as_date_format()?
                .month()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                as i32)
                .into()),
            Interval::Day => Ok((a
                .as_date_format()?
                .day()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                as i32)
                .into()),
            Interval::Hour => Ok((a
                .as_date_format()?
                .hour()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                as i32)
                .into()),
            Interval::Minute => Ok((a
                .as_date_format()?
                .minute()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                as i32)
                .into()),
            Interval::Second => Ok((a
                .as_date_format()?
                .second()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                as i32)
                .into()),
            Interval::Millisecond => Ok((a
                .as_date_format()?
                .millisecond()
                .ok_or_else(|| ExprEvalError::GetNoneFromContext)?
                as i32)
                .into()),
        },
    }
}

pub(crate) fn apply_logical<'a>(
    logical: &common_pb::Logical, a: BorrowObject<'a>, b_opt: Option<BorrowObject<'a>>,
) -> ExprEvalResult<Object> {
    use common_pb::Logical::*;
    if logical == &Not {
        if a.eq(&Object::None) {
            Ok(Object::None)
        } else {
            Ok((!a.eval_bool::<(), NoneContext>(None)?).into())
        }
    } else if logical == &Isnull {
        Ok(a.eq(&BorrowObject::None).into())
    } else {
        if b_opt.is_some() {
            let b = b_opt.unwrap();
            // process null values
            if a.eq(&Object::None) || b.eq(&Object::None) {
                match logical {
                    And => {
                        if (a != Object::None && !a.eval_bool::<(), NoneContext>(None)?)
                            || (b != Object::None && !b.eval_bool::<(), NoneContext>(None)?)
                        {
                            Ok(false.into())
                        } else {
                            Ok(Object::None)
                        }
                    }
                    Or => {
                        if (a != Object::None && a.eval_bool::<(), NoneContext>(None)?)
                            || (b != Object::None && b.eval_bool::<(), NoneContext>(None)?)
                        {
                            Ok(true.into())
                        } else {
                            Ok(Object::None)
                        }
                    }
                    _ => Ok(Object::None),
                }
            } else {
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
                    Regex => {
                        let regex = regex::Regex::new(b.as_str()?.as_ref())?;
                        Ok(regex.is_match(a.as_str()?.as_ref()).into())
                    }
                    Not => unreachable!(),
                    Isnull => unreachable!(),
                }
            }
        } else {
            Err(ExprEvalError::MissingOperands(InnerOpr::Logical(*logical).into()))
        }
    }
}

pub(crate) fn apply_condition_expr<'a, E: Element, C: Context<E>>(
    condition: &Conditional, context: Option<&C>,
) -> ExprEvalResult<Object> {
    match condition {
        Conditional::Case(case) => {
            let else_expr = &case.else_eval;
            for (when, then) in case.when_then_evals.iter() {
                if when.eval_bool(context)? {
                    return then.eval(context);
                }
            }
            return else_expr.eval(context);
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
            if let InnerOpr::Conditional(case) = _first.unwrap() {
                apply_condition_expr(case, context)
            } else {
                _first.unwrap().eval(context)
            }
        } else if self.suffix_tree.len() == 2 {
            let first = _first.unwrap();
            let second = _second.unwrap();
            if let InnerOpr::Logical(logical) = second {
                let mut first = first.eval(context);
                if common_pb::Logical::Isnull.eq(logical) {
                    match first {
                        Err(ExprEvalError::GetNoneFromContext) => first = Ok(Object::None),
                        _ => {}
                    }
                }
                Ok(apply_logical(logical, first?.as_borrow(), None)?)
            } else if let InnerOpr::Function(function) = second {
                let first = first.eval(context)?;
                Ok(apply_function(function, first.as_borrow(), None)?)
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
                if third.is_unary() {
                    // to deal with two unary operators cases, e.g., !(!true), !(isNull(a)),isNull(extract(a)) etc.
                    if let InnerOpr::Logical(inner_logical) = second {
                        let mut inner_first = first.eval(context);
                        if common_pb::Logical::Isnull.eq(inner_logical) {
                            match inner_first {
                                Err(ExprEvalError::GetNoneFromContext) => inner_first = Ok(Object::None),
                                _ => {}
                            }
                        }
                        let mut first = Ok(apply_logical(inner_logical, inner_first?.as_borrow(), None)?);
                        if common_pb::Logical::Isnull.eq(logical) {
                            match first {
                                Err(ExprEvalError::GetNoneFromContext) => first = Ok(Object::None),
                                _ => {}
                            }
                        }
                        return Ok(apply_logical(logical, first?.as_borrow(), None)?);
                    } else if let InnerOpr::Function(function) = second {
                        let inner_first = first.eval(context)?;
                        return Ok(apply_function(function, inner_first.as_borrow(), None)?);
                    } else {
                        return Err(ExprEvalError::OtherErr("invalid expression".to_string()));
                    }
                } else {
                    let a = first.eval(context)?;
                    let b = second.eval(context)?;
                    Ok(apply_logical(logical, a.as_borrow(), Some(b.as_borrow()))?)
                }
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
                stack.push(opr.eval(context));
            } else {
                if let Some(first) = stack.pop() {
                    let rst = match opr {
                        InnerOpr::Logical(logical) => {
                            if logical == &common_pb::Logical::Not {
                                apply_logical(logical, first?.as_borrow(), None)
                            } else if logical == &common_pb::Logical::Isnull {
                                let first_obj = match first {
                                    Ok(obj) => obj,
                                    Err(err) => match err {
                                        ExprEvalError::GetNoneFromContext => Object::None,
                                        _ => return Err(err),
                                    },
                                };
                                apply_logical(logical, first_obj.as_borrow(), None)
                            } else {
                                if let Some(second) = stack.pop() {
                                    apply_logical(logical, second?.as_borrow(), Some(first?.as_borrow()))
                                } else {
                                    Err(ExprEvalError::OtherErr("invalid expression".to_string()))
                                }
                            }
                        }
                        InnerOpr::Function(function) => {
                            if opr.is_unary() {
                                apply_function(function, first?.as_borrow(), None)
                            } else {
                                if let Some(second) = stack.pop() {
                                    apply_function(function, second?.as_borrow(), Some(first?.as_borrow()))
                                } else {
                                    Err(ExprEvalError::OtherErr("invalid expression".to_string()))
                                }
                            }
                        }
                        InnerOpr::Arith(arith) => {
                            if let Some(second) = stack.pop() {
                                apply_arith(arith, second?.as_borrow(), first?.as_borrow())
                            } else {
                                Err(ExprEvalError::OtherErr("invalid expression".to_string()))
                            }
                        }
                        _ => unreachable!(),
                    };
                    stack.push(rst);
                }
            }
        }

        if stack.len() == 1 {
            Ok(stack.pop().unwrap()?)
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

impl TryFrom<common_pb::VariableKeyValues> for Operand {
    type Error = ParsePbError;

    fn try_from(key_vals: common_pb::VariableKeyValues) -> Result<Self, Self::Error> {
        let mut vec = Vec::with_capacity(key_vals.key_vals.len());
        for key_val in key_vals.key_vals {
            let (_key, _value) = (key_val.key, key_val.value);
            let key = if let Some(key) = _key {
                Object::try_from(key)?
            } else {
                return Err(ParsePbError::from("empty key provided in Map"));
            };
            let value = if let Some(value) = _value {
                match value {
                    common_pb::variable_key_value::Value::Val(val) => Operand::try_from(val)?,
                    common_pb::variable_key_value::Value::Nested(nested) => Operand::try_from(nested)?,
                    common_pb::variable_key_value::Value::PathFunc(_path_func) => todo!(),
                }
            } else {
                return Err(ParsePbError::from("empty value provided in Map"));
            };
            vec.push((key, value));
        }
        Ok(Self::Map(vec))
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
                Map(key_vals) => Operand::try_from(key_vals),
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
                Extract(extract) => Ok(Self::Function(Function::Extract(unsafe {
                    std::mem::transmute::<_, common_pb::extract::Interval>(extract.interval)
                }))),
                Case(case) => Ok(Self::Conditional(Conditional::Case(case.clone().try_into()?))),
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
            Operand::Const(obj) => Ok(obj.clone()),
            Operand::Var { tag, prop_key } => {
                if let Some(ctxt) = context {
                    if let Some(element) = ctxt.get(tag.as_ref()) {
                        let result = if let Some(property) = prop_key {
                            property.get_key(element)?
                        } else {
                            element
                                .as_borrow_object()
                                .try_to_owned()
                                .ok_or_else(|| {
                                    ExprEvalError::OtherErr(
                                        "cannot get `Object` from `BorrowObject`".to_string(),
                                    )
                                })?
                        };

                        Ok(result)
                    } else {
                        Err(ExprEvalError::GetNoneFromContext)
                    }
                } else {
                    Err(ExprEvalError::MissingContext(InnerOpr::Operand(self.clone()).into()))
                }
            }
            Operand::Vars(vars) => {
                let mut vec = Vec::with_capacity(vars.len());
                for var in vars {
                    vec.push(get_object(var.eval(context))?);
                }
                Ok(Object::Vector(vec))
            }
            Operand::VarMap(vars) => {
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
            Operand::Map(vars) => {
                let mut map = BTreeMap::new();
                for (obj_key, var) in vars {
                    map.insert(obj_key.clone(), get_object(var.eval(context))?);
                }
                Ok(Object::KV(map))
            }
            Operand::Concat(_) => {
                Err(ExprEvalError::Unsupported("evaluating `Concat` is not supported.".to_string()))
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

    pub fn is_unary(&self) -> bool {
        match self {
            InnerOpr::Logical(logical) => match logical {
                common_pb::Logical::Not | common_pb::Logical::Isnull => true,
                _ => false,
            },
            InnerOpr::Function(function) => match function {
                Function::Extract(_) => true,
            },
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use dyn_type::DateTimeFormats;
    use ir_common::{expr_parse::str_to_expr_pb, generated::physical::physical_opr::operator};

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
            "!(!true)",       // true
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
        // [v1: id = 2, label = 11, age = 26, name = Nancy, birthday = 19950816]
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
            "@0.not_exist", // Object::None
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
            Object::None,
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_errors() {
        let cases: Vec<&str> =
            vec!["@2", "+", "1 + ", "1 1", "1 1 2", "1 1 + 2", "1 + @1.age * 1 1 - 1 - 5", "@2"];
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

    #[test]
    fn test_eval_is_null() {
        // [v0: id = 1, label = 9, age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]
        // [v1: id = 2, label = 11, age = 26, name = Nancy, birthday = 19950816]
        let ctxt = prepare_context();
        let cases: Vec<&str> = vec![
            "isNull @0.hobbies",                 // false
            "isNull (@0.hobbies)",               // false
            "!(isNull @0.hobbies)",              // true
            "isNull @1.hobbies",                 // true
            "isNull (@1.hobbies)",               // true
            "!(isNull @1.hobbies)",              // false
            "isNull true",                       // false
            "isNull false",                      // false
            "!(isNull true)",                    // true
            "isNull @1.hobbies && @1.age == 26", // true
        ];
        let expected: Vec<Object> = vec![
            object!(false),
            object!(false),
            object!(true),
            object!(true),
            object!(true),
            object!(false),
            object!(false),
            object!(false),
            object!(true),
            object!(true),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    fn prepare_context_with_date() -> Vertices {
        let map1: HashMap<NameOrId, Object> = vec![
            (
                NameOrId::from("date1".to_string()),
                (DateTimeFormats::from_str("2020-08-08").unwrap()).into(),
            ),
            (
                NameOrId::from("date2".to_string()),
                chrono::NaiveDate::from_ymd_opt(2020, 8, 8)
                    .unwrap()
                    .into(),
            ),
            (
                NameOrId::from("time1".to_string()),
                (DateTimeFormats::from_str("10:11:12.100").unwrap()).into(),
            ),
            (
                NameOrId::from("time2".to_string()),
                chrono::NaiveTime::from_hms_milli_opt(10, 11, 12, 100)
                    .unwrap()
                    .into(),
            ),
            (
                NameOrId::from("datetime1".to_string()),
                (DateTimeFormats::from_str("2020-08-08T23:11:12.100-11:00").unwrap()).into(),
            ),
            (
                NameOrId::from("datetime2".to_string()),
                (DateTimeFormats::from_str("2020-08-09 10:11:12.100").unwrap()).into(),
            ),
            (
                NameOrId::from("datetime3".to_string()),
                chrono::NaiveDateTime::from_timestamp_millis(1602324610100)
                    .unwrap()
                    .into(),
            ), // 2020-10-10 10:10:10
        ]
        .into_iter()
        .collect();
        Vertices { vec: vec![Vertex::new(1, Some(9.into()), DynDetails::new(map1))] }
    }

    fn prepare_extract(expr_str: &str, interval: common_pb::extract::Interval) -> common_pb::Expression {
        let mut operators = str_to_expr_pb(expr_str.to_string())
            .unwrap()
            .operators;
        let extract_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: interval as i32,
            })),
        };
        operators.push(extract_opr);
        common_pb::Expression { operators }
    }

    #[test]
    fn test_eval_extract() {
        let ctxt = prepare_context_with_date();
        let cases = vec![
            // date1: "2020-08-08"
            prepare_extract("@0.date1", common_pb::extract::Interval::Year),
            prepare_extract("@0.date1", common_pb::extract::Interval::Month),
            prepare_extract("@0.date1", common_pb::extract::Interval::Day),
            // date2: 20200808
            prepare_extract("@0.date2", common_pb::extract::Interval::Year),
            prepare_extract("@0.date2", common_pb::extract::Interval::Month),
            prepare_extract("@0.date2", common_pb::extract::Interval::Day),
            // time1: "10:11:12.100"
            prepare_extract("@0.time1", common_pb::extract::Interval::Hour),
            prepare_extract("@0.time1", common_pb::extract::Interval::Minute),
            prepare_extract("@0.time1", common_pb::extract::Interval::Second),
            prepare_extract("@0.time1", common_pb::extract::Interval::Millisecond),
            // time2: 101112100
            prepare_extract("@0.time2", common_pb::extract::Interval::Hour),
            prepare_extract("@0.time2", common_pb::extract::Interval::Minute),
            prepare_extract("@0.time2", common_pb::extract::Interval::Second),
            prepare_extract("@0.time2", common_pb::extract::Interval::Millisecond),
            // datetime1: "2020-08-08T23:11:12.100-11:00"
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Year),
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Month),
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Day),
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Hour),
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Minute),
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Second),
            prepare_extract("@0.datetime1", common_pb::extract::Interval::Millisecond),
            // datetime2: "2020-08-09 10:11:12.100"
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Year),
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Month),
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Day),
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Hour),
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Minute),
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Second),
            prepare_extract("@0.datetime2", common_pb::extract::Interval::Millisecond),
            // datetime3: 1602324610100, i.e., 2020-10-10 10:10:10
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Year),
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Month),
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Day),
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Hour),
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Minute),
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Second),
            prepare_extract("@0.datetime3", common_pb::extract::Interval::Millisecond),
        ];

        let expected = vec![
            object!(2020),
            object!(8),
            object!(8),
            object!(2020),
            object!(8),
            object!(8),
            object!(10),
            object!(11),
            object!(12),
            object!(100),
            object!(10),
            object!(11),
            object!(12),
            object!(100),
            object!(2020),
            object!(8),
            object!(8),
            object!(23),
            object!(11),
            object!(12),
            object!(100),
            object!(2020),
            object!(8),
            object!(9),
            object!(10),
            object!(11),
            object!(12),
            object!(100),
            object!(2020),
            object!(10),
            object!(10),
            object!(10),
            object!(10),
            object!(10),
            object!(100),
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(case).unwrap();
            println!("{:?}", eval.eval::<_, Vertices>(Some(&ctxt)).unwrap());
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_extract_02() {
        // expression: 0.date1.day >= 9, fall back to general evaluator
        // date1: "2020-08-08"
        // datetime2: "2020-08-09 10:11:12.100"
        let mut operators1 = str_to_expr_pb("@0.date1".to_string())
            .unwrap()
            .operators;
        let mut operators2 = str_to_expr_pb("@0.datetime2".to_string())
            .unwrap()
            .operators;
        let extract_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Extract(common_pb::Extract {
                interval: common_pb::extract::Interval::Day as i32,
            })),
        };
        operators1.push(extract_opr.clone());
        operators2.push(extract_opr);
        let cmp_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Logical(common_pb::Logical::Ge as i32)),
        };
        operators1.push(cmp_opr.clone());
        operators2.push(cmp_opr);
        let right = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Const(common_pb::Value {
                item: Some(common_pb::value::Item::I32(9)),
            })),
        };
        operators1.push(right.clone());
        operators2.push(right);
        let expr1 = common_pb::Expression { operators: operators1 };
        let expr2 = common_pb::Expression { operators: operators2 };

        let eval1 = PEvaluator::try_from(expr1).unwrap();
        let eva2 = PEvaluator::try_from(expr2).unwrap();
        match eval1 {
            PEvaluator::Predicates(_) => panic!("should fall back to general evaluator"),
            PEvaluator::General(_) => assert!(true),
        }
        match eva2 {
            PEvaluator::Predicates(_) => panic!("should fall back to general evaluator"),
            PEvaluator::General(_) => assert!(true),
        }
        let ctxt = prepare_context_with_date();
        assert_eq!(
            eval1
                .eval_bool::<_, Vertices>(Some(&ctxt))
                .unwrap(),
            false
        );
        assert_eq!(
            eva2.eval_bool::<_, Vertices>(Some(&ctxt))
                .unwrap(),
            true
        );
    }

    fn gen_regex_expression(to_match: &str, pattern: &str) -> common_pb::Expression {
        let mut regex_expr = common_pb::Expression { operators: vec![] };
        let left = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Const(common_pb::Value {
                item: Some(common_pb::value::Item::Str(to_match.to_string())),
            })),
        };
        regex_expr.operators.push(left);
        let regex_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Logical(common_pb::Logical::Regex as i32)),
        };
        regex_expr.operators.push(regex_opr);
        let right = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Const(common_pb::Value {
                item: Some(common_pb::value::Item::Str(pattern.to_string())),
            })),
        };
        regex_expr.operators.push(right);
        regex_expr
    }

    #[test]
    fn test_eval_regex() {
        // TODO: the parser does not support escape characters in regex well yet.
        // So use gen_regex_expression() to help generate expression
        let cases: Vec<(&str, &str)> = vec![
            ("Josh", r"^J"),                                                    // startWith, true
            ("Josh", r"^J.*"),                                                  // startWith, true
            ("josh", r"^J.*"),                                                  // startWith, false
            ("BJosh", r"^J.*"),                                                 // startWith, false
            ("Josh", r"J.*"),                                                   // true
            ("Josh", r"h$"),                                                    // endWith, true
            ("Josh", r".*h$"),                                                  // endWith, true
            ("JosH", r".*h$"),                                                  // endWith, false
            ("JoshB", r".*h$"),                                                 // endWith, false
            ("Josh", r".*h"),                                                   // true
            ("Josh", r"os"),                                                    // true
            ("Josh", r"A.*"),                                                   // false
            ("Josh", r".*A"),                                                   // false
            ("Josh", r"ab"),                                                    // false
            ("Josh", r"Josh.+"),                                                // false
            ("2010-03-14", r"^\d{4}-\d{2}-\d{2}$"),                             // true
            (r"I categorically deny having triskaidekaphobia.", r"\b\w{13}\b"), //true
        ];
        let expected: Vec<Object> = vec![
            object!(true),
            object!(true),
            object!(false),
            object!(false),
            object!(true),
            object!(true),
            object!(true),
            object!(false),
            object!(false),
            object!(true),
            object!(true),
            object!(false),
            object!(false),
            object!(false),
            object!(false),
            object!(true),
            object!(true),
        ];

        for ((to_match, pattern), expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(gen_regex_expression(to_match, pattern)).unwrap();
            assert_eq!(eval.eval::<(), NoneContext>(None).unwrap(), expected);
        }
    }

    fn prepare_casewhen(when_then_exprs: Vec<(&str, &str)>, else_expr: &str) -> common_pb::Expression {
        let mut when_then_expressions = vec![];
        for (when_expr, then_expr) in when_then_exprs {
            when_then_expressions.push(common_pb::case::WhenThen {
                when_expression: Some(str_to_expr_pb(when_expr.to_string()).unwrap()),
                then_result_expression: Some(str_to_expr_pb(then_expr.to_string()).unwrap()),
            });
        }
        let case_when_opr = common_pb::ExprOpr {
            node_type: None,
            item: Some(common_pb::expr_opr::Item::Case(common_pb::Case {
                when_then_expressions,
                else_result_expression: Some(str_to_expr_pb(else_expr.to_string()).unwrap()),
            })),
        };
        common_pb::Expression { operators: vec![case_when_opr] }
    }

    #[test]
    fn test_eval_casewhen() {
        // [v0: id = 1, label = 9, age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]
        // [v1: id = 2, label = 11, age = 26, name = Nancy, birthday = 19950816]
        let ctxt = prepare_context();
        let cases = vec![
            (vec![("@0.~id ==1", "1"), ("@0.~id == 2", "2")], "0"),
            (vec![("@0.~id > 10", "1"), ("@0.~id<5", "2")], "0"),
            (vec![("@0.~id < 10 && @0.~id>20", "true")], "false"),
            (vec![("@0.~id < 10 || @0.~id>20", "true")], "false"),
            (vec![("@0.~id < 10 && @0.~id>20", "1+2")], "4+5"),
            (vec![("@0.~id < 10 || @0.~id>20", "1+2")], "4+5"),
            (vec![("true", "@0.name")], "@1.name"),
            (vec![("false", "@0.~name")], "@1.name"),
            (vec![("isNull @0.hobbies", "true")], "false"),
            (vec![("isNull @1.hobbies", "true")], "false"),
        ];
        let expected: Vec<Object> = vec![
            object!(1),
            object!(2),
            object!(false),
            object!(true),
            object!(9),
            object!(3),
            object!("John"),
            object!("Nancy"),
            object!(false),
            object!(true),
        ];

        for ((when_then_exprs, else_expr), expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(prepare_casewhen(when_then_exprs, else_expr)).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }

    #[test]
    fn test_eval_null() {
        // [v0: id = 1, label = 9, age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]
        // [v1: id = 2, label = 11, age = 26, name = Jimmy, birthday = 19950816]
        let ctxt = prepare_context();
        let cases = vec![
            ("isNull @1.hobbies"),          // true
            ("@1.hobbies + 1"),             // null
            ("@1.hobbies  + @1.hobbies "),  // null
            ("@1.hobbies  == @1.hobbies "), // null
            ("@1.hobbies  != @1.hobbies "), // null
            ("@1.hobbies  > @1.hobbies "),  // null
            ("false && @1.hobbies"),        // false
            ("true && @1.hobbies"),         // null
            ("@1.hobbies && @1.hobbies"),   // null
            ("true || @1.hobbies"),         // true
            ("false || @1.hobbies"),        // null
            ("@1.hobbies || @1.hobbies"),   // null
            ("!@1.hobbies"),                // null
        ];
        let expected: Vec<Object> = vec![
            object!(true),
            Object::None,
            Object::None,
            Object::None,
            Object::None,
            Object::None,
            object!(false),
            Object::None,
            Object::None,
            object!(true),
            Object::None,
            Object::None,
            Object::None,
        ];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = Evaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(eval.eval::<_, Vertices>(Some(&ctxt)).unwrap(), expected);
        }
    }
}
