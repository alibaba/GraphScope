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

use std::convert::{TryFrom, TryInto};

use dyn_type::{BorrowObject, Object};
use ir_common::error::ParsePbError;
use ir_common::expr_parse::error::{ExprError, ExprResult};
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;

use crate::apis::{Element, PropKey};
use crate::utils::expr::eval::{apply_logical, Context, Evaluate, Evaluator, Operand};
use crate::utils::expr::{ExprEvalError, ExprEvalResult};

/// The trait to define evaluating a predicate, which return `bool` value.
pub trait EvalPred {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnaryPredicate {
    pub(crate) operand: Operand,
    pub(crate) cmp: common_pb::Logical,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub(crate) left: Operand,
    pub(crate) cmp: common_pb::Logical,
    pub(crate) right: Operand,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum Partial {
    // a single item can represent an operand (if only left is_some()), a unary operator (if both left and cmp is_some()),
    // or a binary operator (if all left, cmp and right is_some())
    SingleItem { left: Option<Operand>, cmp: Option<common_pb::Logical>, right: Option<Operand> },
    Predicates(Predicates),
}

impl Default for Partial {
    fn default() -> Self {
        Self::SingleItem { left: None, cmp: None, right: None }
    }
}

#[allow(dead_code)]
impl Partial {
    pub fn get_left(&self) -> Option<&Operand> {
        match self {
            Partial::SingleItem { left, cmp: _, right: _ } => left.as_ref(),
            Partial::Predicates(_) => None,
        }
    }

    pub fn left(&mut self, item: Operand) -> ExprResult<()> {
        match self {
            Self::SingleItem { left, cmp: _, right: _ } => {
                *left = Some(item);
                Ok(())
            }
            Self::Predicates(_) => {
                Err(ExprError::OtherErr(format!("invalid predicate: {:?}, {:?}", self, item)))
            }
        }
    }

    pub fn cmp(&mut self, logical: common_pb::Logical) -> ExprResult<()> {
        match self {
            Partial::SingleItem { left: _, cmp, right: _ } => {
                if cmp.is_some() {
                    Err(ExprError::OtherErr(format!("invalid predicate: {:?}, {:?}", self, logical)))
                } else {
                    *cmp = Some(logical);
                    Ok(())
                }
            }
            Partial::Predicates(_) => Ok(()),
        }
    }

    pub fn right(&mut self, item: Operand) -> ExprResult<()> {
        match self {
            Partial::SingleItem { left: _, cmp, right } => {
                if cmp.is_none() || right.is_some() {
                    Err(ExprError::OtherErr(format!("invalid predicate: {:?}, {:?}", self, item)))
                } else {
                    *right = Some(item);
                    Ok(())
                }
            }
            Partial::Predicates(_) => Ok(()),
        }
    }

    pub fn predicates(&mut self, pred: Predicates) -> ExprResult<()> {
        match self {
            Partial::SingleItem { left, cmp: _, right: _ } => {
                if !left.is_none() {
                    return Err(ExprError::OtherErr(format!("invalid predicate: {:?}, {:?}", self, pred)));
                }
            }
            Partial::Predicates(_) => {}
        }
        *self = Self::Predicates(pred);

        Ok(())
    }
}

impl TryFrom<Partial> for Predicates {
    type Error = ParsePbError;
    fn try_from(partial: Partial) -> Result<Self, Self::Error> {
        let partical_old = partial.clone();
        let predicate = match partial {
            Partial::SingleItem { left, cmp, right } => {
                if left.is_none() {
                    None
                } else if cmp.is_none() {
                    // the case of operand
                    Some(Predicates::SingleItem(left.unwrap()))
                } else if right.is_none() {
                    // the case of unary operator
                    if !cmp.unwrap().is_unary() {
                        None
                    } else {
                        Some(Predicates::Unary(UnaryPredicate {
                            operand: left.unwrap(),
                            cmp: cmp.unwrap(),
                        }))
                    }
                } else {
                    // the case of binary operator
                    if !cmp.unwrap().is_binary() {
                        None
                    } else {
                        Some(Predicates::Binary(Predicate {
                            left: left.unwrap(),
                            cmp: cmp.unwrap(),
                            right: right.unwrap(),
                        }))
                    }
                }
            }
            Partial::Predicates(pred) => Some(pred),
        };
        predicate.ok_or_else(|| ParsePbError::ParseError(format!("invalid predicate: {:?}", partical_old)))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicates {
    Init,
    SingleItem(Operand),
    Unary(UnaryPredicate),
    Binary(Predicate),
    Not(Box<Predicates>),
    And((Box<Predicates>, Box<Predicates>)),
    Or((Box<Predicates>, Box<Predicates>)),
}

impl Default for Predicates {
    fn default() -> Self {
        Self::Init
    }
}

impl TryFrom<pb::index_predicate::Triplet> for Predicates {
    type Error = ParsePbError;

    fn try_from(triplet: pb::index_predicate::Triplet) -> Result<Self, Self::Error> {
        let value = if let Some(value) = &triplet.value {
            match &value {
                pb::index_predicate::triplet::Value::Const(v) => Some(v.clone()),
                _ => Err(ParsePbError::Unsupported(format!(
                    "unsupported indexed predicate value {:?}",
                    value
                )))?,
            }
        } else {
            None
        };
        let partial = Partial::SingleItem {
            left: triplet
                .key
                .map(|var| var.try_into())
                .transpose()?,
            cmp: Some(unsafe { std::mem::transmute(triplet.cmp) }),
            right: value.map(|val| val.try_into()).transpose()?,
        };

        partial.try_into()
    }
}

impl TryFrom<pb::index_predicate::AndPredicate> for Predicates {
    type Error = ParsePbError;

    fn try_from(and_predicate: pb::index_predicate::AndPredicate) -> Result<Self, Self::Error> {
        let mut predicates: Option<Predicates> = None;
        let mut is_first = true;
        for triplet in and_predicate.predicates {
            if is_first {
                predicates = Some(triplet.try_into()?);
                is_first = false;
            } else {
                let new_pred = triplet.try_into()?;
                predicates = predicates.map(|pred| pred.and(new_pred));
            }
        }

        predicates.ok_or_else(|| {
            ParsePbError::ParseError("invalid `AndPredicate` in `IndexPredicate`".to_string())
        })
    }
}

impl TryFrom<pb::IndexPredicate> for Predicates {
    type Error = ParsePbError;

    fn try_from(index_predicate: pb::IndexPredicate) -> Result<Self, Self::Error> {
        let mut predicates: Option<Predicates> = None;
        let mut is_first = true;
        for and_triplet in index_predicate.or_predicates {
            if is_first {
                predicates = Some(and_triplet.try_into()?);
                is_first = false;
            } else {
                let new_pred = and_triplet.try_into()?;
                predicates = predicates.map(|pred| pred.or(new_pred));
            }
        }

        predicates.ok_or_else(|| (ParsePbError::ParseError("invalid `IndexPredicate`".to_string())))
    }
}

impl<'a> EvalPred for BorrowObject<'a> {
    fn eval_bool<E: Element, C: Context<E>>(&self, _context: Option<&C>) -> ExprEvalResult<bool> {
        match *self {
            BorrowObject::Primitive(p) => Ok(p.as_bool().unwrap_or(true)),
            BorrowObject::String(str) => Ok(!str.is_empty()),
            BorrowObject::Vector(vec) => {
                for obj in vec {
                    // Every object must contain some object
                    match obj {
                        Object::None => return Ok(false),
                        _ => {}
                    }
                }
                Ok(true)
            }
            BorrowObject::KV(kv) => {
                // Every value must contain some object
                for value in kv.values() {
                    match value {
                        Object::None => return Ok(false),
                        _ => {}
                    }
                }
                Ok(true)
            }
            BorrowObject::None => Ok(false),
            _ => Ok(true),
        }
    }
}

impl EvalPred for Object {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        self.as_borrow().eval_bool(context)
    }
}

impl EvalPred for Operand {
    fn eval_bool<E: Element, C: Context<E>>(&self, _context: Option<&C>) -> ExprEvalResult<bool> {
        match self {
            Operand::Const(c) => c.eval_bool(_context),
            Operand::Var { tag, prop_key } => {
                let mut result = false;
                if let Some(context) = _context {
                    if let Some(elem) = context.get(tag.as_ref()) {
                        if let Some(key) = prop_key {
                            if let PropKey::Len = key {
                                result = elem.len() > 0
                            } else {
                                if let Some(graph_element) = elem.as_graph_element() {
                                    match key {
                                        PropKey::Id => result = true,
                                        PropKey::Label => {
                                            result = graph_element.label().is_some();
                                        }
                                        PropKey::Len => unreachable!(),
                                        PropKey::All => {
                                            // TODO(longbin) Do we need to look into the properties?
                                            result = graph_element.get_all_properties().is_some()
                                        }
                                        PropKey::Key(key) => {
                                            result = graph_element.get_property(key).is_some()
                                        }
                                    }
                                } else {
                                    result = false
                                }
                            }
                        } else {
                            result = true;
                        }
                    }
                }

                Ok(result)
            }
            Operand::Vars(vars) | Operand::VarMap(vars) => {
                for var in vars {
                    if !var.eval_bool(_context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Operand::Map(key_vals) => {
                for key_val in key_vals {
                    if !key_val.1.eval_bool(_context)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }
    }
}

impl EvalPred for UnaryPredicate {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        use common_pb::Logical;
        match self.cmp {
            Logical::Isnull => {
                let left = match self.operand.eval(context) {
                    Ok(left) => Ok(left),
                    Err(err) => match err {
                        ExprEvalError::GetNoneFromContext => Ok(Object::None),
                        _ => Err(err),
                    },
                };
                Ok(apply_logical(&self.cmp, left?.as_borrow_object(), None)?
                    .as_bool()
                    .unwrap_or(false))
            }
            _ => Err(ExprEvalError::OtherErr(format!(
                "invalid logical operator: {:?} in a unary predicate",
                self.cmp
            ))),
        }
    }
}

impl EvalPred for Predicate {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        use common_pb::Logical;
        match self.cmp {
            Logical::Eq
            | Logical::Ne
            | Logical::Lt
            | Logical::Le
            | Logical::Gt
            | Logical::Ge
            | Logical::Within
            | Logical::Without
            | Logical::Startswith
            | Logical::Endswith
            | Logical::Regex => Ok(apply_logical(
                &self.cmp,
                self.left.eval(context)?.as_borrow_object(),
                Some(self.right.eval(context)?.as_borrow_object()),
            )?
            .as_bool()
            .unwrap_or(false)),
            _ => Err(ExprEvalError::OtherErr(format!(
                "invalid logical operator: {:?} in a binary predicate",
                self.cmp
            ))),
        }
    }
}

impl EvalPred for Predicates {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        match self {
            Predicates::Init => Ok(false),
            Predicates::SingleItem(item) => item.eval_bool(context),
            Predicates::Unary(upred) => upred.eval_bool(context),
            Predicates::Binary(pred) => pred.eval_bool(context),
            Predicates::Not(pred) => Ok(!pred.eval_bool(context)?),
            Predicates::And((pred1, pred2)) => Ok(pred1.eval_bool(context)? && pred2.eval_bool(context)?),
            Predicates::Or((pred1, pred2)) => Ok(pred1.eval_bool(context)? || pred2.eval_bool(context)?),
        }
    }
}

impl Predicates {
    pub fn not(self) -> Self {
        Predicates::Not(Box::new(self))
    }

    pub fn and(self, other: Predicates) -> Self {
        match (&self, &other) {
            (Predicates::Init, _) => other,
            (_, Predicates::Init) => self,
            _ => Predicates::And((Box::new(self), Box::new(other))),
        }
    }

    pub fn or(self, other: Predicates) -> Self {
        match (&self, &other) {
            (Predicates::Init, _) => other,
            (_, Predicates::Init) => self,
            _ => Predicates::Or((Box::new(self), Box::new(other))),
        }
    }

    fn merge_partial(
        self, curr_cmp: Option<common_pb::Logical>, partial: Partial, is_not: bool,
    ) -> ExprResult<Predicates> {
        use common_pb::Logical;
        let mut new_pred = Predicates::try_from(partial)?;
        if is_not {
            new_pred = new_pred.not()
        };
        if let Some(cmp) = curr_cmp {
            match cmp {
                Logical::And => Ok(self.and(new_pred)),
                Logical::Or => Ok(self.or(new_pred)),
                _ => unreachable!(),
            }
        } else {
            Ok(new_pred)
        }
    }
}

#[allow(dead_code)]
fn process_predicates(
    iter: &mut dyn Iterator<Item = &common_pb::ExprOpr>,
) -> ExprResult<Option<Predicates>> {
    use common_pb::expr_opr::Item;
    use common_pb::Logical;
    let mut partial = Partial::default();
    let mut predicates = Predicates::default();
    let mut curr_cmp: Option<common_pb::Logical> = None;
    let mut is_not = false;
    let mut left_brace_count = 0;
    let mut container: Vec<common_pb::ExprOpr> = Vec::new();

    while let Some(opr) = iter.next() {
        if let Some(item) = &opr.item {
            match item {
                Item::Logical(l) => {
                    if left_brace_count == 0 {
                        let logical = unsafe { std::mem::transmute::<i32, common_pb::Logical>(*l) };
                        match logical {
                            Logical::Eq
                            | Logical::Ne
                            | Logical::Lt
                            | Logical::Le
                            | Logical::Gt
                            | Logical::Ge
                            | Logical::Within
                            | Logical::Without
                            | Logical::Startswith
                            | Logical::Endswith
                            | Logical::Regex
                            | Logical::Isnull => partial.cmp(logical)?,
                            Logical::Not => is_not = true,
                            Logical::And | Logical::Or => {
                                predicates = predicates.merge_partial(curr_cmp, partial, is_not)?;
                                if is_not {
                                    is_not = false;
                                }
                                partial = Partial::default();
                                curr_cmp = Some(logical);
                            }
                        }
                    } else {
                        container.push(opr.clone());
                    }
                }
                Item::Const(_) | Item::Var(_) | Item::Vars(_) | Item::VarMap(_) | Item::Map(_) => {
                    if left_brace_count == 0 {
                        if partial.get_left().is_none() {
                            partial.left(opr.clone().try_into()?)?;
                        } else {
                            partial.right(opr.clone().try_into()?)?;
                        }
                    } else {
                        container.push(opr.clone());
                    }
                }
                Item::Brace(brace) => {
                    if *brace == 0 {
                        if left_brace_count != 0 {
                            container.push(opr.clone());
                        }
                        left_brace_count += 1;
                    } else {
                        left_brace_count -= 1;
                        if left_brace_count == 0 {
                            let mut iter = container.iter();
                            if let Some(p) = process_predicates(&mut iter)? {
                                partial.predicates(p)?;
                                container.clear();
                            } else {
                                return Ok(None);
                            }
                        } else {
                            container.push(opr.clone());
                        }
                    }
                }
                Item::Arith(_) => return Ok(None),
                Item::Param(param) => {
                    return Err(ExprError::unsupported(format!("Dynamic Param {:?}", param)))
                }
                Item::Case(case) => return Err(ExprError::unsupported(format!("Case When {:?}", case))),
                Item::Extract(extract) => {
                    return Err(ExprError::unsupported(format!("Extract {:?}", extract)))
                }
            }
        }
    }

    if left_brace_count == 0 {
        Ok(Some(predicates.merge_partial(curr_cmp, partial, is_not)?))
    } else {
        Err(ExprError::UnmatchedLRBraces)
    }
}

#[derive(Debug)]
pub enum PEvaluator {
    Predicates(Predicates),
    General(Evaluator),
}

impl EvalPred for PEvaluator {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        let result = match self {
            PEvaluator::Predicates(pred) => pred.eval_bool(context),
            PEvaluator::General(eval) => eval.eval_bool(context),
        };
        match result {
            Ok(b) => Ok(b),
            Err(err) => match err {
                ExprEvalError::GetNoneFromContext => Ok(false),
                _ => Err(err),
            },
        }
    }
}

impl TryFrom<common_pb::Expression> for PEvaluator {
    type Error = ParsePbError;

    fn try_from(expr: common_pb::Expression) -> Result<Self, Self::Error> {
        let mut iter = expr.operators.iter();
        if let Some(pred) =
            process_predicates(&mut iter).map_err(|err| ParsePbError::ParseError(format!("{:?}", err)))?
        {
            Ok(Self::Predicates(pred))
        } else {
            Ok(Self::General(expr.try_into()?))
        }
    }
}

impl TryFrom<pb::IndexPredicate> for PEvaluator {
    type Error = ParsePbError;

    fn try_from(index_pred: pb::IndexPredicate) -> Result<Self, Self::Error> {
        Ok(Self::Predicates(index_pred.try_into()?))
    }
}

#[cfg(test)]
mod tests {
    use ahash::HashMap;
    use dyn_type::object;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::NameOrId;

    use super::*;
    use crate::apis::{DynDetails, Vertex};
    use crate::utils::expr::eval::NoneContext;

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
            (NameOrId::from("str_birthday".to_string()), "1990-04-16".to_string().into()),
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
    fn test_parse_predicate() {
        let expr = str_to_expr_pb("1".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(pred.clone(), Predicates::SingleItem(Operand::Const(object!(1))));
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("@a.name".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::SingleItem(Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    })
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(!p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("!@a.name".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::SingleItem(Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    })
                    .not()
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("1 > 2".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::Binary(Predicate {
                        left: Operand::Const(object!(1)),
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    })
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(!p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("!(1 > 2)".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::Binary(Predicate {
                        left: Operand::Const(object!(1)),
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    })
                    .not()
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("!(!(!(!(1 > 2))))".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::Binary(Predicate {
                        left: Operand::Const(object!(1)),
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    })
                    .not()
                    .not()
                    .not()
                    .not()
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(!p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("1 && 1 > 2".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::SingleItem(Operand::Const(object!(1))).and(Predicates::Binary(Predicate {
                        left: Operand::Const(object!(1)),
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    }))
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
        assert!(!p_eval
            .eval_bool::<(), NoneContext>(None)
            .unwrap());

        let expr = str_to_expr_pb("!@a.name && @a.age > 2 || @b.~id == 10".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::not(Predicates::SingleItem(Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    }))
                    .and(Predicates::Binary(Predicate {
                        left: Operand::Var {
                            tag: Some("a".into()),
                            prop_key: Some(PropKey::Key("age".into()))
                        },
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    }))
                    .or(Predicates::Binary(Predicate {
                        left: Operand::Var { tag: Some("b".into()), prop_key: Some(PropKey::Id) },
                        cmp: common_pb::Logical::Eq,
                        right: Operand::Const(object!(10)),
                    }))
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }

        let expr = str_to_expr_pb(
            "(@a.name == \"John\" && @a.age > 27) || (@b.~label == 11 && @b.name == \"Alien\")".to_string(),
        )
        .unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => assert_eq!(
                pred.clone(),
                Predicates::Binary(Predicate {
                    left: Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    },
                    cmp: common_pb::Logical::Eq,
                    right: Operand::Const(object!("John")),
                })
                .and(Predicates::Binary(Predicate {
                    left: Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("age".into()))
                    },
                    cmp: common_pb::Logical::Gt,
                    right: Operand::Const(27_i64.into()),
                }))
                .or(Predicates::Binary(Predicate {
                    left: Operand::Var { tag: Some("b".into()), prop_key: Some(PropKey::Label) },
                    cmp: common_pb::Logical::Eq,
                    right: Operand::Const(11_i64.into()),
                })
                .and(Predicates::Binary(Predicate {
                    left: Operand::Var {
                        tag: Some("b".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    },
                    cmp: common_pb::Logical::Eq,
                    right: Operand::Const(object!("Alien")),
                })))
            ),
            PEvaluator::General(_) => panic!("should be predicate"),
        }

        let expr = str_to_expr_pb(
            "@a.name == \"John\" && ((@a.age > 27 || !(@b.~label)) && @b.name == \"Alien\")".to_string(),
        )
        .unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => assert_eq!(
                pred.clone(),
                Predicates::Binary(Predicate {
                    left: Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    },
                    cmp: common_pb::Logical::Eq,
                    right: Operand::Const(object!("John")),
                })
                .and(
                    Predicates::Binary(Predicate {
                        left: Operand::Var {
                            tag: Some("a".into()),
                            prop_key: Some(PropKey::Key("age".into()))
                        },
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(27_i64.into()),
                    })
                    .or(Predicates::not(Predicates::SingleItem(Operand::Var {
                        tag: Some("b".into()),
                        prop_key: Some(PropKey::Label)
                    })))
                    .and(Predicates::Binary(Predicate {
                        left: Operand::Var {
                            tag: Some("b".into()),
                            prop_key: Some(PropKey::Key("name".into()))
                        },
                        cmp: common_pb::Logical::Eq,
                        right: Operand::Const(object!("Alien")),
                    }))
                )
            ),
            PEvaluator::General(_) => panic!("should be predicate"),
        }

        let expr = str_to_expr_pb("isnull @a.name".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::Unary(UnaryPredicate {
                        operand: Operand::Var {
                            tag: Some("a".into()),
                            prop_key: Some(PropKey::Key("name".into()))
                        },
                        cmp: common_pb::Logical::Isnull,
                    })
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }

        let expr = str_to_expr_pb("isnull @a.name && @a.age > 2 || isnull @b.age".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Predicates(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::Unary(UnaryPredicate {
                        operand: Operand::Var {
                            tag: Some("a".into()),
                            prop_key: Some(PropKey::Key("name".into()))
                        },
                        cmp: common_pb::Logical::Isnull,
                    })
                    .and(Predicates::Binary(Predicate {
                        left: Operand::Var {
                            tag: Some("a".into()),
                            prop_key: Some(PropKey::Key("age".into()))
                        },
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    }))
                    .or(Predicates::Unary(UnaryPredicate {
                        operand: Operand::Var {
                            tag: Some("b".into()),
                            prop_key: Some(PropKey::Key("age".into()))
                        },
                        cmp: common_pb::Logical::Isnull,
                    }))
                );
            }
            PEvaluator::General(_) => panic!("should be predicate"),
        }
    }

    #[test]
    fn test_eval_predicate() {
        let context = prepare_context();
        let expr = str_to_expr_pb("@0.~id == 1".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        let expr = str_to_expr_pb("@0.~id == 1 && @1.~label != 13".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        let expr = str_to_expr_pb("@0.name == \"Tom\" || @1.age > 27".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(!p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        let expr = str_to_expr_pb("@0.name StartsWith \"Jo\"".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        let expr = str_to_expr_pb("@0.name EndsWith \"hn\"".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        let expr = str_to_expr_pb("\"John\" EndsWith \"hn\"".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        let expr = str_to_expr_pb(
            "@0.name == \"John\" && @0.age > 27 || @1.~label == 11 && @1.name == \"Alien\"".to_string(),
        )
        .unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(!p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        // This will degenerate to general evaluator due to the brackets
        // thus, it can be evaluated to true
        let expr = str_to_expr_pb(
            "(@0.name == \"John\" && @0.age > 27) || (@1.~label == 11 && @1.name == \"Alien\")".to_string(),
        )
        .unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        // 2 does not present
        let expr = str_to_expr_pb("@2.age == 13".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(!p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());

        // 1.none_exist does not present
        let expr = str_to_expr_pb("@1.none_exist > 20".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        assert!(!p_eval
            .eval_bool::<_, Vertices>(Some(&context))
            .unwrap());
    }

    #[test]
    fn test_eval_predicates_is_null() {
        // [v0: id = 1, label = 9, age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]
        // [v1: id = 2, label = 11, age = 26, name = Jimmy, birthday = 19950816]
        let ctxt = prepare_context();
        let cases: Vec<&str> = vec![
            "isNull @0.hobbies",                 // false
            "!(isNull @0.hobbies)",              // true
            "isNull @1.hobbies",                 // true
            "!(isNull @1.hobbies)",              // false
            "isNull true",                       // false
            "isNull false",                      // false
            "isNull @1.hobbies && @1.age == 26", // true
        ];
        let expected: Vec<bool> = vec![false, true, true, false, false, false, true];

        for (case, expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = PEvaluator::try_from(str_to_expr_pb(case.to_string()).unwrap()).unwrap();
            assert_eq!(
                eval.eval_bool::<_, Vertices>(Some(&ctxt))
                    .unwrap(),
                expected
            );
        }
    }

    fn gen_regex_expression(to_match: &str, pattern: &str) -> common_pb::Expression {
        let mut regex_expr = str_to_expr_pb(to_match.to_string()).unwrap();
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
    fn test_eval_predicates_regex() {
        // [v0: id = 1, label = 9, age = 31, name = John, birthday = 19900416, hobbies = [football, guitar]]
        // [v1: id = 2, label = 11, age = 26, name = Jimmy, birthday = 19950816]
        let ctxt = prepare_context();

        // TODO: the parser does not support escape characters in regex well yet.
        // So use gen_regex_expression() to help generate expression
        let cases: Vec<(&str, &str)> = vec![
            ("@0.name", r"^J"),                          // startWith, true
            ("@0.name", r"J.*"),                         // true
            ("@0.name", r"n$"),                          // endWith, true
            ("@0.name", r".*n"),                         // true
            ("@0.name", r"oh"),                          // true
            ("@0.name", r"A.*"),                         // false
            ("@0.name", r".*A"),                         // false
            ("@0.name", r"ab"),                          // false
            ("@0.name", r"John.+"),                      // false
            ("@0.str_birthday", r"^\d{4}-\d{2}-\d{2}$"), // true
        ];
        let expected: Vec<bool> = vec![true, true, true, true, true, false, false, false, false, true];

        for ((to_match, pattern), expected) in cases.into_iter().zip(expected.into_iter()) {
            let eval = PEvaluator::try_from(gen_regex_expression(to_match, pattern)).unwrap();
            assert_eq!(
                eval.eval_bool::<_, Vertices>(Some(&ctxt))
                    .unwrap(),
                expected
            );
        }
    }
}
