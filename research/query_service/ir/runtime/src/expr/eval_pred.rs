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
use ir_common::error::{ParsePbError, ParsePbResult};
use ir_common::generated::common as common_pb;

use crate::expr::eval::{apply_logical, Context, Evaluate, Evaluator, Operand};
use crate::expr::{ExprEvalError, ExprEvalResult};
use crate::graph::element::Element;
use crate::graph::property::{Details, PropKey};

/// The trait to define evaluating a predicate, which return `bool` value.
pub trait EvalPred {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    left: Operand,
    cmp: common_pb::Logical,
    right: Operand,
}

#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
struct PartialPredicate {
    is_not: bool,
    left: Option<Operand>,
    cmp: Option<common_pb::Logical>,
    right: Option<Operand>,
}

#[allow(dead_code)]
impl PartialPredicate {
    pub fn left(&mut self, item: Operand) {
        self.left = Some(item);
    }

    pub fn cmp(&mut self, cmp: common_pb::Logical) -> ParsePbResult<()> {
        if self.left.is_none() || self.cmp.is_some() {
            Err(ParsePbError::ParseError(format!("invalid predicate: {:?}, {:?}", self, cmp)))
        } else {
            self.cmp = Some(cmp);
            Ok(())
        }
    }

    pub fn right(&mut self, item: Operand) -> ParsePbResult<()> {
        if self.cmp.is_none() || self.right.is_some() {
            Err(ParsePbError::ParseError(format!("invalid predicate: {:?}, {:?}", self, item)))
        } else {
            self.right = Some(item);
            Ok(())
        }
    }

    pub fn not(&mut self) -> ParsePbResult<()> {
        if self.left.is_some() {
            Err(ParsePbError::ParseError(format!("invalid predicate: NOT({:?})", self)))
        } else {
            self.is_not = true;
            Ok(())
        }
    }
}

impl From<PartialPredicate> for Option<Predicates> {
    fn from(partial: PartialPredicate) -> Option<Predicates> {
        let pred = if partial.left.is_none() {
            None
        } else if partial.cmp.is_none() {
            Some(Predicates::SingleItem(partial.left.unwrap()))
        } else if partial.right.is_some() {
            Some(Predicates::Pred(Predicate {
                left: partial.left.unwrap(),
                cmp: partial.cmp.unwrap(),
                right: partial.right.unwrap(),
            }))
        } else {
            None
        };
        if partial.is_not {
            pred.map(|pred| Predicates::Not(Box::new(pred)))
        } else {
            pred
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicates {
    SingleItem(Operand),
    Pred(Predicate),
    Not(Box<Predicates>),
    And((Box<Predicates>, Box<Predicates>)),
    Or((Box<Predicates>, Box<Predicates>)),
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

impl EvalPred for Operand {
    fn eval_bool<E: Element, C: Context<E>>(&self, context_: Option<&C>) -> ExprEvalResult<bool> {
        match self {
            Operand::Const(c) => c.as_borrow().eval_bool(context_),
            Operand::Var { tag, prop_key } => {
                let mut result = false;
                if let Some(context) = context_ {
                    if let Some(elem) = context.get(tag.as_ref()) {
                        if let Some(key) = prop_key {
                            match key {
                                PropKey::Id => {
                                    result = elem
                                        .as_graph_element()
                                        .map(|g| g.id())
                                        .is_some();
                                }
                                PropKey::Label => {
                                    result = elem
                                        .as_graph_element()
                                        .and_then(|g| g.label())
                                        .is_some();
                                }
                                PropKey::Len => {
                                    result = elem
                                        .as_graph_element()
                                        .map(|g| g.len() > 0)
                                        .unwrap_or(false);
                                }
                                PropKey::Key(key) => {
                                    if let Some(details) = elem.details() {
                                        result = details.get_property(key).is_some();
                                    }
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
                    if !var.eval_bool(context_)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
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
            | Logical::Without => Ok(apply_logical(
                &self.cmp,
                self.left.eval(context)?.as_borrow_object(),
                Some(self.right.eval(context)?.as_borrow_object()),
            )?
            .as_bool()
            .unwrap_or(false)),
            _ => Err(ExprEvalError::OtherErr(format!(
                "invalid logical operator: {:?} in a predicate",
                self.cmp
            ))),
        }
    }
}

impl EvalPred for Predicates {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        match self {
            Predicates::SingleItem(item) => item.eval_bool(context),
            Predicates::Pred(pred) => pred.eval_bool(context),
            Predicates::Not(pred) => Ok(!pred.eval_bool(context)?),
            Predicates::And((pred1, pred2)) => Ok(pred1.eval_bool(context)? && pred2.eval_bool(context)?),
            Predicates::Or((pred1, pred2)) => Ok(pred1.eval_bool(context)? || pred2.eval_bool(context)?),
        }
    }
}

impl Predicates {
    pub fn not(pred: Predicates) -> Self {
        Predicates::Not(Box::new(pred))
    }

    pub fn and(self, other: Predicates) -> Self {
        Predicates::And((Box::new(self), Box::new(other)))
    }

    pub fn or(self, other: Predicates) -> Self {
        Predicates::Or((Box::new(self), Box::new(other)))
    }
}

#[allow(dead_code)]
fn merge_predicates(
    partial: PartialPredicate, curr_cmp: Option<common_pb::Logical>, mut predicates: Option<Predicates>,
) -> ParsePbResult<Predicates> {
    use common_pb::Logical;

    let old_partial = partial.clone();
    let new_pred = Option::<Predicates>::from(partial);
    if new_pred.is_none() {
        return Err(ParsePbError::ParseError(format!("invalid predicate: {:?}", old_partial)));
    }
    if let Some(cmp) = curr_cmp {
        if let Some(pred) = &predicates {
            match cmp {
                Logical::And => predicates = Some(pred.clone().and(new_pred.unwrap())),
                Logical::Or => predicates = Some(pred.clone().or(new_pred.unwrap())),
                _ => unreachable!(),
            }
        }
    } else {
        predicates = new_pred;
    }

    Ok(predicates.unwrap())
}

#[allow(dead_code)]
fn process_predicates(iter: &mut dyn Iterator<Item = &common_pb::ExprOpr>) -> ParsePbResult<Predicates> {
    use common_pb::expr_opr::Item;
    use common_pb::Logical;
    let mut partial = PartialPredicate::default();
    let mut predicates: Option<Predicates> = None;
    let mut curr_cmp: Option<common_pb::Logical> = None;

    while let Some(opr) = iter.next() {
        if let Some(item) = &opr.item {
            match item {
                Item::Logical(l) => {
                    let logical = unsafe { std::mem::transmute::<i32, common_pb::Logical>(*l) };
                    match logical {
                        Logical::Eq
                        | Logical::Ne
                        | Logical::Lt
                        | Logical::Le
                        | Logical::Gt
                        | Logical::Ge
                        | Logical::Within
                        | Logical::Without => partial.cmp(logical)?,
                        Logical::Not => partial.not()?,
                        Logical::And | Logical::Or => {
                            predicates = Some(merge_predicates(partial, curr_cmp, predicates)?);
                            partial = PartialPredicate::default();
                            curr_cmp = Some(logical);
                        }
                    }
                }
                Item::Const(_) | Item::Var(_) | Item::Vars(_) | Item::VarMap(_) => {
                    if partial.left.is_none() {
                        partial.left(opr.clone().try_into()?);
                    } else {
                        partial.right(opr.clone().try_into()?)?;
                    }
                }
                Item::Arith(_) => {
                    return Err(ParsePbError::NotSupported(
                        "arithmetic is not supported in predicates".to_string(),
                    ))
                }
                Item::Brace(_) => {
                    return Err(ParsePbError::NotSupported(
                        "brace is not supported in predicates".to_string(),
                    ))
                }
            }
        }
    }

    merge_predicates(partial, curr_cmp, predicates)
}

#[derive(Debug)]
pub enum PEvaluator {
    Pred(Predicates),
    General(Evaluator),
}

impl EvalPred for PEvaluator {
    fn eval_bool<E: Element, C: Context<E>>(&self, context: Option<&C>) -> ExprEvalResult<bool> {
        match self {
            PEvaluator::Pred(pred) => pred.eval_bool(context),
            PEvaluator::General(eval) => eval.eval_bool(context),
        }
    }
}

impl TryFrom<common_pb::Expression> for PEvaluator {
    type Error = ParsePbError;

    fn try_from(expr: common_pb::Expression) -> Result<Self, Self::Error> {
        let mut iter = expr.operators.iter();
        if let Ok(pred) = process_predicates(&mut iter) {
            Ok(Self::Pred(pred))
        } else {
            Ok(Self::General(expr.try_into()?))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use dyn_type::Object;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::NameOrId;

    use super::*;
    use crate::expr::eval::NoneContext;
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
                Vertex::new(1, Some(9.into()), DynDetails::new(DefaultDetails::new(map1))),
                Vertex::new(2, Some(11.into()), DynDetails::new(DefaultDetails::new(map2))),
            ],
        }
    }

    #[test]
    fn test_parse_predicate() {
        let expr = str_to_expr_pb("1".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Pred(pred) => {
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
            PEvaluator::Pred(pred) => {
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

        let expr = str_to_expr_pb("1 > 2".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Pred(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::Pred(Predicate {
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

        let expr = str_to_expr_pb("1 && 1 > 2".to_string()).unwrap();
        let p_eval = PEvaluator::try_from(expr).unwrap();
        match &p_eval {
            PEvaluator::Pred(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::SingleItem(Operand::Const(object!(1))).and(Predicates::Pred(Predicate {
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
            PEvaluator::Pred(pred) => {
                assert_eq!(
                    pred.clone(),
                    Predicates::not(Predicates::SingleItem(Operand::Var {
                        tag: Some("a".into()),
                        prop_key: Some(PropKey::Key("name".into()))
                    }))
                    .and(Predicates::Pred(Predicate {
                        left: Operand::Var {
                            tag: Some("a".into()),
                            prop_key: Some(PropKey::Key("age".into()))
                        },
                        cmp: common_pb::Logical::Gt,
                        right: Operand::Const(object!(2)),
                    }))
                    .or(Predicates::Pred(Predicate {
                        left: Operand::Var { tag: Some("b".into()), prop_key: Some(PropKey::Id) },
                        cmp: common_pb::Logical::Eq,
                        right: Operand::Const(object!(10)),
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
    }
}
