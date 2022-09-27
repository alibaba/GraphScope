use std::convert::{TryFrom, TryInto};

use global_query::store_api::condition::Operand as StoreOperand;
use global_query::store_api::{
    condition::predicate::CmpOperator as StoreOprator,
    condition::predicate::PredCondition as StorePredCondition,
};
use global_query::store_api::{prelude::Property, Condition, ConditionBuilder, PropId};
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;

use crate::apis::PropKey;
use crate::utils::expr::eval::Operand;
use crate::utils::expr::eval_pred::{PEvaluator, Predicate, Predicates};
use crate::{GraphProxyError, GraphProxyResult};

pub(crate) fn zip_option_vecs<T>(left: Option<Vec<T>>, right: Option<Vec<T>>) -> Option<Vec<T>> {
    match (left, right) {
        (Some(mut left), Some(mut right)) => {
            left.append(&mut right);
            Some(left)
        }
        (None, Some(right)) => Some(right),
        (Some(left), None) => Some(left),
        (None, None) => None,
    }
}

impl Operand {
    /// only get the PropId, else None
    pub(crate) fn get_var_prop_id(&self) -> GraphProxyResult<PropId> {
        match self {
            Operand::Var { tag: None, prop_key: Some(prop_key) } => match prop_key {
                PropKey::Key(NameOrId::Id(id)) => Ok(*id as PropId),
                _ => Err(GraphProxyError::UnSupported(format!("var error {:?}", self))),
            },
            _ => Err(GraphProxyError::FilterPushDownError(format!("not a var {:?}", self))),
        }
    }

    pub(crate) fn to_store_oprand(&self) -> GraphProxyResult<StoreOperand> {
        match self {
            Operand::Var { tag: None, prop_key: Some(prop_key) } => match prop_key {
                PropKey::Key(NameOrId::Id(id)) => Ok(StoreOperand::PropId(*id as PropId)),
                PropKey::Label => Ok(StoreOperand::Label),
                PropKey::Id => Ok(StoreOperand::Id),
                _ => Err(GraphProxyError::FilterPushDownError(format!("var error {:?}", self))),
            },
            Operand::Const(obj) => {
                let prop = Property::from_borrow_object(obj.as_borrow())
                    .map_err(|e| GraphProxyError::FilterPushDownError(format!("{:?}", e)));
                prop.map(StoreOperand::Const)
            }
            _ => Err(GraphProxyError::FilterPushDownError(format!("not a var {:?}", self))),
        }
    }
}

impl Predicate {
    fn extract_prop_ids(&self) -> Option<Vec<PropId>> {
        let left = self.left.get_var_prop_id();
        let right = self.right.get_var_prop_id();
        match (left, right) {
            (Ok(left), Ok(right)) => Some(vec![left, right]),
            (Ok(left), _) => Some(vec![left]),
            (_, Ok(right)) => Some(vec![right]),
            _ => None,
        }
    }
}

impl Predicates {
    pub(crate) fn extract_prop_ids(&self) -> Option<Vec<PropId>> {
        match self {
            Predicates::Init => None,
            Predicates::SingleItem(operand) => operand
                .get_var_prop_id()
                .map(|id| Some(vec![id]))
                .unwrap_or(None),
            Predicates::Predicate(pred) => pred.extract_prop_ids(),
            Predicates::Not(pred) => pred.extract_prop_ids(),
            Predicates::And((left, right)) => {
                let left = left.extract_prop_ids();
                let right = right.extract_prop_ids();
                zip_option_vecs(left, right)
            }
            Predicates::Or((left, right)) => {
                let left = left.extract_prop_ids();
                let right = right.extract_prop_ids();
                zip_option_vecs(left, right)
            }
        }
    }
}

impl PEvaluator {
    pub(crate) fn extract_prop_ids(&self) -> Option<Vec<PropId>> {
        match self {
            PEvaluator::Predicates(preds) => preds.extract_prop_ids(),
            _ => None,
        }
    }
}

impl TryFrom<&Predicate> for StorePredCondition {
    type Error = GraphProxyError;

    fn try_from(pred: &Predicate) -> GraphProxyResult<StorePredCondition> {
        let (left, right) = (pred.left.to_store_oprand()?, pred.right.to_store_oprand()?);
        let pred = match pred.cmp {
            common_pb::Logical::Eq => StorePredCondition::new_predicate(left, StoreOprator::Equal, right),
            common_pb::Logical::Ne => {
                StorePredCondition::new_predicate(left, StoreOprator::NotEqual, right)
            }
            common_pb::Logical::Lt => {
                StorePredCondition::new_predicate(left, StoreOprator::LessThan, right)
            }
            common_pb::Logical::Le => {
                StorePredCondition::new_predicate(left, StoreOprator::LessEqual, right)
            }
            common_pb::Logical::Gt => {
                StorePredCondition::new_predicate(left, StoreOprator::GreaterThan, right)
            }
            common_pb::Logical::Ge => {
                StorePredCondition::new_predicate(left, StoreOprator::GreaterEqual, right)
            }
            common_pb::Logical::Within => {
                StorePredCondition::new_predicate(left, StoreOprator::WithIn, right)
            }
            common_pb::Logical::Without => {
                StorePredCondition::new_predicate(left, StoreOprator::WithOut, right)
            }
            common_pb::Logical::Startswith => {
                StorePredCondition::new_predicate(left, StoreOprator::StartWith, right)
            }
            common_pb::Logical::Endswith => {
                StorePredCondition::new_predicate(left, StoreOprator::EndWith, right)
            }
            _ => {
                return Err(GraphProxyError::FilterPushDownError(format!(
                    "op {:?} shouldn't appear",
                    pred.cmp
                )))
            }
        };
        Ok(pred)
    }
}

impl TryFrom<&Predicates> for Option<Condition> {
    type Error = GraphProxyError;

    fn try_from(preds: &Predicates) -> Result<Self, Self::Error> {
        let mut builder = ConditionBuilder::new();
        match preds {
            Predicates::Init => Ok(None),
            Predicates::SingleItem(op) => {
                let key = op.get_var_prop_id()?;
                let pred = StorePredCondition::new_has_prop(key);
                builder.and(Condition::new(pred));
                Ok(builder.build())
            }
            Predicates::Predicate(pred) => {
                let pred: StorePredCondition = pred.try_into()?;
                builder.and(Condition::new(pred));
                Ok(builder.build())
            }
            Predicates::Not(pred) => {
                let cond: Option<Condition> = pred.as_ref().try_into()?;
                if let Some(cond) = cond {
                    builder.and(cond);
                    builder.not();
                    Ok(builder.build())
                } else {
                    Ok(None)
                }
            }
            Predicates::And((left, right)) => {
                let left_cond: Option<Condition> = left.as_ref().try_into()?;
                let right_cond: Option<Condition> = right.as_ref().try_into()?;
                match (left_cond, right_cond) {
                    (Some(left_cond), Some(right_cond)) => {
                        builder.and(left_cond);
                        builder.and(right_cond);
                        Ok(builder.build())
                    }
                    _ => Ok(None),
                }
            }
            Predicates::Or((left, right)) => {
                let left_cond: Option<Condition> = left.as_ref().try_into()?;
                let right_cond: Option<Condition> = right.as_ref().try_into()?;
                match (left_cond, right_cond) {
                    (Some(left_cond), Some(right_cond)) => {
                        builder.and(left_cond);
                        builder.or(right_cond);
                        Ok(builder.build())
                    }
                    _ => Ok(None),
                }
            }
        }
    }
}

impl TryFrom<&PEvaluator> for Option<Condition> {
    type Error = GraphProxyError;

    fn try_from(pe: &PEvaluator) -> Result<Self, Self::Error> {
        let cond = match pe {
            PEvaluator::General(eval) => Err(GraphProxyError::FilterPushDownError(format!(
                "don't support General(Evaluator) {:?}",
                eval
            ))),
            PEvaluator::Predicates(preds) => preds.try_into(),
        };
        cond
    }
}

#[cfg(test)]
mod test {
    use dyn_type::{Object, Primitives};
    use global_query::store_api::prelude::{Operand as StoreOperand, Property as StoreProperty};
    use ir_common::NameOrId;

    use super::*;
    use crate::apis::PropKey;
    use crate::utils::expr::eval::Operand;

    #[test]
    fn test_empty_predicates_to_condition() {
        let pred = &Predicates::Init;
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        assert_eq!(cond.unwrap(), None);
    }

    #[test]
    fn test_singleitem_predicates_to_condition() {
        let oprand = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let pred = &Predicates::SingleItem(oprand);
        let target = ConditionBuilder::new()
            .and(Condition::Pred(StorePredCondition::new_has_prop(1)))
            .build();
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        let cond = cond.unwrap();
        assert_eq!(cond, target);
    }

    #[test]
    fn test_single_op_predicates_to_condition() {
        let left = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let right = Operand::Const(Object::Primitive(Primitives::Integer(10)));
        let cmp = common_pb::Logical::Eq;

        let pred = &Predicates::Predicate(Predicate { left, cmp, right });

        let target = ConditionBuilder::new()
            .and(Condition::Pred(StorePredCondition::new_predicate(
                StoreOperand::PropId(1),
                StoreOprator::Equal,
                StoreOperand::Const(StoreProperty::Int(10)),
            )))
            .build();
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        let cond = cond.unwrap();
        assert_eq!(cond, target);

        let left = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let right = Operand::Const(Object::String("hello world".to_owned()));
        let cmp = common_pb::Logical::Startswith;

        let pred = &Predicates::Predicate(Predicate { left, cmp, right });

        let target = ConditionBuilder::new()
            .and(Condition::Pred(StorePredCondition::new_predicate(
                StoreOperand::PropId(1),
                StoreOprator::StartWith,
                StoreOperand::Const(StoreProperty::String("hello world".to_owned())),
            )))
            .build();
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        let cond = cond.unwrap();
        assert_eq!(cond, target);
    }
    #[test]
    fn test_not_predicates_to_condition() {
        let oprand = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let pred = &Predicates::Not(Box::new(Predicates::SingleItem(oprand)));
        let target = ConditionBuilder::new()
            .and(Condition::Pred(StorePredCondition::new_has_prop(1)))
            .not()
            .build();
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        let cond = cond.unwrap();
        assert_eq!(cond, target);
    }

    #[test]
    fn test_and_predicates_to_condition() {
        let left = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let right = Operand::Const(Object::Primitive(Primitives::Integer(10)));
        let cmp = common_pb::Logical::Ge;

        let pred_left = Predicates::Predicate(Predicate { left, cmp, right });

        let left = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let right = Operand::Const(Object::Primitive(Primitives::Integer(20)));
        let cmp = common_pb::Logical::Le;

        let pred_right = Predicates::Predicate(Predicate { left, cmp, right });

        let pred = &Predicates::And((Box::new(pred_left), Box::new(pred_right)));

        let target = ConditionBuilder::new()
            .and(Condition::Pred(StorePredCondition::new_predicate(
                StoreOperand::PropId(1),
                StoreOprator::GreaterEqual,
                StoreOperand::Const(StoreProperty::Int(10)),
            )))
            .and(Condition::Pred(StorePredCondition::new_predicate(
                StoreOperand::PropId(1),
                StoreOprator::LessEqual,
                StoreOperand::Const(StoreProperty::Int(20)),
            )))
            .build();
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        let cond = cond.unwrap();
        assert_eq!(cond, target);
    }

    #[test]
    fn test_or_predicates_to_condition() {
        let left = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let right = Operand::Const(Object::Primitive(Primitives::Integer(10)));
        let cmp = common_pb::Logical::Ge;

        let pred_left = Predicates::Predicate(Predicate { left, cmp, right });

        let left = Operand::Var { tag: None, prop_key: Some(PropKey::Key(NameOrId::Id(1))) };
        let right = Operand::Const(Object::Primitive(Primitives::Integer(20)));
        let cmp = common_pb::Logical::Le;

        let pred_right = Predicates::Predicate(Predicate { left, cmp, right });

        let pred = &Predicates::Or((Box::new(pred_left), Box::new(pred_right)));

        let target = ConditionBuilder::new()
            .and(Condition::Pred(StorePredCondition::new_predicate(
                StoreOperand::PropId(1),
                StoreOprator::GreaterEqual,
                StoreOperand::Const(StoreProperty::Int(10)),
            )))
            .or(Condition::Pred(StorePredCondition::new_predicate(
                StoreOperand::PropId(1),
                StoreOprator::LessEqual,
                StoreOperand::Const(StoreProperty::Int(20)),
            )))
            .build();
        let cond: Result<Option<Condition>, GraphProxyError> = pred.try_into();
        assert!(cond.is_ok());
        let cond = cond.unwrap();
        assert_eq!(cond, target);
    }
}
