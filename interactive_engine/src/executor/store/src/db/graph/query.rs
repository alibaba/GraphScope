use crate::db::api::*;
use super::codec::*;

pub fn check_condition(decoder: &Decoder, data: &[u8], condition: &Condition) -> bool {
    match *condition {
        Condition::Predicate(ref c) => single_predicate_check(c, decoder, data),
        Condition::And(ref c) => {
            for cond in &c.sub_conditions {
                if !check_condition(decoder, data, cond.as_ref()) {
                    return false;
                }
            }
            true
        }
        Condition::Or(ref c) => {
            for cond in &c.sub_conditions {
                if check_condition(decoder, data, cond.as_ref()) {
                    return true;
                }
            }
            false
        }
        Condition::Not(ref c) => {
            !check_condition(decoder, data, c.sub_condition.as_ref())
        }
    }
}

fn single_predicate_check(condition: &PredicateCondition, decoder: &Decoder, data: &[u8]) -> bool {
    if let Some(v) = decoder.decode_property(data, condition.prop) {
        return match condition.predicate {
            ComparisonOp::LessThan => v < condition.value.as_ref(),
            ComparisonOp::LessEqual => v <= condition.value.as_ref(),
            ComparisonOp::Equal => v == condition.value.as_ref(),
            ComparisonOp::GreaterThan => v > condition.value.as_ref(),
            ComparisonOp::GreaterEqual => v >= condition.value.as_ref(),
            ComparisonOp::NotEqual => v != condition.value.as_ref(),
        };
    }
    false
}