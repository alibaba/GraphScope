#![allow(dead_code)]
use super::PropId;
use super::property::Value;

pub enum Condition {
    And(AndCondition),
    Or(OrCondition),
    Not(NotCondition),
    Predicate(PredicateCondition),
}

pub struct AndCondition {
    pub sub_conditions: Vec<Box<Condition>>,
}

impl AndCondition {
    pub fn new(conditions: Vec<Condition>) -> Self {
        let sub_conditions = conditions.into_iter()
            .map(|c| Box::new(c)).collect();
        AndCondition {
            sub_conditions,
        }
    }
}

pub struct OrCondition {
    pub sub_conditions: Vec<Box<Condition>>,
}

impl OrCondition {
    pub fn new(condition: Vec<Condition>) -> Self {
        let sub_conditions = condition.into_iter()
            .map(|c| Box::new(c)).collect();
        OrCondition {
            sub_conditions,
        }
    }
}

pub struct NotCondition {
    pub sub_condition: Box<Condition>,
}

impl NotCondition {
    pub fn new(condition: Condition) -> Self {
        NotCondition {
            sub_condition: Box::new(condition),
        }
    }
}

pub struct PredicateCondition {
    pub prop: PropId,
    pub predicate: ComparisonOp,
    pub value: Value,
}

impl PredicateCondition {
    pub fn new(prop: PropId, predicate: ComparisonOp, value: Value) -> Self {
        PredicateCondition {
            prop,
            predicate,
            value,
        }
    }
}

pub enum ComparisonOp {
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    Equal,
    NotEqual,
}

pub struct ConditionBuilder {
    condition: Option<Condition>,
}

impl ConditionBuilder {
    pub fn new() -> Self {
        ConditionBuilder {
            condition: None,
        }
    }

    pub fn and(&mut self, condition: Condition) -> &mut Self {
        match self.condition.take() {
            Some(Condition::And(mut c)) => {
                c.sub_conditions.push(Box::new(condition));
                self.condition = Some(Condition::And(c));
            }
            Some(c) => {
                let new_condition = AndCondition::new(vec![c, condition]);
                self.condition = Some(Condition::And(new_condition));
            }
            None => {
                self.condition = Some(condition);
            }
        }
        self
    }

    pub fn or(&mut self, condition: Condition) -> &mut Self {
        match self.condition.take() {
            Some(Condition::Or(mut c)) => {
                c.sub_conditions.push(Box::new(condition));
                self.condition = Some(Condition::Or(c));
            }
            Some(c) => {
                let new_condition = OrCondition::new(vec![c, condition]);
                self.condition = Some(Condition::Or(new_condition));
            }
            None => {
                self.condition = Some(condition);
            }
        }
        self
    }

    pub fn not(&mut self) -> &mut Self {
        match self.condition.take() {
            Some(c) => {
                self.condition = Some(Condition::Not(NotCondition::new(c)));
            }
            None => {}
        }
        self
    }

    pub fn build(&mut self) -> Option<Condition> {
        self.condition.take()
    }
}