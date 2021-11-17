#![allow(dead_code)]
use super::PropertyId;
use super::property::Value;

pub enum OldCondition {
    And(AndCondition),
    Or(OrCondition),
    Not(NotCondition),
    Predicate(PredicateCondition),
}

pub struct AndCondition {
    pub sub_conditions: Vec<Box<OldCondition>>,
}

impl AndCondition {
    pub fn new(conditions: Vec<OldCondition>) -> Self {
        let sub_conditions = conditions.into_iter()
            .map(|c| Box::new(c)).collect();
        AndCondition {
            sub_conditions,
        }
    }
}

pub struct OrCondition {
    pub sub_conditions: Vec<Box<OldCondition>>,
}

impl OrCondition {
    pub fn new(condition: Vec<OldCondition>) -> Self {
        let sub_conditions = condition.into_iter()
            .map(|c| Box::new(c)).collect();
        OrCondition {
            sub_conditions,
        }
    }
}

pub struct NotCondition {
    pub sub_condition: Box<OldCondition>,
}

impl NotCondition {
    pub fn new(condition: OldCondition) -> Self {
        NotCondition {
            sub_condition: Box::new(condition),
        }
    }
}

pub struct PredicateCondition {
    pub prop: PropertyId,
    pub predicate: ComparisonOp,
    pub value: Value,
}

impl PredicateCondition {
    pub fn new(prop: PropertyId, predicate: ComparisonOp, value: Value) -> Self {
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
    condition: Option<OldCondition>,
}

impl ConditionBuilder {
    pub fn new() -> Self {
        ConditionBuilder {
            condition: None,
        }
    }

    pub fn and(&mut self, condition: OldCondition) -> &mut Self {
        match self.condition.take() {
            Some(OldCondition::And(mut c)) => {
                c.sub_conditions.push(Box::new(condition));
                self.condition = Some(OldCondition::And(c));
            }
            Some(c) => {
                let new_condition = AndCondition::new(vec![c, condition]);
                self.condition = Some(OldCondition::And(new_condition));
            }
            None => {
                self.condition = Some(condition);
            }
        }
        self
    }

    pub fn or(&mut self, condition: OldCondition) -> &mut Self {
        match self.condition.take() {
            Some(OldCondition::Or(mut c)) => {
                c.sub_conditions.push(Box::new(condition));
                self.condition = Some(OldCondition::Or(c));
            }
            Some(c) => {
                let new_condition = OrCondition::new(vec![c, condition]);
                self.condition = Some(OldCondition::Or(new_condition));
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
                self.condition = Some(OldCondition::Not(NotCondition::new(c)));
            }
            None => {}
        }
        self
    }

    pub fn build(&mut self) -> Option<OldCondition> {
        self.condition.take()
    }
}