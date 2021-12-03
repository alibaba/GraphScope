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

use super::property::*;
use crate::schema::prelude::*;

#[derive(Debug, Clone)]
pub enum Condition {
    And(AndCondition),
    Or(OrCondition),
    Not(NotCondition),
    Cmp(CmpCondition),
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct OrCondition {
    sub_conditions: Vec<Box<Condition>>,
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

#[derive(Debug, Clone)]
pub struct NotCondition {
    sub_condition: Box<Condition>,
}

impl NotCondition {
    pub fn new(condition: Condition) -> Self {
        NotCondition {
            sub_condition: Box::new(condition),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CmpCondition {
    pub key: PropId,
    pub op: CmpOp,
    pub value: Property,
}

impl CmpCondition {
    pub fn new(key: PropId, op: CmpOp, value: Property) -> Self {
        CmpCondition {
            key,
            op,
            value,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum CmpOp {
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    Equal,
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

#[test]
fn test_condition() {
    let mut builder = ConditionBuilder::new();

    // (prop#1 >= 2 and prop#1 <= 10) or prop#2 == "xxx"
    let condition = builder
        .and(Condition::Cmp(CmpCondition::new(1, CmpOp::GreaterEqual, Property::Int(2))))
        .and(Condition::Cmp(CmpCondition::new(1, CmpOp::LessEqual, Property::Int(10))))
        .or(Condition::Cmp(CmpCondition::new(2, CmpOp::Equal, Property::String("xxx".to_owned()))))
        .build().unwrap();
    println!("{:?}", condition);
}
