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

mod operand;
pub mod predicate;
#[cfg(test)]
mod test;
pub use operand::Operand;
pub use predicate::CmpOperator;
pub use predicate::PredCondition;

use super::filter::ElemFilter;
use super::{Edge, Vertex};
use crate::GraphResult;

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    And(AndCondition),
    Or(OrCondition),
    Not(NotCondition),
    Pred(PredCondition),
}

impl Condition {
    pub fn new(pred: PredCondition) -> Self {
        Condition::Pred(pred)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AndCondition {
    pub sub_conditions: Vec<Box<Condition>>,
}

impl AndCondition {
    pub fn new(conditions: Vec<Condition>) -> Self {
        let sub_conditions = conditions
            .into_iter()
            .map(|c| Box::new(c))
            .collect();
        AndCondition { sub_conditions }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrCondition {
    sub_conditions: Vec<Box<Condition>>,
}

impl OrCondition {
    pub fn new(condition: Vec<Condition>) -> Self {
        let sub_conditions = condition
            .into_iter()
            .map(|c| Box::new(c))
            .collect();
        OrCondition { sub_conditions }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub struct NotCondition {
    sub_condition: Box<Condition>,
}

impl NotCondition {
    pub fn new(condition: Condition) -> Self {
        NotCondition { sub_condition: Box::new(condition) }
    }
}

impl ElemFilter for Condition {
    fn filter_vertex<V: Vertex>(&self, vertex: &V) -> GraphResult<bool> {
        match self {
            Condition::And(AndCondition { sub_conditions }) => {
                for sub_cond in sub_conditions.iter() {
                    if !sub_cond.filter_vertex(vertex)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Condition::Or(OrCondition { sub_conditions }) => {
                for sub_cond in sub_conditions.iter() {
                    if sub_cond.filter_vertex(vertex)? {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            Condition::Not(NotCondition { sub_condition }) => {
                return Ok(!sub_condition.filter_vertex(vertex)?);
            }
            Condition::Pred(pred) => {
                return pred.filter_vertex(vertex);
            }
        }
    }

    fn filter_edge<E: Edge>(&self, edge: &E) -> GraphResult<bool> {
        match self {
            Condition::And(AndCondition { sub_conditions }) => {
                for sub_cond in sub_conditions.iter() {
                    if !sub_cond.filter_edge(edge)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            Condition::Or(OrCondition { sub_conditions }) => {
                for sub_cond in sub_conditions.iter() {
                    if sub_cond.filter_edge(edge)? {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            Condition::Not(NotCondition { sub_condition }) => {
                return Ok(!sub_condition.filter_edge(edge)?);
            }
            Condition::Pred(pred) => {
                return pred.filter_edge(edge);
            }
        }
    }
}

pub struct ConditionBuilder {
    condition: Option<Condition>,
}

impl ConditionBuilder {
    pub fn new() -> Self {
        ConditionBuilder { condition: None }
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
