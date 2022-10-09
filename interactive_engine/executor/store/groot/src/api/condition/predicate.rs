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

use super::operand::Operand;
use crate::api::filter::ElemFilter;
use crate::api::{property::*, Edge, Vertex};
use crate::schema::prelude::*;
use crate::{unwrap_some_or, GraphResult};

#[derive(Debug, Clone, PartialEq)]
pub enum PredCondition {
    HasProp(PropId),
    Cmp(CmpCondition),
}

impl PredCondition {
    pub fn new_has_prop(prop_id: PropId) -> Self {
        PredCondition::HasProp(prop_id)
    }
    pub fn new_predicate(left: Operand, op: CmpOperator, right: Operand) -> Self {
        PredCondition::Cmp(CmpCondition { left, op, right })
    }
}

impl ElemFilter for PredCondition {
    fn filter_vertex<V: Vertex>(&self, vertex: &V) -> GraphResult<bool> {
        let ret = match self {
            PredCondition::HasProp(prop_id) => Ok(vertex.get_property(*prop_id).is_some()),
            PredCondition::Cmp(cmp_pred) => cmp_pred.filter_vertex(vertex),
        };
        ret
    }

    fn filter_edge<E: Edge>(&self, edge: &E) -> GraphResult<bool> {
        let ret = match self {
            PredCondition::HasProp(prop_id) => Ok(edge.get_property(*prop_id).is_some()),
            PredCondition::Cmp(cmp_pred) => cmp_pred.filter_edge(edge),
        };
        ret
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CmpCondition {
    left: Operand,
    op: CmpOperator,
    right: Operand,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CmpOperator {
    Equal,
    NotEqual,
    LessThan,
    LessEqual,
    GreaterThan,
    GreaterEqual,
    WithIn,
    WithOut,
    StartWith,
    EndWith,
}

impl CmpOperator {
    pub fn compute(&self, left: &Property, right: &Property) -> GraphResult<bool> {
        match self {
            CmpOperator::Equal => Ok(left == right),
            CmpOperator::NotEqual => Ok(left != right),
            CmpOperator::LessThan => Ok(left < right),
            CmpOperator::LessEqual => Ok(left <= right),
            CmpOperator::GreaterThan => Ok(left > right),
            CmpOperator::GreaterEqual => Ok(left >= right),
            CmpOperator::WithIn => right.contains(left),
            CmpOperator::WithOut => right.contains(left).map(|ret| !ret),
            CmpOperator::StartWith => left.start_with(right),
            CmpOperator::EndWith => left.end_with(right),
        }
    }
}

impl ElemFilter for CmpCondition {
    fn filter_vertex<V: Vertex>(&self, vertex: &V) -> GraphResult<bool> {
        let owned_left = self.left.extract_vertex_property(vertex);
        let mut left = owned_left.as_ref();
        if left.is_none() {
            left = self.left.get_const_property();
        }
        let left = unwrap_some_or!(left, return Ok(false));

        let mut right = self.right.get_const_property();
        let owned_right;
        if right.is_none() {
            owned_right = self.right.extract_vertex_property(vertex);
            right = owned_right.as_ref();
        }
        let right = unwrap_some_or!(right, return Ok(false));
        self.op.compute(left, right)
    }

    fn filter_edge<E: Edge>(&self, edge: &E) -> GraphResult<bool> {
        let owned_left = self.left.extract_edge_property(edge);
        let mut left = owned_left.as_ref();
        if left.is_none() {
            left = self.left.get_const_property();
        }
        let left = unwrap_some_or!(left, return Ok(false));

        let mut right = self.right.get_const_property();
        let owned_right;
        if right.is_none() {
            owned_right = self.right.extract_edge_property(edge);
            right = owned_right.as_ref();
        }
        let right = unwrap_some_or!(right, return Ok(false));
        self.op.compute(left, right)
    }
}
