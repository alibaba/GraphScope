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
use std::collections::HashMap;
use std::vec::IntoIter;

use super::*;
use crate::api::filter::ElemFilter;
use crate::api::{property::*, Edge, Vertex};
use crate::schema::prelude::*;

#[derive(Debug, Clone)]
pub struct LocalEntity {
    id: i64,
    prop_list: HashMap<u32, Property>,
}

unsafe impl Send for LocalEntity {}

impl LocalEntity {
    pub fn new(id: i64) -> Self {
        LocalEntity { id, prop_list: HashMap::new() }
    }

    pub fn add_properties(&mut self, prop_list: Vec<(i32, Property)>) {
        for (propid, propval) in prop_list.into_iter() {
            self.prop_list.insert(propid as u32, propval);
        }
    }
}

impl Vertex for LocalEntity {
    type PI = IntoIter<(PropId, Property)>;

    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_label_id(&self) -> u32 {
        0
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        if let Some(prop) = self.prop_list.get(&prop_id) {
            Some(prop.clone())
        } else {
            None
        }
    }

    fn get_properties(&self) -> Self::PI {
        self.prop_list
            .clone()
            .into_iter()
            .collect::<Vec<(PropId, Property)>>()
            .into_iter()
    }
}

impl Edge for LocalEntity {
    type PI = IntoIter<(PropId, Property)>;

    fn get_label_id(&self) -> u32 {
        0
    }

    fn get_property(&self, prop_id: u32) -> Option<Property> {
        if let Some(prop) = self.prop_list.get(&prop_id) {
            Some(prop.clone())
        } else {
            None
        }
    }

    fn get_properties(&self) -> Self::PI {
        self.prop_list
            .clone()
            .into_iter()
            .collect::<Vec<(PropId, Property)>>()
            .into_iter()
    }

    fn get_src_label_id(&self) -> crate::api::LabelId {
        0
    }

    fn get_dst_label_id(&self) -> crate::api::LabelId {
        0
    }

    fn get_src_id(&self) -> crate::api::VertexId {
        0
    }

    fn get_dst_id(&self) -> crate::api::VertexId {
        0
    }

    fn get_edge_id(&self) -> crate::api::EdgeId {
        0
    }
}

fn prepare_entites() -> impl Iterator<Item = LocalEntity> {
    let mut v = vec![];
    let props = vec![
        (1, Property::Int(10)),
        (2, Property::String("Astr, Hello World".to_string())),
        (3, Property::Float(1.0)),
    ];
    let mut vertex = LocalEntity::new(1);
    vertex.add_properties(props);
    v.push(vertex);

    let props = vec![
        (1, Property::Int(20)),
        (2, Property::String("Bstr, GraphScope".to_string())),
        (3, Property::Float(2.0)),
    ];
    let mut vertex = LocalEntity::new(2);
    vertex.add_properties(props);
    v.push(vertex);

    let props = vec![
        (1, Property::Int(30)),
        (2, Property::String("Cstr, interactive engine".to_string())),
        (3, Property::Float(3.0)),
    ];
    let mut vertex = LocalEntity::new(3);
    vertex.add_properties(props);
    v.push(vertex);

    let props = vec![
        (1, Property::Int(40)),
        (2, Property::String("Dstr, learning engine".to_string())),
        (3, Property::Float(4.0)),
    ];
    let mut vertex = LocalEntity::new(4);
    vertex.add_properties(props);
    v.push(vertex);

    v.into_iter()
}

#[test]
fn test_condition_has_prop_operation() {
    let entites = prepare_entites().collect::<Vec<LocalEntity>>();
    let predicate = PredCondition::new_has_prop(1);
    assert_eq!(
        4,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        4,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );

    let predicate = PredCondition::new_has_prop(4);
    assert_eq!(
        0,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        0,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
}

#[test]
fn test_condition_within_operation() {
    let entites = prepare_entites().collect::<Vec<LocalEntity>>();
    let predicate = PredCondition::new_predicate(
        Operand::PropId(1),
        CmpOperator::WithIn,
        Operand::Const(Property::ListInt(vec![20, 30, 40])),
    );
    let e1 = entites
        .clone()
        .into_iter()
        .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(3, e1.len());
    assert_eq!(e1[0].get_id(), 2);
    assert_eq!(e1[1].get_id(), 3);
    assert_eq!(e1[2].get_id(), 4);
    let e1 = entites
        .clone()
        .into_iter()
        .filter(|v| predicate.filter_edge(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(3, e1.len());
    assert_eq!(e1[0].get_id(), 2);
    assert_eq!(e1[1].get_id(), 3);
    assert_eq!(e1[2].get_id(), 4);
    let predicate = PredCondition::new_predicate(
        Operand::PropId(2),
        CmpOperator::WithIn,
        Operand::Const(Property::ListString(vec!["Astr, Hello World".to_owned()])),
    );
    assert_eq!(
        1,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        1,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );

    let predicate = PredCondition::new_predicate(
        Operand::PropId(3),
        CmpOperator::WithIn,
        Operand::Const(Property::ListFloat(vec![2.0, 3.0])),
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
}

#[test]
fn test_condition_without_operation() {
    let entites = prepare_entites().collect::<Vec<LocalEntity>>();
    let predicate = PredCondition::new_predicate(
        Operand::PropId(1),
        CmpOperator::WithOut,
        Operand::Const(Property::ListInt(vec![40, 50, 60])),
    );
    let e1 = entites
        .clone()
        .into_iter()
        .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(3, e1.len());
    assert_eq!(e1[0].get_id(), 1);
    assert_eq!(e1[1].get_id(), 2);
    assert_eq!(e1[2].get_id(), 3);
    let e1 = entites
        .clone()
        .into_iter()
        .filter(|v| predicate.filter_edge(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(3, e1.len());
    assert_eq!(e1[0].get_id(), 1);
    assert_eq!(e1[1].get_id(), 2);
    assert_eq!(e1[2].get_id(), 3);
    let predicate = PredCondition::new_predicate(
        Operand::PropId(2),
        CmpOperator::WithOut,
        Operand::Const(Property::ListString(vec![
            "Astr, Hello World".to_owned(),
            "Bstr, GraphScope".to_string(),
        ])),
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );

    let predicate = PredCondition::new_predicate(
        Operand::PropId(3),
        CmpOperator::WithOut,
        Operand::Const(Property::ListFloat(vec![1.0, 4.0, 2.0, 3.0])),
    );
    assert_eq!(
        0,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        0,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
}

#[test]
fn test_condition_start_end_with_operation() {
    let entites = prepare_entites().collect::<Vec<LocalEntity>>();
    let predicate = PredCondition::new_predicate(
        Operand::PropId(2),
        CmpOperator::StartWith,
        Operand::Const(Property::String("Astr".to_owned())),
    );
    let e1 = entites
        .clone()
        .into_iter()
        .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(1, e1.len());
    assert_eq!(e1[0].get_id(), 1);

    let e1 = entites
        .clone()
        .into_iter()
        .filter(|v| predicate.filter_edge(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(1, e1.len());
    assert_eq!(e1[0].get_id(), 1);

    let predicate = PredCondition::new_predicate(
        Operand::PropId(2),
        CmpOperator::EndWith,
        Operand::Const(Property::String("engine".to_owned())),
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
}

#[test]
fn test_condition_cmp_operation() {
    let entites = prepare_entites().collect::<Vec<LocalEntity>>();
    let predicate = PredCondition::new_predicate(
        Operand::PropId(1),
        CmpOperator::Equal,
        Operand::Const(Property::Int(10)),
    );
    assert_eq!(
        1,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        1,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );

    let predicate = PredCondition::new_predicate(
        Operand::PropId(1),
        CmpOperator::LessEqual,
        Operand::Const(Property::Int(20)),
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );

    let predicate = PredCondition::new_predicate(
        Operand::PropId(1),
        CmpOperator::GreaterEqual,
        Operand::Const(Property::Int(30)),
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_vertex(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
    assert_eq!(
        2,
        entites
            .clone()
            .into_iter()
            .filter(|v| predicate.filter_edge(v).unwrap_or(false))
            .collect::<Vec<LocalEntity>>()
            .len()
    );
}

#[test]
fn test_condition_builder() {
    let mut builder = ConditionBuilder::new();

    // // (prop#1 >= 2 and prop#1 <= 10) or prop#2 == "xxx"
    let condition = builder
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(1),
            CmpOperator::GreaterEqual,
            Operand::Const(Property::Int(2)),
        )))
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(1),
            CmpOperator::LessEqual,
            Operand::Const(Property::Int(10)),
        )))
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(2),
            CmpOperator::Equal,
            Operand::Const(Property::String("xxx".to_owned())),
        )))
        .build()
        .expect("build failed");
    println!("condition is {:?}", condition);
}

#[test]
fn test_condition_filter_vertex() {
    // (#1 > 20 and #3 in [2.0, 3.0]) or (#1 <= 40 and #2 contains("engine"))
    // 3\4
    let condition1 = ConditionBuilder::new()
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(1),
            CmpOperator::GreaterThan,
            Operand::Const(Property::Int(20)),
        )))
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(3),
            CmpOperator::WithIn,
            Operand::Const(Property::ListFloat(vec![2.0, 3.0])),
        )))
        .build()
        .expect("build failed");
    let condition2 = ConditionBuilder::new()
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(1),
            CmpOperator::LessEqual,
            Operand::Const(Property::Int(40)),
        )))
        .and(Condition::new(PredCondition::new_predicate(
            Operand::Const(Property::String("engine".to_owned())),
            CmpOperator::WithIn,
            Operand::PropId(2),
        )))
        .build()
        .expect("build failed");

    let condition = ConditionBuilder::new()
        .or(condition1)
        .or(condition2)
        .build()
        .expect("build failed");

    println!("condition is {:?}", condition);
    let vertices = prepare_entites()
        .filter(|v| condition.filter_vertex(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(2, vertices.len());
    assert_eq!(vertices[0].get_id(), 3);
    assert_eq!(vertices[1].get_id(), 4);
}

#[test]
fn test_condition_filter_edge() {
    // (#1 > 20 and #3 in [2.0, 3.0]) or (#1 <= 40 and #2 endWith("engine"))
    // 3\4
    let condition1 = ConditionBuilder::new()
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(1),
            CmpOperator::GreaterThan,
            Operand::Const(Property::Int(20)),
        )))
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(3),
            CmpOperator::WithIn,
            Operand::Const(Property::ListFloat(vec![2.0, 3.0])),
        )))
        .build()
        .expect("build failed");
    let condition2 = ConditionBuilder::new()
        .and(Condition::new(PredCondition::new_predicate(
            Operand::PropId(1),
            CmpOperator::LessEqual,
            Operand::Const(Property::Int(40)),
        )))
        .and(Condition::new(PredCondition::new_predicate(
            Operand::Const(Property::String("engine".to_owned())),
            CmpOperator::WithIn,
            Operand::PropId(2),
        )))
        .build()
        .expect("build failed");

    let condition = ConditionBuilder::new()
        .or(condition1)
        .or(condition2)
        .build()
        .expect("build failed");

    println!("condition is {:?}", condition);
    let vertices = prepare_entites()
        .filter(|v| condition.filter_edge(v).unwrap_or(false))
        .collect::<Vec<LocalEntity>>();
    assert_eq!(2, vertices.len());
    assert_eq!(vertices[0].get_id(), 3);
    assert_eq!(vertices[1].get_id(), 4);
}
