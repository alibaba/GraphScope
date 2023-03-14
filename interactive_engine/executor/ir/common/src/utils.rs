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

use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::ops::Deref;

use dyn_type::{Object, Primitives};

use crate::error::ParsePbError;
use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use crate::generated::physical as physical_pb;
use crate::generated::physical::PhysicalOpr;
use crate::NameOrId;

pub const SPLITTER: &'static str = ".";
pub const VAR_PREFIX: &'static str = "@";

pub enum OneOrMany<T> {
    One([T; 1]),
    Many(Vec<T>),
}

impl<T: Clone> Clone for OneOrMany<T> {
    fn clone(&self) -> Self {
        match self {
            OneOrMany::One(one) => Self::One(one.clone()),
            OneOrMany::Many(many) => Self::Many(many.clone()),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for OneOrMany<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OneOrMany::One(one) => one.get(0).unwrap().fmt(f),
            OneOrMany::Many(many) => many.fmt(f),
        }
    }
}

impl<T: Default> Default for OneOrMany<T> {
    fn default() -> Self {
        Self::One([T::default()])
    }
}

impl<T> AsRef<[T]> for OneOrMany<T> {
    fn as_ref(&self) -> &[T] {
        match self {
            OneOrMany::One(one) => &one[..],
            OneOrMany::Many(many) => many.as_slice(),
        }
    }
}

impl<T> Deref for OneOrMany<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> From<T> for OneOrMany<T> {
    fn from(t: T) -> Self {
        Self::One([t])
    }
}

impl<T> From<Vec<T>> for OneOrMany<T> {
    fn from(ts: Vec<T>) -> Self {
        Self::Many(ts)
    }
}

impl From<common_pb::Arithmetic> for common_pb::ExprOpr {
    fn from(arith: common_pb::Arithmetic) -> Self {
        common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Arith(unsafe {
                std::mem::transmute::<common_pb::Arithmetic, i32>(arith)
            })),
        }
    }
}

impl From<common_pb::Logical> for common_pb::ExprOpr {
    fn from(logical: common_pb::Logical) -> Self {
        common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Logical(unsafe {
                std::mem::transmute::<common_pb::Logical, i32>(logical)
            })),
        }
    }
}

impl From<common_pb::Value> for common_pb::ExprOpr {
    fn from(const_val: common_pb::Value) -> Self {
        common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Const(const_val)) }
    }
}

impl From<common_pb::Variable> for common_pb::ExprOpr {
    fn from(var: common_pb::Variable) -> Self {
        common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Var(var)) }
    }
}

/// An indicator for whether it is a map
impl From<(common_pb::VariableKeys, bool)> for common_pb::ExprOpr {
    fn from(vars: (common_pb::VariableKeys, bool)) -> Self {
        if !vars.1 {
            // not a map
            common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::Vars(vars.0)) }
        } else {
            // is a map
            common_pb::ExprOpr { item: Some(common_pb::expr_opr::Item::VarMap(vars.0)) }
        }
    }
}

impl From<bool> for common_pb::Value {
    fn from(b: bool) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::Boolean(b)) }
    }
}

impl From<f64> for common_pb::Value {
    fn from(f: f64) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::F64(f)) }
    }
}

impl From<i32> for common_pb::Value {
    fn from(i: i32) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::I32(i)) }
    }
}

impl From<i64> for common_pb::Value {
    fn from(i: i64) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::I64(i)) }
    }
}

impl From<String> for common_pb::Value {
    fn from(s: String) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::Str(s)) }
    }
}

impl From<Vec<i64>> for common_pb::Value {
    fn from(item: Vec<i64>) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::I64Array(common_pb::I64Array { item })) }
    }
}

impl From<Vec<f64>> for common_pb::Value {
    fn from(item: Vec<f64>) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::F64Array(common_pb::DoubleArray { item })) }
    }
}

impl From<Vec<String>> for common_pb::Value {
    fn from(item: Vec<String>) -> Self {
        common_pb::Value { item: Some(common_pb::value::Item::StrArray(common_pb::StringArray { item })) }
    }
}

impl From<i32> for common_pb::NameOrId {
    fn from(i: i32) -> Self {
        common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Id(i)) }
    }
}

impl From<&str> for common_pb::NameOrId {
    fn from(str: &str) -> Self {
        common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Name(str.to_string())) }
    }
}

impl From<String> for common_pb::NameOrId {
    fn from(str: String) -> Self {
        common_pb::NameOrId { item: Some(common_pb::name_or_id::Item::Name(str)) }
    }
}

pub const ID_KEY: &'static str = "~id";
pub const LABEL_KEY: &'static str = "~label";
pub const LENGTH_KEY: &'static str = "~len";
pub const ALL_KEY: &'static str = "~all";

impl From<String> for common_pb::Property {
    fn from(str: String) -> Self {
        if str == ID_KEY {
            common_pb::Property { item: Some(common_pb::property::Item::Id(common_pb::IdKey {})) }
        } else if str == LABEL_KEY {
            common_pb::Property { item: Some(common_pb::property::Item::Label(common_pb::LabelKey {})) }
        } else if str == LENGTH_KEY {
            common_pb::Property { item: Some(common_pb::property::Item::Len(common_pb::LengthKey {})) }
        } else if str == ALL_KEY {
            common_pb::Property { item: Some(common_pb::property::Item::All(common_pb::AllKey {})) }
        } else {
            common_pb::Property { item: Some(common_pb::property::Item::Key(str.into())) }
        }
    }
}

fn str_as_tag(str: String) -> Option<common_pb::NameOrId> {
    if !str.is_empty() {
        Some(if let Ok(str_int) = str.parse::<i32>() { str_int.into() } else { str.into() })
    } else {
        None
    }
}

impl From<String> for common_pb::Variable {
    fn from(str: String) -> Self {
        assert!(str.starts_with(VAR_PREFIX));
        // skip the var variable
        let str: String = str.chars().skip(1).collect();
        if !str.contains(SPLITTER) {
            common_pb::Variable {
                // If the tag is represented as an integer
                tag: str_as_tag(str),
                property: None,
            }
        } else {
            let mut splitter = str.split(SPLITTER);
            let tag: Option<common_pb::NameOrId> =
                if let Some(first) = splitter.next() { str_as_tag(first.to_string()) } else { None };
            let property: Option<common_pb::Property> =
                if let Some(second) = splitter.next() { Some(second.to_string().into()) } else { None };
            common_pb::Variable { tag, property }
        }
    }
}

impl From<i64> for pb::index_predicate::AndPredicate {
    fn from(id: i64) -> Self {
        pb::index_predicate::AndPredicate {
            predicates: vec![pb::index_predicate::Triplet {
                key: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Id(common_pb::IdKey {})),
                }),
                value: Some(id.into()),
                cmp: None,
            }],
        }
    }
}

impl From<Vec<i64>> for pb::IndexPredicate {
    fn from(ids: Vec<i64>) -> Self {
        let or_predicates: Vec<pb::index_predicate::AndPredicate> =
            ids.into_iter().map(|id| id.into()).collect();

        pb::IndexPredicate { or_predicates }
    }
}

impl From<String> for pb::index_predicate::AndPredicate {
    fn from(label: String) -> Self {
        pb::index_predicate::AndPredicate {
            predicates: vec![pb::index_predicate::Triplet {
                key: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Label(common_pb::LabelKey {})),
                }),
                value: Some(label.into()),
                cmp: None,
            }],
        }
    }
}

impl From<Vec<String>> for pb::IndexPredicate {
    fn from(names: Vec<String>) -> Self {
        let or_predicates: Vec<pb::index_predicate::AndPredicate> = names
            .into_iter()
            .map(|name| name.into())
            .collect();

        pb::IndexPredicate { or_predicates }
    }
}

impl TryFrom<common_pb::Value> for Object {
    type Error = ParsePbError;

    fn try_from(value: common_pb::Value) -> Result<Self, Self::Error> {
        use common_pb::value::Item::*;
        if let Some(item) = value.item.as_ref() {
            return match item {
                Boolean(b) => Ok((*b).into()),
                I32(i) => Ok((*i).into()),
                I64(i) => Ok((*i).into()),
                F64(f) => Ok((*f).into()),
                Str(s) => Ok(s.clone().into()),
                Blob(blob) => Ok(blob.clone().into()),
                None(_) => Ok(Object::None),
                I32Array(v) => Ok(v.item.clone().into()),
                I64Array(v) => Ok(v.item.clone().into()),
                F64Array(v) => Ok(v.item.clone().into()),
                StrArray(v) => Ok(v.item.clone().into()),
                PairArray(pairs) => {
                    let mut vec = Vec::<(Object, Object)>::with_capacity(pairs.item.len());
                    for item in pairs.item.clone().into_iter() {
                        let (key_obj, val_obj) =
                            (Object::try_from(item.key.unwrap())?, Object::try_from(item.val.unwrap())?);
                        vec.push((key_obj, val_obj));
                    }
                    Ok(vec.into())
                }
            };
        }

        Err(ParsePbError::from("empty value provided"))
    }
}

impl TryFrom<pb::IndexPredicate> for Vec<i64> {
    type Error = ParsePbError;

    fn try_from(value: pb::IndexPredicate) -> Result<Self, Self::Error> {
        let mut global_ids = vec![];
        for and_predicate in value.or_predicates {
            let predicate = and_predicate
                .predicates
                .get(0)
                .ok_or(ParsePbError::EmptyFieldError("`AndCondition` is emtpy".to_string()))?;

            let (key, value) = (predicate.key.as_ref(), predicate.value.as_ref());
            let key = key.ok_or("key is empty in kv_pair in indexed_scan")?;
            if let Some(common_pb::property::Item::Id(_id_key)) = key.item.as_ref() {
                let value = value.ok_or("value is empty in kv_pair in indexed_scan")?;

                match &value.item {
                    Some(common_pb::value::Item::I64(v)) => {
                        global_ids.push(*v);
                    }
                    Some(common_pb::value::Item::I64Array(arr)) => {
                        global_ids.extend(arr.item.iter().cloned())
                    }
                    Some(common_pb::value::Item::I32(v)) => {
                        global_ids.push(*v as i64);
                    }
                    Some(common_pb::value::Item::I32Array(arr)) => {
                        global_ids.extend(arr.item.iter().map(|i| *i as i64));
                    }
                    _ => Err(ParsePbError::Unsupported(
                        "indexed value other than integer (I32, I64) and integer array".to_string(),
                    ))?,
                }
            }
        }
        Ok(global_ids)
    }
}

impl TryFrom<pb::IndexPredicate> for Vec<(NameOrId, Object)> {
    type Error = ParsePbError;

    fn try_from(value: pb::IndexPredicate) -> Result<Self, Self::Error> {
        let mut primary_key_values = vec![];
        // for pk values, which should be a set of and_conditions.
        let and_predicates = value
            .or_predicates
            .get(0)
            .ok_or(ParsePbError::EmptyFieldError("`OrCondition` is emtpy".to_string()))?;
        for predicate in &and_predicates.predicates {
            let key_pb = predicate
                .key
                .clone()
                .ok_or("key is empty in kv_pair in indexed_scan")?;
            let value = predicate
                .value
                .clone()
                .ok_or("value is empty in kv_pair in indexed_scan")?;
            let key = match key_pb.item {
                Some(common_pb::property::Item::Key(prop_key)) => prop_key.try_into()?,
                _ => Err(ParsePbError::Unsupported(
                    "Other keys rather than property key in kv_pair in indexed_scan".to_string(),
                ))?,
            };
            let obj_val = Object::try_from(value)?;
            primary_key_values.push((key, obj_val));
        }
        Ok(primary_key_values)
    }
}

impl From<pb::Project> for pb::logical_plan::Operator {
    fn from(opr: pb::Project) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Project(opr)) }
    }
}

impl From<pb::Select> for pb::logical_plan::Operator {
    fn from(opr: pb::Select) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Select(opr)) }
    }
}

impl From<pb::Join> for pb::logical_plan::Operator {
    fn from(opr: pb::Join) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Join(opr)) }
    }
}

impl From<pb::Union> for pb::logical_plan::Operator {
    fn from(opr: pb::Union) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Union(opr)) }
    }
}

impl From<pb::Intersect> for pb::logical_plan::Operator {
    fn from(opr: pb::Intersect) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Intersect(opr)) }
    }
}

impl From<pb::GroupBy> for pb::logical_plan::Operator {
    fn from(opr: pb::GroupBy) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::GroupBy(opr)) }
    }
}

impl From<pb::OrderBy> for pb::logical_plan::Operator {
    fn from(opr: pb::OrderBy) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::OrderBy(opr)) }
    }
}

impl From<pb::Dedup> for pb::logical_plan::Operator {
    fn from(opr: pb::Dedup) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Dedup(opr)) }
    }
}

impl From<pb::Unfold> for pb::logical_plan::Operator {
    fn from(opr: pb::Unfold) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Unfold(opr)) }
    }
}

impl From<pb::Apply> for pb::logical_plan::Operator {
    fn from(opr: pb::Apply) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Apply(opr)) }
    }
}

impl From<pb::SegmentApply> for pb::logical_plan::Operator {
    fn from(opr: pb::SegmentApply) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::SegApply(opr)) }
    }
}

impl From<pb::Scan> for pb::logical_plan::Operator {
    fn from(opr: pb::Scan) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Scan(opr)) }
    }
}

impl From<pb::logical_plan::Operator> for Option<pb::Scan> {
    fn from(opr: pb::logical_plan::Operator) -> Self {
        if let Some(opr) = opr.opr {
            match opr {
                pb::logical_plan::operator::Opr::Scan(scan) => return Some(scan),
                _ => (),
            }
        }
        None
    }
}

impl From<pb::Limit> for pb::logical_plan::Operator {
    fn from(opr: pb::Limit) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Limit(opr)) }
    }
}

impl From<pb::As> for pb::logical_plan::Operator {
    fn from(opr: pb::As) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::As(opr)) }
    }
}

impl From<pb::EdgeExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::EdgeExpand) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Edge(opr)) }
    }
}

impl From<pb::PathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::PathExpand) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Path(opr)) }
    }
}

impl From<pb::PathStart> for pb::logical_plan::Operator {
    fn from(opr: pb::PathStart) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::PathStart(opr)) }
    }
}

impl From<pb::PathEnd> for pb::logical_plan::Operator {
    fn from(opr: pb::PathEnd) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::PathEnd(opr)) }
    }
}

impl From<pb::FusedOperator> for pb::logical_plan::Operator {
    fn from(opr: pb::FusedOperator) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Fused(opr)) }
    }
}

/*
impl From<pb::ShortestPathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::ShortestPathExpand) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::ShortestPath(opr)) }
    }
}
 */

impl From<pb::GetV> for pb::logical_plan::Operator {
    fn from(opr: pb::GetV) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Vertex(opr)) }
    }
}

impl From<pb::Pattern> for pb::logical_plan::Operator {
    fn from(opr: pb::Pattern) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Pattern(opr)) }
    }
}

impl From<pb::Sink> for pb::logical_plan::Operator {
    fn from(opr: pb::Sink) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Sink(opr)) }
    }
}

impl From<Object> for common_pb::Value {
    fn from(value: Object) -> Self {
        let item = match value {
            Object::Primitive(v) => match v {
                // TODO: It seems that Byte is only used for bool for now
                Primitives::Byte(v) => common_pb::value::Item::Boolean(!(v == 0)),
                Primitives::Integer(v) => common_pb::value::Item::I32(v),
                Primitives::Long(v) => common_pb::value::Item::I64(v),
                Primitives::ULLong(v) => common_pb::value::Item::Str(v.to_string()),
                Primitives::Float(v) => common_pb::value::Item::F64(v),
            },
            Object::String(s) => common_pb::value::Item::Str(s),
            Object::Blob(b) => common_pb::value::Item::Blob(b.to_vec()),
            Object::Vector(v) => common_pb::value::Item::StrArray(common_pb::StringArray {
                item: v
                    .into_iter()
                    .map(|obj| obj.to_string())
                    .collect(),
            }),
            Object::KV(kv) => {
                let mut pairs: Vec<common_pb::Pair> = Vec::with_capacity(kv.len());
                for (key, val) in kv {
                    let key_pb: common_pb::Value = key.into();
                    let val_pb: common_pb::Value = val.into();
                    pairs.push(common_pb::Pair { key: Some(key_pb), val: Some(val_pb) })
                }
                common_pb::value::Item::PairArray(common_pb::PairArray { item: pairs })
            }
            Object::None => common_pb::value::Item::None(common_pb::None {}),
            _ => unimplemented!(),
        };

        common_pb::Value { item: Some(item) }
    }
}

impl pb::logical_plan::Operator {
    pub fn is_whole_graph(&self) -> bool {
        if let Some(opr) = &self.opr {
            match opr {
                pb::logical_plan::operator::Opr::Scan(scan) => {
                    scan.idx_predicate.is_none()
                        && scan.alias.is_none()
                        && scan
                            .params
                            .as_ref()
                            .map(|params| !params.is_queryable())
                            .unwrap_or(true)
                }
                _ => false,
            }
        } else {
            false
        }
    }
}

impl pb::QueryParams {
    fn is_queryable(&self) -> bool {
        !(self.tables.is_empty()
            && self.predicate.is_none()
            && self.limit.is_none()
            && self.sample_ratio == 1.0)
    }
}

impl pb::edge_expand::Direction {
    pub fn reverse(&self) -> pb::edge_expand::Direction {
        match self {
            pb::edge_expand::Direction::Out => pb::edge_expand::Direction::In,
            pb::edge_expand::Direction::In => pb::edge_expand::Direction::Out,
            pb::edge_expand::Direction::Both => pb::edge_expand::Direction::Both,
        }
    }
}

impl From<physical_pb::physical_opr::operator::OpKind> for physical_pb::PhysicalOpr {
    fn from(op_kind: physical_pb::physical_opr::operator::OpKind) -> Self {
        let opr = physical_pb::physical_opr::Operator { op_kind: Some(op_kind) };
        // TODO: add op_meta once supported
        physical_pb::PhysicalOpr { opr: Some(opr), op_meta: vec![] }
    }
}

impl From<physical_pb::Repartition> for physical_pb::PhysicalOpr {
    fn from(repartition: physical_pb::Repartition) -> Self {
        let op_kind = physical_pb::physical_opr::operator::OpKind::Repartition(repartition);
        op_kind.into()
    }
}

impl From<physical_pb::EdgeExpand> for physical_pb::PhysicalOpr {
    fn from(expand: physical_pb::EdgeExpand) -> Self {
        let op_kind = physical_pb::physical_opr::operator::OpKind::Edge(expand);
        op_kind.into()
    }
}

impl From<physical_pb::Scan> for physical_pb::PhysicalOpr {
    fn from(scan: physical_pb::Scan) -> Self {
        let op_kind = physical_pb::physical_opr::operator::OpKind::Scan(scan);
        op_kind.into()
    }
}

impl From<pb::Project> for physical_pb::Project {
    fn from(project: pb::Project) -> Self {
        let mappings = project
            .mappings
            .into_iter()
            .map(|expr| physical_pb::project::ExprAlias {
                expr: expr.expr,
                alias: expr.alias.map(|tag| tag.try_into().unwrap()),
            })
            .collect();
        physical_pb::Project { mappings, is_append: project.is_append }
    }
}

impl From<pb::GroupBy> for physical_pb::GroupBy {
    fn from(group: pb::GroupBy) -> Self {
        let mappings = group
            .mappings
            .into_iter()
            .map(|key_alias| physical_pb::group_by::KeyAlias {
                key: key_alias.key.map(|tag| tag.try_into().unwrap()),
                alias: key_alias
                    .alias
                    .map(|tag| tag.try_into().unwrap()),
            })
            .collect();
        let functions = group
            .functions
            .into_iter()
            .map(|agg_func| physical_pb::group_by::AggFunc {
                vars: agg_func.vars,
                aggregate: agg_func.aggregate,
                alias: agg_func
                    .alias
                    .map(|tag| tag.try_into().unwrap()),
            })
            .collect();
        physical_pb::GroupBy { mappings, functions }
    }
}

impl From<pb::Unfold> for physical_pb::Unfold {
    fn from(unfold: pb::Unfold) -> Self {
        physical_pb::Unfold {
            tag: unfold.tag.map(|tag| tag.try_into().unwrap()),
            alias: unfold.alias.map(|tag| tag.try_into().unwrap()),
        }
    }
}

impl From<pb::GetV> for physical_pb::GetV {
    fn from(get_v: pb::GetV) -> Self {
        physical_pb::GetV {
            tag: get_v.tag.map(|tag| tag.try_into().unwrap()),
            opt: get_v.opt,
            params: get_v.params,
            alias: get_v.alias.map(|tag| tag.try_into().unwrap()),
        }
    }
}

impl From<pb::EdgeExpand> for physical_pb::EdgeExpand {
    fn from(edge: pb::EdgeExpand) -> Self {
        physical_pb::EdgeExpand {
            v_tag: edge.v_tag.map(|tag| tag.try_into().unwrap()),
            direction: edge.direction,
            params: edge.params,
            alias: edge.alias.map(|tag| tag.try_into().unwrap()),
            expand_opt: edge.expand_opt,
        }
    }
}

impl From<pb::PathExpand> for physical_pb::PathExpand {
    fn from(path: pb::PathExpand) -> Self {
        physical_pb::PathExpand {
            base: path.base.map(|base| base.into()),
            start_tag: path
                .start_tag
                .map(|tag| tag.try_into().unwrap()),
            alias: path.alias.map(|tag| tag.try_into().unwrap()),
            hop_range: path.hop_range,
            path_opt: path.path_opt,
            result_opt: path.result_opt,
            condition: path.condition,
        }
    }
}

impl From<pb::Scan> for physical_pb::Scan {
    fn from(scan: pb::Scan) -> Self {
        physical_pb::Scan {
            scan_opt: scan.scan_opt,
            alias: scan.alias.map(|tag| tag.try_into().unwrap()),
            params: scan.params,
            idx_predicate: scan.idx_predicate,
        }
    }
}

impl From<pb::Sink> for physical_pb::Sink {
    fn from(sink: pb::Sink) -> Self {
        physical_pb::Sink {
            tags: sink
                .tags
                .into_iter()
                .map(|tag| physical_pb::sink::OptTag { tag: tag.key.map(|tag| tag.try_into().unwrap()) })
                .collect(),
            sink_target: sink.sink_target,
        }
    }
}

impl TryFrom<&physical_pb::PhysicalOpr> for physical_pb::physical_opr::operator::OpKind {
    type Error = ParsePbError;

    fn try_from(op: &PhysicalOpr) -> Result<Self, Self::Error> {
        op.clone().try_into()
    }
}

impl TryFrom<physical_pb::PhysicalOpr> for physical_pb::physical_opr::operator::OpKind {
    type Error = ParsePbError;

    fn try_from(op: PhysicalOpr) -> Result<Self, Self::Error> {
        let op_kind = op
            .opr
            .ok_or(ParsePbError::EmptyFieldError("algebra op is empty".to_string()))?
            .op_kind
            .ok_or(ParsePbError::EmptyFieldError("algebra op_kind is empty".to_string()))?;
        Ok(op_kind)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_str_to_variable() {
        let case1 = "@1";
        assert_eq!(
            common_pb::Variable { tag: Some(common_pb::NameOrId::from(1)), property: None },
            common_pb::Variable::from(case1.to_string())
        );

        let case2 = "@a";
        assert_eq!(
            common_pb::Variable { tag: Some(common_pb::NameOrId::from("a".to_string())), property: None },
            common_pb::Variable::from(case2.to_string())
        );

        let case3 = "@1.~id";
        assert_eq!(
            common_pb::Variable {
                tag: Some(common_pb::NameOrId::from(1)),
                property: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Id(common_pb::IdKey {}))
                })
            },
            common_pb::Variable::from(case3.to_string())
        );

        let case4 = "@1.~label";
        assert_eq!(
            common_pb::Variable {
                tag: Some(common_pb::NameOrId::from(1)),
                property: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Label(common_pb::LabelKey {}))
                })
            },
            common_pb::Variable::from(case4.to_string())
        );

        let case5 = "@1.name";
        assert_eq!(
            common_pb::Variable {
                tag: Some(common_pb::NameOrId::from(1)),
                property: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Key("name".to_string().into()))
                })
            },
            common_pb::Variable::from(case5.to_string())
        );

        let case6 = "@.name";
        assert_eq!(
            common_pb::Variable {
                tag: None,
                property: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Key("name".to_string().into()))
                })
            },
            common_pb::Variable::from(case6.to_string())
        );

        let case7 = "@";
        assert_eq!(
            common_pb::Variable { tag: None, property: None },
            common_pb::Variable::from(case7.to_string())
        );
    }
}
