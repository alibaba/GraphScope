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
use std::io;

use dyn_type::{BorrowObject, Object, Primitives};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use prost::Message;

use crate::error::{ParsePbError, ParsePbResult};
use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use crate::generated::results as result_pb;

pub mod error;
pub mod expr_parse;

#[macro_use]
extern crate serde;

#[cfg(feature = "proto_inplace")]
pub mod generated {
    #[path = "algebra.rs"]
    pub mod algebra;
    #[path = "common.rs"]
    pub mod common;
    #[path = "job_service.rs"]
    pub mod job_service;
    #[path = "results.rs"]
    pub mod results;
    #[path = "schema.rs"]
    pub mod schema;
}

#[cfg(not(feature = "proto_inplace"))]
pub mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }
    pub mod algebra {
        tonic::include_proto!("algebra");
    }
    pub mod results {
        tonic::include_proto!("results");
    }
    pub mod schema {
        tonic::include_proto!("schema");
    }
    pub mod job_service {
        tonic::include_proto!("job_service");
    }
}

pub type KeyId = i32;
pub const SPLITTER: &'static str = ".";
pub const VAR_PREFIX: &'static str = "@";

/// Refer to a key of a relation or a graph element, by either a string-type name or an identifier
#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub enum NameOrId {
    Str(String),
    Id(KeyId),
}

impl Default for NameOrId {
    fn default() -> Self {
        Self::Str("".to_string())
    }
}

impl NameOrId {
    pub fn as_object(&self) -> Object {
        match self {
            NameOrId::Str(s) => s.to_string().into(),
            NameOrId::Id(id) => (*id as i32).into(),
        }
    }

    pub fn as_borrow_object(&self) -> BorrowObject {
        match self {
            NameOrId::Str(s) => BorrowObject::String(s.as_str()),
            NameOrId::Id(id) => (*id as i32).into(),
        }
    }
}

impl From<KeyId> for NameOrId {
    fn from(id: KeyId) -> Self {
        Self::Id(id)
    }
}

impl From<String> for NameOrId {
    fn from(str: String) -> Self {
        Self::Str(str)
    }
}

impl From<&str> for NameOrId {
    fn from(str: &str) -> Self {
        Self::Str(str.to_string())
    }
}

impl Encode for NameOrId {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            NameOrId::Id(id) => {
                writer.write_u8(0)?;
                writer.write_i32(*id)?;
            }
            NameOrId::Str(str) => {
                writer.write_u8(1)?;
                str.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for NameOrId {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let id = reader.read_i32()?;
                Ok(NameOrId::Id(id))
            }
            1 => {
                let str = <String>::read_from(reader)?;
                Ok(NameOrId::Str(str))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl TryFrom<common_pb::NameOrId> for NameOrId {
    type Error = ParsePbError;

    fn try_from(t: common_pb::NameOrId) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use common_pb::name_or_id::Item;

        if let Some(item) = t.item {
            match item {
                Item::Name(name) => Ok(NameOrId::Str(name)),
                Item::Id(id) => {
                    if id < 0 {
                        Err(ParsePbError::from("key id must be positive number"))
                    } else {
                        Ok(NameOrId::Id(id as KeyId))
                    }
                }
            }
        } else {
            Err(ParsePbError::from("empty content provided"))
        }
    }
}

impl TryFrom<common_pb::NameOrId> for KeyId {
    type Error = ParsePbError;

    fn try_from(t: common_pb::NameOrId) -> ParsePbResult<Self>
    where
        Self: Sized,
    {
        use common_pb::name_or_id::Item;

        if let Some(item) = t.item {
            match item {
                Item::Name(_) => Err(ParsePbError::from("key must be a number")),
                Item::Id(id) => {
                    if id < 0 {
                        Err(ParsePbError::from("key id must be positive number"))
                    } else {
                        Ok(id as KeyId)
                    }
                }
            }
        } else {
            Err(ParsePbError::from("empty content provided"))
        }
    }
}

impl From<NameOrId> for common_pb::NameOrId {
    fn from(tag: NameOrId) -> Self {
        let name_or_id = match tag {
            NameOrId::Str(name) => common_pb::name_or_id::Item::Name(name),
            NameOrId::Id(id) => common_pb::name_or_id::Item::Id(id),
        };
        common_pb::NameOrId { item: Some(name_or_id) }
    }
}

impl From<NameOrId> for Object {
    fn from(tag: NameOrId) -> Self {
        match tag {
            NameOrId::Str(name) => Object::from(name),
            NameOrId::Id(id) => Object::from(id),
        }
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

impl From<pb::Limit> for pb::logical_plan::Operator {
    fn from(opr: pb::Limit) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Limit(opr)) }
    }
}

impl From<pb::Auxilia> for pb::logical_plan::Operator {
    fn from(opr: pb::Auxilia) -> Self {
        pb::logical_plan::Operator { opr: Some(pb::logical_plan::operator::Opr::Auxilia(opr)) }
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

impl Encode for result_pb::Results {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        let mut bytes = vec![];
        self.encode_raw(&mut bytes);
        writer.write_u32(bytes.len() as u32)?;
        writer.write_all(bytes.as_slice())?;
        Ok(())
    }
}

impl Decode for result_pb::Results {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let len = reader.read_u32()? as usize;
        let mut buffer = Vec::with_capacity(len);
        reader.read_exact(&mut buffer)?;
        result_pb::Results::decode(buffer.as_slice())
            .map_err(|_e| std::io::Error::new(std::io::ErrorKind::Other, "decoding result_pb failed!"))
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
