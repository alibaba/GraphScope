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

use crate::error::{ParsePbError, ParsePbResult};
use crate::generated::algebra as pb;
use crate::generated::common as common_pb;
use dyn_type::{BorrowObject, Object};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::convert::TryFrom;
use std::io;

pub mod error;

#[cfg(feature = "proto_inplace")]
pub mod generated {
    #[path = "algebra.rs"]
    pub mod algebra;
    #[path = "common.rs"]
    pub mod common;
}

#[cfg(not(feature = "proto_inplace"))]
mod generated {
    pub mod common {
        tonic::include_proto!("common");
    }
    pub mod algebra {
        tonic::include_proto!("algebra");
    }
}

pub type KeyId = i32;
pub const SPLITTER: &'static str = ".";
pub const VAR_PREFIX: &'static str = "@";

/// Refer to a key of a relation or a graph element, by either a string-type name or an identifier
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum NameOrId {
    Str(String),
    Id(KeyId),
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

impl From<common_pb::Const> for common_pb::ExprOpr {
    fn from(const_val: common_pb::Const) -> Self {
        common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Const(const_val)),
        }
    }
}

impl From<common_pb::Variable> for common_pb::ExprOpr {
    fn from(var: common_pb::Variable) -> Self {
        common_pb::ExprOpr {
            item: Some(common_pb::expr_opr::Item::Var(var)),
        }
    }
}

impl From<bool> for common_pb::Value {
    fn from(b: bool) -> Self {
        common_pb::Value {
            item: Some(common_pb::value::Item::Boolean(b)),
        }
    }
}

impl From<f64> for common_pb::Value {
    fn from(f: f64) -> Self {
        common_pb::Value {
            item: Some(common_pb::value::Item::F64(f)),
        }
    }
}

impl From<i32> for common_pb::Value {
    fn from(i: i32) -> Self {
        common_pb::Value {
            item: Some(common_pb::value::Item::I32(i)),
        }
    }
}

impl From<i64> for common_pb::Value {
    fn from(i: i64) -> Self {
        common_pb::Value {
            item: Some(common_pb::value::Item::I64(i)),
        }
    }
}

impl From<String> for common_pb::Value {
    fn from(s: String) -> Self {
        common_pb::Value {
            item: Some(common_pb::value::Item::Str(s)),
        }
    }
}

impl From<i32> for common_pb::NameOrId {
    fn from(i: i32) -> Self {
        common_pb::NameOrId {
            item: Some(common_pb::name_or_id::Item::Id(i)),
        }
    }
}

impl From<String> for common_pb::NameOrId {
    fn from(str: String) -> Self {
        common_pb::NameOrId {
            item: Some(common_pb::name_or_id::Item::Name(str)),
        }
    }
}

const ID_KEY: &'static str = "ID";
const LABEL_KEY: &'static str = "LABEL";

impl From<String> for common_pb::Property {
    fn from(str: String) -> Self {
        if str == ID_KEY {
            common_pb::Property {
                item: Some(common_pb::property::Item::Id(common_pb::IdKey {})),
            }
        } else if str == LABEL_KEY {
            common_pb::Property {
                item: Some(common_pb::property::Item::Label(common_pb::LabelKey {})),
            }
        } else {
            common_pb::Property {
                item: Some(common_pb::property::Item::Key(str.into())),
            }
        }
    }
}

fn str_as_tag(str: String) -> Option<common_pb::NameOrId> {
    if !str.is_empty() {
        Some(if let Ok(str_int) = str.parse::<i32>() {
            str_int.into()
        } else {
            str.into()
        })
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
            let tag: Option<common_pb::NameOrId> = if let Some(first) = splitter.next() {
                str_as_tag(first.to_string())
            } else {
                None
            };
            let property: Option<common_pb::Property> = if let Some(second) = splitter.next() {
                Some(second.to_string().into())
            } else {
                None
            };
            common_pb::Variable { tag, property }
        }
    }
}

impl common_pb::Const {
    #[allow(dead_code)]
    pub fn into_object(self) -> ParsePbResult<Option<Object>> {
        use common_pb::value::Item::*;
        if let Some(val) = &self.value {
            if let Some(item) = val.item.as_ref() {
                return match item {
                    Boolean(b) => Ok(Some((*b).into())),
                    I32(i) => Ok(Some((*i).into())),
                    I64(i) => Ok(Some((*i).into())),
                    F64(f) => Ok(Some((*f).into())),
                    Str(s) => Ok(Some(s.clone().into())),
                    Blob(blob) => Ok(Some(blob.clone().into())),
                    None(_) => Ok(Option::None),
                    I32Array(_) | I64Array(_) | F64Array(_) | StrArray(_) => {
                        Err(ParsePbError::from("the const values of `I32Array`, `I64Array`, `F64Array`, `StrArray` are unsupported"))
                    }
                };
            }
        }

        Err(ParsePbError::from("empty value provided"))
    }
}

impl From<pb::Project> for pb::logical_plan::Operator {
    fn from(opr: pb::Project) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Project(opr)),
        }
    }
}

impl From<pb::Select> for pb::logical_plan::Operator {
    fn from(opr: pb::Select) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Select(opr)),
        }
    }
}

impl From<pb::Join> for pb::logical_plan::Operator {
    fn from(opr: pb::Join) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Join(opr)),
        }
    }
}

impl From<pb::Union> for pb::logical_plan::Operator {
    fn from(opr: pb::Union) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Union(opr)),
        }
    }
}

impl From<pb::GroupBy> for pb::logical_plan::Operator {
    fn from(opr: pb::GroupBy) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::GroupBy(opr)),
        }
    }
}

impl From<pb::OrderBy> for pb::logical_plan::Operator {
    fn from(opr: pb::OrderBy) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::OrderBy(opr)),
        }
    }
}

impl From<pb::Dedup> for pb::logical_plan::Operator {
    fn from(opr: pb::Dedup) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Dedup(opr)),
        }
    }
}

impl From<pb::Unfold> for pb::logical_plan::Operator {
    fn from(opr: pb::Unfold) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Unfold(opr)),
        }
    }
}

impl From<pb::Apply> for pb::logical_plan::Operator {
    fn from(opr: pb::Apply) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Apply(opr)),
        }
    }
}

impl From<pb::SegmentApply> for pb::logical_plan::Operator {
    fn from(opr: pb::SegmentApply) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::SegApply(opr)),
        }
    }
}

impl From<pb::Scan> for pb::logical_plan::Operator {
    fn from(opr: pb::Scan) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Scan(opr)),
        }
    }
}

impl From<pb::IndexedScan> for pb::logical_plan::Operator {
    fn from(opr: pb::IndexedScan) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::IndexedScan(opr)),
        }
    }
}

impl From<pb::Limit> for pb::logical_plan::Operator {
    fn from(opr: pb::Limit) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Limit(opr)),
        }
    }
}

impl From<pb::GetDetails> for pb::logical_plan::Operator {
    fn from(opr: pb::GetDetails) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Details(opr)),
        }
    }
}

impl From<pb::EdgeExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::EdgeExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Edge(opr)),
        }
    }
}

impl From<pb::PathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::PathExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Path(opr)),
        }
    }
}

impl From<pb::ShortestPathExpand> for pb::logical_plan::Operator {
    fn from(opr: pb::ShortestPathExpand) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::ShortestPath(opr)),
        }
    }
}

impl From<pb::GetV> for pb::logical_plan::Operator {
    fn from(opr: pb::GetV) -> Self {
        pb::logical_plan::Operator {
            opr: Some(pb::logical_plan::operator::Opr::Vertex(opr)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_str_to_variable() {
        let case1 = "@1";
        assert_eq!(
            common_pb::Variable {
                tag: Some(common_pb::NameOrId::from(1)),
                property: None
            },
            common_pb::Variable::from(case1.to_string())
        );

        let case2 = "@a";
        assert_eq!(
            common_pb::Variable {
                tag: Some(common_pb::NameOrId::from("a".to_string())),
                property: None
            },
            common_pb::Variable::from(case2.to_string())
        );

        let case3 = "@1.ID";
        assert_eq!(
            common_pb::Variable {
                tag: Some(common_pb::NameOrId::from(1)),
                property: Some(common_pb::Property {
                    item: Some(common_pb::property::Item::Id(common_pb::IdKey {}))
                })
            },
            common_pb::Variable::from(case3.to_string())
        );

        let case4 = "@1.LABEL";
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
            common_pb::Variable {
                tag: None,
                property: None
            },
            common_pb::Variable::from(case7.to_string())
        );
    }
}
