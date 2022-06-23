//
//! Copyright 2021 Alibaba Group Holding Limited.
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

pub mod accum;
pub mod filter;
pub mod flatmap;
pub mod group;
pub mod join;
pub mod keyed;
pub mod map;
pub mod shuffle;
pub mod sink;
pub mod sort;
pub mod source;
pub mod subtask;

use std::convert::TryFrom;
use std::sync::Arc;

use dyn_type::Object;
use graph_proxy::apis::{Details, Element, GraphElement, PropKey};
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::FnExecError;
use crate::process::record::{CommonObject, Entry, Record, RecordElement};

#[derive(Clone, Debug, Default)]
pub struct TagKey {
    tag: Option<KeyId>,
    key: Option<PropKey>,
}

impl TagKey {
    /// This is for key generation, which generate the key of the input Record according to the tag_key field
    pub fn get_arc_entry(&self, input: &Record) -> Result<Arc<Entry>, FnExecError> {
        let entry = input
            .get(self.tag)
            .ok_or(FnExecError::get_tag_error(&format!(
                "Get tag {:?} failed since it refers to an empty entry",
                self.tag
            )))?
            .clone();
        if let Some(prop_key) = self.key.as_ref() {
            let prop = self.get_key(entry, prop_key)?;
            Ok(Arc::new(prop))
        } else {
            Ok(entry)
        }
    }

    /// This is for accum, which get the entry of the input Record according to the tag_key field
    pub fn get_entry(&self, input: &Record) -> Result<Entry, FnExecError> {
        let entry = input
            .get(self.tag)
            .ok_or(FnExecError::get_tag_error(&format!(
                "Get tag {:?} failed since it refers to an empty entry",
                self.tag
            )))?
            .clone();
        if let Some(prop_key) = self.key.as_ref() {
            Ok(self.get_key(entry, prop_key)?)
        } else {
            Ok(entry.as_ref().clone())
        }
    }

    fn get_key(&self, entry: Arc<Entry>, prop_key: &PropKey) -> Result<Entry, FnExecError> {
        if let Entry::Element(RecordElement::OnGraph(element)) = entry.as_ref() {
            let prop_obj = match prop_key {
                PropKey::Id => element.id().into(),
                PropKey::Label => element
                    .label()
                    .cloned()
                    .map(|label| match label {
                        NameOrId::Str(str) => str.into(),
                        NameOrId::Id(id) => id.into(),
                    })
                    .unwrap_or(Object::None),
                PropKey::Len => (element.len() as u64).into(),
                PropKey::All => {
                    let details = element
                        .details()
                        .ok_or(FnExecError::get_tag_error(
                            "Get key failed since get details from a graph element failed",
                        ))?;

                    if let Some(properties) = details.get_all_properties() {
                        properties
                            .into_iter()
                            .map(|(key, value)| {
                                let obj_key: Object = match key {
                                    NameOrId::Str(str) => str.into(),
                                    NameOrId::Id(id) => id.into(),
                                };
                                (obj_key, value)
                            })
                            .collect::<Vec<(Object, Object)>>()
                            .into()
                    } else {
                        Object::None
                    }
                }
                PropKey::Key(key) => {
                    let details = element
                        .details()
                        .ok_or(FnExecError::get_tag_error(
                            "Get key failed since get details from a graph element failed",
                        ))?;
                    if let Some(properties) = details.get_property(key) {
                        properties
                            .try_to_owned()
                            .ok_or(FnExecError::UnExpectedData(
                                "unable to own the `BorrowObject`".to_string(),
                            ))?
                    } else {
                        Object::None
                    }
                }
            };

            match prop_obj {
                Object::None => Ok(CommonObject::None.into()),
                _ => Ok(CommonObject::Prop(prop_obj).into()),
            }
        } else {
            Err(FnExecError::get_tag_error(
                "Get key failed when attempt to get prop_key from a non-graph element",
            ))
        }
    }
}

impl TryFrom<common_pb::Variable> for TagKey {
    type Error = ParsePbError;

    fn try_from(v: common_pb::Variable) -> Result<Self, Self::Error> {
        let tag = if let Some(tag) = v.tag { Some(KeyId::try_from(tag)?) } else { None };
        let prop = if let Some(prop) = v.property { Some(PropKey::try_from(prop)?) } else { None };
        Ok(TagKey { tag, key: prop })
    }
}

impl Encode for TagKey {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match (&self.tag, &self.key) {
            (Some(tag), Some(key)) => {
                writer.write_u8(0)?;
                tag.write_to(writer)?;
                key.write_to(writer)?;
            }
            (Some(tag), None) => {
                writer.write_u8(1)?;
                tag.write_to(writer)?;
            }
            (None, Some(key)) => {
                writer.write_u8(2)?;
                key.write_to(writer)?;
            }
            (None, None) => {
                writer.write_u8(3)?;
            }
        }
        Ok(())
    }
}

impl Decode for TagKey {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let tag = <KeyId>::read_from(reader)?;
                let key = <PropKey>::read_from(reader)?;
                Ok(TagKey { tag: Some(tag), key: Some(key) })
            }
            1 => {
                let tag = <KeyId>::read_from(reader)?;
                Ok(TagKey { tag: Some(tag), key: None })
            }
            2 => {
                let key = <PropKey>::read_from(reader)?;
                Ok(TagKey { tag: None, key: Some(key) })
            }
            3 => Ok(TagKey { tag: None, key: None }),
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;

    use dyn_type::Object;
    use graph_proxy::apis::{DefaultDetails, DynDetails, GraphElement, Vertex};
    use ir_common::KeyId;

    use super::*;
    use crate::process::record::RecordElement;

    pub const TAG_A: KeyId = 0;
    pub const TAG_B: KeyId = 1;
    pub const TAG_C: KeyId = 2;
    pub const TAG_D: KeyId = 3;
    pub const TAG_E: KeyId = 4;

    pub fn init_vertex1() -> Vertex {
        let map1: HashMap<NameOrId, Object> = vec![
            ("id".into(), object!(1)),
            ("age".into(), object!(29)),
            ("name".into(), object!("marko")),
            ("code".into(), object!("11051")),
        ]
        .into_iter()
        .collect();
        Vertex::new(1, Some("person".into()), DynDetails::new(DefaultDetails::new(map1)))
    }

    pub fn init_vertex2() -> Vertex {
        let map2: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(2)), ("age".into(), object!(27)), ("name".into(), object!("vadas"))]
                .into_iter()
                .collect();
        Vertex::new(2, Some("person".into()), DynDetails::new(DefaultDetails::new(map2)))
    }

    fn init_record() -> Record {
        let vertex1 = init_vertex1();
        let vertex2 = init_vertex2();
        let object3 = CommonObject::Count(10);

        let mut record = Record::new(vertex1, None);
        record.append(vertex2, Some((0 as KeyId).into()));
        record.append(object3, Some((1 as KeyId).into()));
        record
    }

    pub fn init_source() -> Vec<Record> {
        let v1 = init_vertex1();
        let v2 = init_vertex2();
        let r1 = Record::new(v1, None);
        let r2 = Record::new(v2, None);
        vec![r1, r2]
    }

    pub fn init_source_with_tag() -> Vec<Record> {
        let v1 = init_vertex1();
        let v2 = init_vertex2();
        let r1 = Record::new(v1, Some(TAG_A.into()));
        let r2 = Record::new(v2, Some(TAG_A.into()));
        vec![r1, r2]
    }

    pub fn init_source_with_multi_tags() -> Vec<Record> {
        let v1 = init_vertex1();
        let v2 = init_vertex2();
        let mut r1 = Record::new(v1, Some(TAG_A.into()));
        r1.append(v2, Some(TAG_B.into()));
        vec![r1]
    }

    pub fn to_var_pb(tag: Option<NameOrId>, key: Option<NameOrId>) -> common_pb::Variable {
        common_pb::Variable {
            tag: tag.map(|t| t.into()),
            property: key
                .map(|k| common_pb::Property { item: Some(common_pb::property::Item::Key(k.into())) }),
        }
    }

    pub fn to_expr_var_pb(tag: Option<NameOrId>, key: Option<NameOrId>) -> common_pb::Expression {
        common_pb::Expression {
            operators: vec![common_pb::ExprOpr {
                item: Some(common_pb::expr_opr::Item::Var(to_var_pb(tag, key))),
            }],
        }
    }

    pub fn to_expr_vars_pb(
        tag_keys: Vec<(Option<NameOrId>, Option<NameOrId>)>, is_map: bool,
    ) -> common_pb::Expression {
        let vars = tag_keys
            .into_iter()
            .map(|(tag, key)| to_var_pb(tag, key))
            .collect();
        common_pb::Expression {
            operators: vec![common_pb::ExprOpr {
                item: if is_map {
                    Some(common_pb::expr_opr::Item::VarMap(common_pb::VariableKeys { keys: vars }))
                } else {
                    Some(common_pb::expr_opr::Item::Vars(common_pb::VariableKeys { keys: vars }))
                },
            }],
        }
    }

    #[test]
    // None tag refers to the last appended entry;
    fn test_get_none_tag_entry() {
        let tag_key = TagKey { tag: None, key: None };
        let record = init_record();
        let expected = CommonObject::Count(10).into();
        let entry = tag_key.get_arc_entry(&record).unwrap();
        assert_eq!(entry.as_ref().clone(), expected)
    }

    #[test]
    fn test_get_tag_entry() {
        let tag_key = TagKey { tag: Some((0 as KeyId).into()), key: None };
        let expected = init_vertex2();
        let record = init_record();
        let entry = tag_key.get_arc_entry(&record).unwrap();
        if let Some(element) = entry.as_graph_vertex() {
            assert_eq!(element.id(), expected.id());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_get_tag_key_entry() {
        let tag_key = TagKey { tag: Some((0 as KeyId).into()), key: Some(PropKey::Key("age".into())) };
        let expected = 27;
        let record = init_record();
        let entry = tag_key.get_arc_entry(&record).unwrap();
        match entry.as_ref() {
            Entry::Element(RecordElement::OffGraph(CommonObject::Prop(obj))) => {
                assert_eq!(obj.clone(), object!(expected));
            }
            _ => {
                assert!(false);
            }
        }
    }
}
