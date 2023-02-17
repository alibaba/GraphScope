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

use dyn_type::Object;
use graph_proxy::apis::{get_graph, Details, Element, GraphElement, PropKey, QueryParams};
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::error::{FnExecError, FnExecResult};
use crate::process::entry::{DynEntry, Entry};
use crate::process::record::Record;

#[derive(Clone, Debug, Default)]
pub struct TagKey {
    tag: Option<KeyId>,
    key: Option<PropKey>,
}

impl TagKey {
    /// This is for key generation, which generate the key of the input Record according to the tag_key field
    pub fn get_arc_entry(&self, input: &Record) -> FnExecResult<DynEntry> {
        if let Some(entry) = input.get(self.tag) {
            if let Some(prop_key) = self.key.as_ref() {
                let prop = self.get_key(entry, prop_key)?;
                Ok(prop)
            } else {
                Ok(entry.clone())
            }
        } else {
            Ok(DynEntry::new(Object::None))
        }
    }

    fn get_key(&self, entry: &DynEntry, prop_key: &PropKey) -> FnExecResult<DynEntry> {
        if let PropKey::Len = prop_key {
            let obj: Object = (entry.len() as u64).into();
            Ok(DynEntry::new(obj))
        } else {
            if let Some(element) = entry.as_graph_element() {
                let prop_obj = match prop_key {
                    PropKey::Id => element.id().into(),
                    PropKey::Label => element
                        .label()
                        .map(|label| label.into())
                        .unwrap_or(Object::None),
                    PropKey::Len => unreachable!(),
                    PropKey::All => {
                        let details = element
                            .details()
                            .ok_or(FnExecError::unexpected_data_error(&format!(
                                "Get `PropKey::All` on {:?}",
                                entry,
                            )))?;
                        let properties = if details.is_empty() {
                            debug!("details should not be empty!!");
                            assert!(false);
                            // TODO: canbe edge
                            get_graph()
                                .unwrap()
                                .get_vertex(&vec![element.id()], &QueryParams::default())
                                .unwrap()
                                .next()
                                .unwrap()
                                .details()
                                .unwrap()
                                .get_all_properties()
                        } else {
                            details.get_all_properties()
                        };

                        if let Some(properties) = properties {
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
                            .ok_or(FnExecError::unexpected_data_error(&format!(
                                "Get `PropKey::Key` of {:?} on {:?}",
                                key, entry,
                            )))?;

                        if details.is_empty() {
                            debug!("details should not be empty!!111");
                            assert!(false);
                            if let Some(vertex) = entry.as_vertex() {
                                let vertex = get_graph()
                                    .unwrap()
                                    .get_vertex(&vec![vertex.id()], &QueryParams::default())
                                    .unwrap()
                                    .next()
                                    .ok_or(FnExecError::unexpected_data_error(&format!(
                                        "Get Vertex failed in get_key {:?} when get property {:?}",
                                        element.id(),
                                        key
                                    )))?;
                                if let Some(properties) = vertex.details().unwrap().get_property(key) {
                                    let prop_obj = properties.try_to_owned().ok_or(
                                        FnExecError::unexpected_data_error(
                                            "unable to own the `BorrowObject`",
                                        ),
                                    )?;
                                    return Ok(DynEntry::new(prop_obj));
                                } else {
                                    Object::None
                                }
                            } else {
                                let edge = get_graph()
                                    .unwrap()
                                    .get_edge(&vec![element.id()], &QueryParams::default())
                                    .unwrap()
                                    .next()
                                    .ok_or(FnExecError::unexpected_data_error(&format!(
                                        "Get Edge failed in get_key {:?} when get property {:?}",
                                        element.id(),
                                        key
                                    )))?;

                                if let Some(properties) = edge.details().unwrap().get_property(key) {
                                    let prop_obj = properties.try_to_owned().ok_or(
                                        FnExecError::unexpected_data_error(
                                            "unable to own the `BorrowObject`",
                                        ),
                                    )?;
                                    return Ok(DynEntry::new(prop_obj));
                                } else {
                                    Object::None
                                }
                            }
                        } else if details.is_default() {
                            if let Some(properties) = details.get_property(key) {
                                properties
                                    .try_to_owned()
                                    .ok_or(FnExecError::unexpected_data_error(
                                        "unable to own the `BorrowObject`",
                                    ))?
                            } else if let Some(properties) = get_graph()
                                .unwrap()
                                .get_vertex(&vec![element.id()], &QueryParams::default())
                                .unwrap()
                                .next()
                                .unwrap()
                                .details()
                                .unwrap()
                                .get_property(key)
                            {
                                let prop_obj =
                                    properties
                                        .try_to_owned()
                                        .ok_or(FnExecError::unexpected_data_error(
                                            "unable to own the `BorrowObject`",
                                        ))?;
                                return Ok(DynEntry::new(prop_obj));
                            } else {
                                Object::None
                            }
                        } else if let Some(properties) = details.get_property(key) {
                            properties
                                .try_to_owned()
                                .ok_or(FnExecError::unexpected_data_error(
                                    "unable to own the `BorrowObject`",
                                ))?
                        } else {
                            Object::None
                        }
                    }
                };

                Ok(DynEntry::new(prop_obj))
            } else {
                Err(FnExecError::unexpected_data_error(&format!(
                    "
                Get key failed since get details from a none-graph element {:?} ",
                    entry
                )))
            }
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
    use ahash::HashMap;
    use dyn_type::Object;
    use graph_proxy::apis::{DynDetails, GraphElement, Vertex};
    use ir_common::{KeyId, LabelId};

    use super::*;
    use crate::process::entry::Entry;

    pub const TAG_A: KeyId = 0;
    pub const TAG_B: KeyId = 1;
    pub const TAG_C: KeyId = 2;
    pub const TAG_D: KeyId = 3;
    pub const TAG_E: KeyId = 4;

    pub const PERSON_LABEL: LabelId = 0;

    pub fn init_vertex1() -> Vertex {
        let map1: HashMap<NameOrId, Object> = vec![
            ("id".into(), object!(1)),
            ("age".into(), object!(29)),
            ("name".into(), object!("marko")),
            ("code".into(), object!("11051")),
        ]
        .into_iter()
        .collect();
        Vertex::new(1, Some(PERSON_LABEL), DynDetails::new(map1))
    }

    pub fn init_vertex2() -> Vertex {
        let map2: HashMap<NameOrId, Object> =
            vec![("id".into(), object!(2)), ("age".into(), object!(27)), ("name".into(), object!("vadas"))]
                .into_iter()
                .collect();
        Vertex::new(2, Some(PERSON_LABEL), DynDetails::new(map2))
    }

    fn init_record() -> Record {
        let vertex1 = init_vertex1();
        let vertex2 = init_vertex2();
        let object3 = object!(10);

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
        let expected = object!(10);
        let entry = tag_key.get_arc_entry(&record).unwrap();
        assert_eq!(entry.as_object().unwrap().clone(), expected)
    }

    #[test]
    fn test_get_tag_entry() {
        let tag_key = TagKey { tag: Some((0 as KeyId).into()), key: None };
        let expected = init_vertex2();
        let record = init_record();
        let entry = tag_key.get_arc_entry(&record).unwrap();
        if let Some(element) = entry.as_vertex() {
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
        let entry = tag_key
            .get_arc_entry(&record)
            .unwrap()
            .as_object()
            .unwrap()
            .clone();

        assert_eq!(entry, object!(expected));
    }
}
