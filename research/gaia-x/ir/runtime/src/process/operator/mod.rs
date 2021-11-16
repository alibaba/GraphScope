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

pub(crate) mod filter;
pub(crate) mod flatmap;
pub(crate) mod join;
pub(crate) mod keyed;
pub(crate) mod map;
pub(crate) mod shuffle;
pub(crate) mod sink;
pub(crate) mod sort;
pub(crate) mod source;

use crate::graph::element::Element;
use crate::graph::property::{Details, PropKey};
use crate::process::record::{Entry, ObjectElement, Record, RecordElement};
use dyn_type::BorrowObject;
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::convert::TryFrom;

#[derive(Debug)]
pub struct KeyedError {
    desc: String,
}

impl std::fmt::Display for KeyedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Get key failed: {}", self.desc)
    }
}

impl std::error::Error for KeyedError {}

impl From<String> for KeyedError {
    fn from(desc: String) -> Self {
        KeyedError { desc }
    }
}

impl From<&str> for KeyedError {
    fn from(desc: &str) -> Self {
        desc.to_string().into()
    }
}

#[derive(Clone, Debug, Default)]
pub struct TagKey {
    tag: Option<NameOrId>,
    key: Option<PropKey>,
}

impl<'a> TagKey {
    /// This is for KeySelector, which generate the key of the input Record according to the tag_key field
    pub fn get_entry(&self, input: &Record) -> Result<Entry, KeyedError> {
        let entry = input
            .get(self.tag.as_ref())
            .ok_or(KeyedError::from(
                "Get tag failed since it refers to an empty entry",
            ))?
            .clone();
        if let Some(key) = self.key.as_ref() {
            match entry {
                Entry::Element(RecordElement::OnGraph(element)) => {
                    let details = element.details().ok_or(KeyedError::from(
                        "Get key failed since get prop_key from a graph element failed",
                    ))?;
                    let properties = details
                        .get(key)
                        .ok_or(KeyedError::from(
                            "Get key failed since get prop_key from a graph element failed",
                        ))?
                        .into();
                    Ok(ObjectElement::Prop(properties).into())
                }
                _ => Err(KeyedError::from(
                    "Get key failed when attempt to get prop_key from a non-graph element",
                )),
            }
        } else {
            Ok(entry)
        }
    }

    /// This is for Order, which generate the comparable object (by ref) of the input Record according to the tag_key field
    pub fn get_obj(&self, input: &'a Record) -> Result<BorrowObject<'a>, KeyedError> {
        let entry = input.get(self.tag.as_ref()).ok_or(KeyedError::from(
            "Get tag failed since it refers to an empty entry",
        ))?;
        if let Some(key) = self.key.as_ref() {
            match entry {
                Entry::Element(RecordElement::OnGraph(element)) => {
                    if let Some(details) = element.details() {
                        Ok(details.get(key).ok_or(KeyedError::from(
                            "Get key failed since get prop_key from a graph element failed",
                        ))?)
                    } else {
                        Ok(element.as_borrow_object())
                    }
                }
                _ => Err(KeyedError::from(
                    "Get key failed when attempt to get prop_key from a non-graph element",
                )),
            }
        } else {
            match entry {
                // TODO(bingqing): In gremlin, it regards OnGraph element as not Comparable. For now we compare its id by default.
                Entry::Element(RecordElement::OnGraph(element)) => Ok(element.as_borrow_object()),
                Entry::Element(RecordElement::OutGraph(object)) => match object {
                    ObjectElement::None => Err(KeyedError::from(
                        "Get key failed since it refers to an empty object element",
                    )),
                    ObjectElement::Prop(prop) => Ok(prop.as_borrow()),
                    ObjectElement::Count(cnt) => Ok((*cnt).into()),
                    ObjectElement::Agg(agg) => Ok(agg.as_borrow()),
                },
                Entry::Collection(_) => Err(KeyedError::from(
                    "Get key failed since it refers to a Collection type entry",
                )),
            }
        }
    }
}

impl TryFrom<common_pb::Variable> for TagKey {
    type Error = ParsePbError;

    fn try_from(v: common_pb::Variable) -> Result<Self, Self::Error> {
        let tag = if let Some(tag) = v.tag {
            Some(NameOrId::try_from(tag)?)
        } else {
            None
        };
        let prop = if let Some(prop) = v.property {
            Some(PropKey::try_from(prop)?)
        } else {
            None
        };
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
                let tag = <NameOrId>::read_from(reader)?;
                let key = <PropKey>::read_from(reader)?;
                Ok(TagKey {
                    tag: Some(tag),
                    key: Some(key),
                })
            }
            1 => {
                let tag = <NameOrId>::read_from(reader)?;
                Ok(TagKey {
                    tag: Some(tag),
                    key: None,
                })
            }
            2 => {
                let key = <PropKey>::read_from(reader)?;
                Ok(TagKey {
                    tag: None,
                    key: Some(key),
                })
            }
            3 => Ok(TagKey {
                tag: None,
                key: None,
            }),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unreachable",
            )),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::graph::element::Vertex;
    use crate::graph::property::{DefaultDetails, DynDetails};
    use dyn_type::Object;
    use std::collections::HashMap;

    fn init_vertex1() -> Vertex {
        let map1: HashMap<NameOrId, Object> = vec![
            (NameOrId::from("id".to_string()), 1.into()),
            (NameOrId::from("age".to_string()), 29.into()),
            (
                NameOrId::from("name".to_string()),
                "marko".to_string().into(),
            ),
        ]
        .into_iter()
        .collect();

        Vertex::new(DynDetails::new(DefaultDetails::with_property(
            1,
            NameOrId::from("person".to_string()),
            map1,
        )))
    }

    fn init_vertex2() -> Vertex {
        let map2: HashMap<NameOrId, Object> = vec![
            (NameOrId::from("id".to_string()), 2.into()),
            (NameOrId::from("age".to_string()), 27.into()),
            (
                NameOrId::from("name".to_string()),
                "vadas".to_string().into(),
            ),
        ]
        .into_iter()
        .collect();

        Vertex::new(DynDetails::new(DefaultDetails::with_property(
            2,
            NameOrId::from("person".to_string()),
            map2,
        )))
    }

    fn init_record() -> Record {
        let vertex1 = init_vertex1();
        let vertex2 = init_vertex2();
        let object3 = ObjectElement::Count(10);

        let mut record = Record::new(vertex1, None);
        record.append(vertex2, Some(NameOrId::Id(0)));
        record.append(object3, Some(NameOrId::Id(1)));
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
        let r1 = Record::new(v1, Some(NameOrId::from("a".to_string())));
        let r2 = Record::new(v2, Some(NameOrId::from("a".to_string())));
        vec![r1, r2]
    }

    #[test]
    fn test_get_none_tag_entry() {
        let tag_key = TagKey {
            tag: None,
            key: None,
        };
        let expected = init_vertex1();
        let record = init_record();
        let entry = tag_key.get_entry(&record).unwrap();
        match entry {
            Entry::Element(RecordElement::OnGraph(element)) => {
                assert_eq!(element.id(), expected.id());
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_get_none_tag_obj() {
        let tag_key = TagKey {
            tag: None,
            key: None,
        };
        let expected = init_vertex1();
        let record = init_record();
        let obj = tag_key.get_obj(&record).unwrap();
        assert_eq!(obj, expected.as_borrow_object());
    }

    #[test]
    fn test_get_tag_entry() {
        let tag_key = TagKey {
            tag: Some(NameOrId::Id(0)),
            key: None,
        };
        let expected = init_vertex2();
        let record = init_record();
        let entry = tag_key.get_entry(&record).unwrap();
        match entry {
            Entry::Element(RecordElement::OnGraph(element)) => {
                assert_eq!(element.id(), expected.id());
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_get_tag_obj() {
        let tag_key = TagKey {
            tag: Some(NameOrId::Id(1)),
            key: None,
        };
        let expected = 10.into();
        let record = init_record();
        let obj = tag_key.get_obj(&record).unwrap();
        assert_eq!(obj, expected)
    }

    #[test]
    fn test_get_tag_key_entry() {
        let tag_key = TagKey {
            tag: Some(NameOrId::Id(0)),
            key: Some(PropKey::Key(NameOrId::Str("age".to_string()))),
        };
        let expected = 27;
        let record = init_record();
        let entry = tag_key.get_entry(&record).unwrap();
        match entry {
            Entry::Element(RecordElement::OutGraph(ObjectElement::Prop(obj))) => {
                assert_eq!(obj, expected.into());
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_get_tag_key_obj() {
        let tag_key = TagKey {
            tag: Some(NameOrId::Id(0)),
            key: Some(PropKey::Key(NameOrId::Str("name".to_string()))),
        };
        let expected = "vadas";
        let record = init_record();
        let obj = tag_key.get_obj(&record).unwrap();
        assert_eq!(obj, expected.into())
    }
}
