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
pub(crate) mod map;
pub(crate) mod shuffle;
pub(crate) mod sink;
pub(crate) mod source;

use crate::graph::element::Element;
use crate::graph::property::{Details, PropKey};
use crate::process::record::{Entry, ObjectElement, Record, RecordElement};
use dyn_type::BorrowObject;
use ir_common::error::ParsePbError;
use ir_common::generated::common as common_pb;
use ir_common::NameOrId;
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
    /// This is for Accum, which take the key entry the input Record according to the tag_key field
    pub fn take_entry(&self, input: &mut Record) -> Result<Entry, KeyedError> {
        let entry = input.take(self.tag.as_ref()).ok_or(KeyedError::from(
            "Get tag failed since it refers to an empty entry",
        ))?;
        self.inner_get_key(entry)
    }

    /// This is for KeySelector, which generate the key of the input Record according to the tag_key field
    pub fn get_entry(&self, input: &Record) -> Result<Entry, KeyedError> {
        let entry = input
            .get(self.tag.as_ref())
            .ok_or(KeyedError::from(
                "Get tag failed since it refers to an empty entry",
            ))?
            .clone();
        self.inner_get_key(entry)
    }

    /// This is for Order, which generate the comparable field (by ref) of the input Record according to the tag_key field
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
                Entry::Element(RecordElement::OutGraph(object)) => match object {
                    ObjectElement::None => Err(KeyedError::from(
                        "Get key failed since it refers to an empty object element",
                    )),
                    ObjectElement::Prop(prop) => Ok(prop.as_borrow()),
                    ObjectElement::Count(cnt) => Ok((*cnt).into()),
                    ObjectElement::Agg(agg) => Ok(agg.as_borrow()),
                },
                _ => Err(KeyedError::from(
                    "Get key failed since it refers to a Collection type entry",
                )),
            }
        }
    }

    fn inner_get_key(&self, entry: Entry) -> Result<Entry, KeyedError> {
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
