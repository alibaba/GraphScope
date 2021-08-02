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

use crate::generated::common as pb_type;
use crate::generated::gremlin as pb;
use crate::structure::filter::*;
use crate::structure::{Label, PropId};
use crate::{Element, ID};
use dyn_type::{CastError, Object, Primitives};
use graph_store::prelude::INVALID_LABEL_ID;
use pegasus::BuildJobError;
use prost::{DecodeError, Message};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt::Display;

pub fn pb_chain_to_filter<E: Element>(
    pb_chain: &pb::FilterChain,
) -> Result<Option<Filter<E, ElementFilter>>, ParseError> {
    let size = pb_chain.node.len();
    if size == 0 {
        return Ok(None);
    }

    if size == 1 {
        let node = &pb_chain.node[0];
        parse_node(node)
    } else {
        let mut chain = Filter::default();
        let mut connect = ChainKind::Or;
        for node in pb_chain.node.iter() {
            if let Some(f) = parse_node(node)? {
                match connect {
                    ChainKind::And => {
                        chain.and(f);
                    }
                    ChainKind::Or => {
                        chain.or(f);
                    }
                }
            }
            let logic_opr: pb::Connect = unsafe { std::mem::transmute(node.next) };
            match logic_opr {
                pb::Connect::Or => connect = ChainKind::Or,
                pb::Connect::And => connect = ChainKind::And,
            }
        }
        if chain.is_empty() {
            Ok(None)
        } else {
            Ok(Some(chain))
        }
    }
}

pub fn pb_value_to_object(raw: &pb_type::Value) -> Option<Object> {
    match &raw.item {
        Some(pb_type::value::Item::Blob(blob)) => {
            let mut bytes = vec![0; blob.len()];
            bytes.copy_from_slice(blob);
            Some(bytes.into())
        }
        Some(pb_type::value::Item::Boolean(item)) => Some((*item).into()),
        Some(pb_type::value::Item::I32(item)) => Some((*item).into()),
        Some(pb_type::value::Item::I64(item)) => Some((*item).into()),
        Some(pb_type::value::Item::F64(item)) => Some((*item).into()),
        Some(pb_type::value::Item::Str(item)) => Some(item.as_str().into()),
        Some(pb_type::value::Item::I32Array(_)) => unimplemented!(),
        Some(pb_type::value::Item::I64Array(_)) => unimplemented!(),
        Some(pb_type::value::Item::F64Array(_)) => unimplemented!(),
        Some(pb_type::value::Item::StrArray(_)) => unimplemented!(),
        Some(pb_type::value::Item::None(_)) => None,
        _ => None,
    }
}

pub fn pb_value_to_array_object(
    raw: &pb_type::Value,
) -> Result<Option<HashSet<Object>>, ParseError> {
    match &raw.item {
        Some(pb_type::value::Item::I32Array(array)) => {
            let mut set = HashSet::with_capacity(array.item.len());
            for item in &array.item {
                set.insert((*item).into());
            }
            Ok(Some(set))
        }
        Some(pb_type::value::Item::I64Array(array)) => {
            let mut set = HashSet::with_capacity(array.item.len());
            for item in &array.item {
                set.insert((*item).into());
            }
            Ok(Some(set))
        }
        Some(pb_type::value::Item::F64Array(array)) => {
            let mut set = HashSet::with_capacity(array.item.len());
            for item in &array.item {
                set.insert((*item).into());
            }
            Ok(Some(set))
        }
        Some(pb_type::value::Item::StrArray(array)) => {
            let mut set = HashSet::with_capacity(array.item.len());
            for item in &array.item {
                set.insert(item.as_str().into());
            }
            Ok(Some(set))
        }
        _ => Err(ParseError::InvalidData),
    }
}

fn get_single(node: &pb::FilterNode) -> Option<&pb::FilterExp> {
    match &node.inner {
        Some(pb::filter_node::Inner::Single(single)) => Some(single),
        _ => None,
    }
}

fn get_chain(node: &pb::FilterNode) -> Option<&Vec<u8>> {
    match &node.inner {
        Some(pb::filter_node::Inner::Chain(chain)) => Some(chain),
        _ => None,
    }
}

pub fn parse_node<E: Element>(
    node: &pb::FilterNode,
) -> Result<Option<Filter<E, ElementFilter>>, ParseError> {
    if let Some(single) = get_single(node) {
        assert!(single.left.is_some() && single.right.is_some());
        let right = single.right.as_ref().unwrap();
        let left = single.left.as_ref().unwrap();
        let cmp: pb::Compare = { unsafe { std::mem::transmute(single.cmp) } };
        let f = match cmp {
            pb::Compare::Eq => eq(left, right)?,
            pb::Compare::Ne => {
                let mut f = eq(left, right)?;
                f.reverse();
                f
            }
            pb::Compare::Lt => lt(left, right)?,
            pb::Compare::Le => lte(left, right)?,
            pb::Compare::Gt => {
                let mut f = lte(left, right)?;
                f.reverse();
                f
            }
            pb::Compare::Ge => {
                let mut f = lt(left, right)?;
                f.reverse();
                f
            }
            pb::Compare::Within => with_in(left, right)?,
            pb::Compare::Without => {
                let mut f = with_in(left, right)?;
                f.reverse();
                f
            }
        };
        Ok(Some(Filter::with(f)))
    } else {
        if let Some(chain_bytes) = get_chain(node) {
            let chain = Message::decode(chain_bytes.as_slice())?;
            pb_chain_to_filter(&chain)
        } else {
            Err(ParseError::InvalidData)
        }
    }
}

#[inline]
fn eq(left: &pb_type::Key, right: &pb_type::Value) -> Result<ElementFilter, ParseError> {
    let right: Option<Object> = pb_value_to_object(right);
    match &left.item {
        Some(pb_type::key::Item::Name(prop_name)) => {
            if let Some(value) = right {
                // TODO(longbin) String clone, potentially downgrade performance
                Ok(has_property(prop_name.into(), value))
            } else {
                Ok(by_property(prop_name.into()))
            }
        }
        Some(pb_type::key::Item::NameId(prop_id)) => {
            if let Some(value) = right {
                // TODO(longbin) String clone, potentially downgrade performance
                Ok(has_property((*prop_id as PropId).into(), value))
            } else {
                Ok(by_property((*prop_id as PropId).into()))
            }
        }
        Some(pb_type::key::Item::Id(_)) => {
            let r = right.map(|r|
                // try to parse as i64 in case id is negative
                if let Ok(id) = r.as_i64() { Ok(id as ID) } else { r.as_u128()})
                .transpose()?;
            Ok(has_id(r))
        }
        Some(pb_type::key::Item::Label(_)) => {
            let label = right
                .map(|r| match r {
                    Object::Primitive(Primitives::Integer(id)) => {
                        Ok(Label::Id(id.try_into().unwrap_or(INVALID_LABEL_ID)))
                    }
                    Object::String(str) => Ok(Label::Str(str)),
                    _ => Err(ParseError::InvalidData),
                })
                .transpose()?;
            Ok(has_label(label))
        }
        _ => Err(ParseError::InvalidData),
    }
}

#[inline]
fn lt(left: &pb_type::Key, right: &pb_type::Value) -> Result<ElementFilter, ParseError> {
    match &left.item {
        Some(pb_type::key::Item::Name(name)) => {
            let right: Option<Object> = pb_value_to_object(right);
            if let Some(value) = right {
                // TODO(longbin) String clone, potentially downgrade performance
                Ok(has_property_lt(name.into(), value))
            } else {
                Ok(by_property_lt(name.into()))
            }
        }
        Some(pb_type::key::Item::NameId(prop_id)) => {
            let right: Option<Object> = pb_value_to_object(right);
            if let Some(value) = right {
                Ok(has_property_lt((*prop_id as PropId).into(), value))
            } else {
                Ok(by_property_lt((*prop_id as PropId).into()))
            }
        }
        Some(pb_type::key::Item::Id(_)) | Some(pb_type::key::Item::Label(_)) => {
            Err(ParseError::OtherErr("Can only compare for property values;".to_string()))
        }
        _ => Err(ParseError::InvalidData),
    }
}

#[inline]
fn lte(left: &pb_type::Key, right: &pb_type::Value) -> Result<ElementFilter, ParseError> {
    match &left.item {
        Some(pb_type::key::Item::Name(name)) => {
            let right: Option<Object> = pb_value_to_object(right);
            if let Some(value) = right {
                Ok(has_property_le(name.into(), value))
            } else {
                Ok(by_property_le(name.into()))
            }
        }
        Some(pb_type::key::Item::NameId(prop_id)) => {
            let right: Option<Object> = pb_value_to_object(right);
            if let Some(value) = right {
                Ok(has_property_le((*prop_id as PropId).into(), value))
            } else {
                Ok(by_property_le((*prop_id as PropId).into()))
            }
        }
        Some(pb_type::key::Item::Id(_)) | Some(pb_type::key::Item::Label(_)) => {
            Err(ParseError::OtherErr("Can only compare for property values;".to_string()))
        }
        _ => Err(ParseError::InvalidData),
    }
}

#[inline]
fn with_in(left: &pb_type::Key, right: &pb_type::Value) -> Result<ElementFilter, ParseError> {
    let right = pb_value_to_array_object(right)?;
    match &left.item {
        Some(pb_type::key::Item::Name(name)) => {
            if let Some(right) = right {
                Ok(contains_property(name.into(), right))
            } else {
                Err(ParseError::InvalidData)
            }
        }
        Some(pb_type::key::Item::NameId(prop_id)) => {
            if let Some(right) = right {
                Ok(contains_property((*prop_id as PropId).into(), right))
            } else {
                Err(ParseError::InvalidData)
            }
        }
        Some(pb_type::key::Item::Id(_)) => {
            if let Some(right) = right {
                let mut right_ids = HashSet::new();
                for obj in right {
                    // try to parse as i64 in case id is negative
                    if let Ok(id) = obj.as_i64() {
                        right_ids.insert(id as ID);
                    } else if let Ok(id) = obj.as_u128() {
                        right_ids.insert(id);
                    } else {
                        warn!("parse id failed in contains_id: {:?}", obj);
                    }
                }
                Ok(contains_id(right_ids))
            } else {
                Err(ParseError::InvalidData)
            }
        }
        Some(pb_type::key::Item::Label(_)) => {
            if let Some(right) = right {
                let mut right_label_ids = HashSet::new();
                for obj in right {
                    let label_id = match obj {
                        Object::Primitive(Primitives::Integer(id)) => {
                            Label::Id(id.try_into().unwrap_or(INVALID_LABEL_ID))
                        }
                        Object::String(str) => Label::Str(str),
                        _ => Label::Id(INVALID_LABEL_ID),
                    };
                    right_label_ids.insert(label_id);
                }
                Ok(contains_label(right_label_ids))
            } else {
                Err(ParseError::InvalidData)
            }
        }
        None => Err(ParseError::InvalidData),
    }
}

#[derive(Debug)]
pub enum ParseError {
    ReadPB(DecodeError),
    TypeCast(CastError),
    InvalidData,
    OtherErr(String),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParseError::ReadPB(e) => write!(f, "parse pb error: {}", e),
            ParseError::TypeCast(e) => write!(f, "type cast error {}", e),
            ParseError::InvalidData => write!(f, "invalid data error"),
            ParseError::OtherErr(e) => write!(f, "parse error {}", e),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<&str> for ParseError {
    fn from(e: &str) -> Self {
        ParseError::OtherErr(e.into())
    }
}

impl From<DecodeError> for ParseError {
    fn from(e: DecodeError) -> Self {
        ParseError::ReadPB(e)
    }
}

impl From<CastError> for ParseError {
    fn from(e: CastError) -> Self {
        ParseError::TypeCast(e)
    }
}

impl From<ParseError> for BuildJobError {
    fn from(e: ParseError) -> Self {
        format!("decode filter error: {}", e).into()
    }
}
