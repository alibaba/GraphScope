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

use crate::common::object::{CastError, Primitives};
use crate::generated::common::*;
use crate::generated::gremlin::*;
use crate::structure::filter::*;
use crate::structure::Label;
use crate::{Element, Object};
use graph_store::common::INVALID_LABEL_ID;
use pegasus::BuildJobError;
use prost::{DecodeError, Message};
use std::convert::TryInto;
use std::fmt::Display;

pub fn pb_chain_to_filter<E: Element>(
    pb_chain: &FilterChain,
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
            let logic_opr: Connect = unsafe { std::mem::transmute(node.next) };
            match logic_opr {
                Connect::Or => connect = ChainKind::Or,
                Connect::And => connect = ChainKind::And,
            }
        }
        if chain.is_empty() {
            Ok(None)
        } else {
            Ok(Some(chain))
        }
    }
}

fn get_single(node: &FilterNode) -> Option<&FilterExp> {
    match &node.inner {
        Some(filter_node::Inner::Single(single)) => Some(single),
        _ => None,
    }
}

fn get_chain(node: &FilterNode) -> Option<&Vec<u8>> {
    match &node.inner {
        Some(filter_node::Inner::Chain(chain)) => Some(chain),
        _ => None,
    }
}

pub fn parse_node<E: Element>(
    node: &FilterNode,
) -> Result<Option<Filter<E, ElementFilter>>, ParseError> {
    if let Some(single) = get_single(node) {
        assert!(single.left.is_some() && single.right.is_some());
        let right = single.right.as_ref().unwrap();
        let left = single.left.as_ref().unwrap();
        let cmp: Compare = { unsafe { std::mem::transmute(single.cmp) } };
        let f = match cmp {
            Compare::Eq => eq(left, right)?,
            Compare::Ne => {
                let mut f = eq(left, right)?;
                f.reverse();
                f
            }
            Compare::Lt => lt(left, right)?,
            Compare::Le => lte(left, right)?,
            Compare::Gt => {
                let mut f = lte(left, right)?;
                f.reverse();
                f
            }
            Compare::Ge => {
                let mut f = lt(left, right)?;
                f.reverse();
                f
            }
            Compare::Within => with_in(left, right)?,
            Compare::Without => {
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
fn eq(left: &Key, right: &Value) -> Result<ElementFilter, ParseError> {
    let right: Option<Object> = right.into();
    match &left.item {
        Some(key::Item::Name(name)) => {
            if let Some(value) = right {
                // TODO(longbin) String clone, potentially downgrade performance
                Ok(has_property(name.clone(), value))
            } else {
                Ok(by_property(name.clone()))
            }
        }
        Some(key::Item::NameId(_)) => unimplemented!(),
        Some(key::Item::Id(_)) => {
            #[cfg(not(feature = "llong_id"))]
            let r = right.map(|r| r.as_u64()).transpose()?;
            #[cfg(feature = "llong_id")]
            let r = right.map(|r| r.as_u128()).transpose()?;
            Ok(has_id(r))
        }
        Some(key::Item::Label(_)) => {
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
fn lt(left: &Key, right: &Value) -> Result<ElementFilter, ParseError> {
    match &left.item {
        Some(key::Item::Name(name)) => {
            let right: Option<Object> = right.into();
            if let Some(value) = right {
                // TODO(longbin) String clone, potentially downgrade performance
                Ok(has_property_lt(name.clone(), value))
            } else {
                Ok(by_property_lt(name.clone()))
            }
        }
        Some(key::Item::NameId(_)) => unimplemented!(),
        Some(key::Item::Id(_)) => unimplemented!("can't compare between element id;"),
        Some(key::Item::Label(_)) => unimplemented!("can't compare between element label;"),
        _ => Err(ParseError::InvalidData),
    }
}

#[inline]
fn lte(left: &Key, right: &Value) -> Result<ElementFilter, ParseError> {
    match &left.item {
        Some(key::Item::Name(name)) => {
            let right: Option<Object> = right.into();
            if let Some(value) = right {
                Ok(has_property_le(name.clone(), value))
            } else {
                Ok(by_property_le(name.clone()))
            }
        }
        _ => unimplemented!(),
    }
}

#[inline]
fn with_in(_left: &Key, _right: &Value) -> Result<ElementFilter, ParseError> {
    unimplemented!()
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
