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

use crate::generated::common as common_pb;
use crate::generated::gremlin as pb;
use crate::structure::codec::ParseError;
use crate::structure::{PropId, PropKey, Tag, Token};
use crate::FromPb;

/// Define the possible options in Gremlin's `by()-step`
#[derive(Clone, Debug)]
pub enum ByStepOption {
    /// by(id), by(label), by('name') where 'name' refers to a property name
    OptToken(Token),
    /// by(valueMap('name1','name2',...)) where 'name1', 'name2' etc. refers to a property name
    OptProperties(Vec<PropKey>),
    /// `group()` will produce key-value pairs, and `by()` can follow keys, e.g., order().by(keys) or order().by(select(keys).values('id'))
    OptGroupKeys(Option<Token>),
    /// `group()` will produce key-value pairs, and `by()` can follow values, e.g., order().by(values) or order().by(select(values).values('id'))
    OptGroupValues(Option<Token>),
    /// by a value computed in previous sub_traversal
    OptSubtraversal,
}

#[derive(Clone, Debug, Default)]
pub struct TagKey {
    pub tag: Option<Tag>,
    pub by_key: Option<ByStepOption>,
}

impl FromPb<common_pb::Key> for Token {
    fn from_pb(token_pb: common_pb::Key) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match token_pb.item {
            Some(common_pb::key::Item::Name(prop_name)) => Ok(Token::Property(prop_name.into())),
            Some(common_pb::key::Item::NameId(prop_id)) => {
                Ok(Token::Property((prop_id as PropId).into()))
            }
            Some(common_pb::key::Item::Id(_)) => Ok(Token::Id),
            Some(common_pb::key::Item::Label(_)) => Ok(Token::Label),
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl FromPb<pb::ByKey> for ByStepOption {
    fn from_pb(by_key_pb: pb::ByKey) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match by_key_pb.item {
            Some(pb::by_key::Item::Key(key)) => Ok(ByStepOption::OptToken(Token::from_pb(key)?)),
            // TODO(bingqing): should be prop_name or prop_id
            Some(pb::by_key::Item::PropKeys(properties_pb)) => {
                let mut properties = vec![];
                for prop_key_pb in properties_pb.prop_keys {
                    properties.push(PropKey::from_pb(prop_key_pb)?)
                }
                Ok(ByStepOption::OptProperties(properties))
            }
            Some(pb::by_key::Item::MapKeys(map_key)) => {
                if let Some(map_key) = map_key.key {
                    Ok(ByStepOption::OptGroupKeys(Some(Token::from_pb(map_key)?)))
                } else {
                    Ok(ByStepOption::OptGroupKeys(None))
                }
            }
            Some(pb::by_key::Item::MapValues(map_value)) => {
                if let Some(map_value) = map_value.key {
                    Ok(ByStepOption::OptGroupValues(Some(Token::from_pb(map_value)?)))
                } else {
                    Ok(ByStepOption::OptGroupValues(None))
                }
            }
            Some(pb::by_key::Item::Computed(_)) => Ok(ByStepOption::OptSubtraversal),
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl FromPb<pb::TagKey> for TagKey {
    fn from_pb(tag_key_pb: pb::TagKey) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        let key = if let Some(key_pb) = tag_key_pb.by_key {
            Some(ByStepOption::from_pb(key_pb)?)
        } else {
            None
        };
        // e.g., by(id) which indicates by(head.id)
        if tag_key_pb.tag.is_none() {
            Ok(TagKey { tag: None, by_key: key })
        } else {
            // e.g., select("a").by(id)
            Ok(TagKey { tag: Some(Tag::from_pb(tag_key_pb.tag.unwrap())?), by_key: key })
        }
    }
}
