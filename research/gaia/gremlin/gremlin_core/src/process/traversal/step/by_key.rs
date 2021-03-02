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

use crate::generated::common::key::Item;
use crate::generated::gremlin as pb;
use crate::structure::{Tag, Token};

/// Define the possible options in Gremlin's `by()-step`
#[derive(Clone)]
pub enum ByStepOption {
    /// by(id), by(label), by('name') where 'name' refers to a property name
    OptToken(Token),
    /// by(valueMap('name1','name2',...)) where 'name1', 'name2' etc. refers to a property name
    OptProperties(Vec<String>),
    /// `group()` will produce key-value pairs, and `by()` can follow keys
    OptGroupKeys,
    /// `group()` will produce key-value pairs, and `by()` can follow values
    OptGroupValues,
    /// by a value computed in previous sub_traversal
    OptSubtraversal,
    /// any invalid signal
    OptInvalid,
}

#[derive(Clone)]
pub struct TagKey {
    pub tag: Option<Tag>,
    pub by_key: Option<ByStepOption>,
}

impl TagKey {
    pub fn empty() -> Self {
        TagKey { tag: None, by_key: None }
    }
}

// TODO(bingqing): throw exception instead of panic(), expect() or unwrap()
impl From<pb::ByKey> for ByStepOption {
    fn from(by_key_pb: pb::ByKey) -> Self {
        match by_key_pb.item {
            Some(pb::by_key::Item::Key(key)) => match key.item {
                Some(Item::Name(prop_name)) => ByStepOption::OptToken(Token::Property(prop_name)),
                // TODO(bingqing): by(prop_id)
                Some(Item::NameId(_)) => unimplemented!(),
                Some(Item::Id(_)) => ByStepOption::OptToken(Token::Id),
                Some(Item::Label(_)) => ByStepOption::OptToken(Token::Label),
                _ => ByStepOption::OptInvalid,
            },
            Some(pb::by_key::Item::Name(properties_pb)) => {
                let mut properties = vec![];
                for prop in properties_pb.item {
                    properties.push(prop);
                }
                ByStepOption::OptProperties(properties)
            }
            Some(pb::by_key::Item::MapKeys(_)) => ByStepOption::OptGroupKeys,
            Some(pb::by_key::Item::MapValues(_)) => ByStepOption::OptGroupValues,
            Some(pb::by_key::Item::Computed(_)) => ByStepOption::OptSubtraversal,
            _ => ByStepOption::OptInvalid,
        }
    }
}

impl From<pb::TagKey> for TagKey {
    fn from(tag_key_pb: pb::TagKey) -> Self {
        let key = if let Some(key_pb) = tag_key_pb.by_key { Some(key_pb.into()) } else { None };
        // e.g., by(id) which indicates by(head.id)
        if tag_key_pb.tag.is_empty() {
            TagKey { tag: None, by_key: key }
        } else {
            // e.g., select("a").by(id)
            TagKey { tag: Some(tag_key_pb.tag), by_key: key }
        }
    }
}
