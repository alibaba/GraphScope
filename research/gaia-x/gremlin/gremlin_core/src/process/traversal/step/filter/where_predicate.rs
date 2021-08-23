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

use crate::generated::gremlin as pb;
use crate::process::traversal::step::FilterFuncGen;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::pb_chain_to_filter;
use crate::structure::{with_tag, Details, Element, Filter, Tag, Token, TraverserFilterChain};
use crate::{str_to_dyn_error, DynResult, FromPb};
use pegasus::api::function::{FilterFunction, FnResult};
use std::sync::Arc;

struct WhereStep {
    pub start_key: Option<Tag>,
    pub start_token: Token,
    filter: Arc<TraverserFilterChain>,
}

impl FilterFunction<Traverser> for WhereStep {
    fn test(&self, input: &Traverser) -> FnResult<bool> {
        let start = if let Some(ref start) = self.start_key {
            if let Some(t) = input.select(start) {
                t.as_element().ok_or(str_to_dyn_error("invalid input for where predicate"))?
            } else {
                return Ok(false);
            }
        } else {
            input.get_element().ok_or(str_to_dyn_error("invalid input for where predicate"))?
        };
        match self.start_token {
            Token::Id => {
                let id = start.id();
                crate::structure::reset_tlv_left_value(id);
            }
            Token::Label => {
                let label = start.label().clone();
                crate::structure::reset_tlv_left_value(label.as_object());
            }
            Token::Property(ref key) => {
                if let Some(v) = start.details().get_property(key) {
                    if let Some(value) = v.try_to_owned() {
                        crate::structure::reset_tlv_left_value(value);
                    } else {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        };

        let result = self.filter.test(input).unwrap_or(false);
        Ok(result)
    }
}

impl FilterFuncGen for pb::WhereStep {
    fn gen_filter(mut self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        let start_key = if self.start_tag.is_none() {
            None
        } else {
            Some(Tag::from_pb(self.start_tag.unwrap())?)
        };
        let start_token_pb =
            self.start_token.take().ok_or(str_to_dyn_error("start token is none"))?;
        let start_token = Token::from_pb(start_token_pb)?;
        let tags_pb = std::mem::replace(&mut self.tags, vec![]);
        let mut select_tags = vec![];
        for tag_pb in tags_pb {
            select_tags.push(Tag::from_pb(tag_pb)?);
        }
        let mut filter = Filter::default();
        if let Some(predicates) = self.predicates {
            if let Some(test) = pb_chain_to_filter(&predicates)? {
                let mut iter = select_tags.into_iter();
                filter = with_tag(&mut iter, test);
            }
        };

        Ok(Box::new(WhereStep { start_key, start_token, filter: Arc::new(filter) }))
    }
}
