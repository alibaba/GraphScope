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

use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{FilterFuncGen, Step};
use crate::process::traversal::traverser::Traverser;
use crate::structure::{Details, Element, Tag, Token, TraverserFilterChain};
use crate::{str_to_dyn_error, DynResult};
use bit_set::BitSet;
use pegasus::api::function::{FilterFunction, FnResult};
use std::sync::Arc;

pub struct WherePredicateStep {
    pub symbol: StepSymbol,
    pub start_key: Option<Tag>,
    pub start_token: Token,
    tags: Vec<Tag>,
    filter: Arc<TraverserFilterChain>,
}

impl WherePredicateStep {
    pub fn new(start_key: Option<Tag>, start_token: Token, filter: TraverserFilterChain) -> Self {
        WherePredicateStep {
            symbol: StepSymbol::Where,
            start_key,
            start_token,
            tags: vec![],
            filter: Arc::new(filter),
        }
    }
}

impl Step for WherePredicateStep {
    fn get_symbol(&self) -> StepSymbol {
        self.symbol
    }

    fn add_tag(&mut self, tag: Tag) {
        self.tags.push(tag);
    }

    fn tags(&self) -> &[Tag] {
        &self.tags
    }
}

struct Select {
    pub start_key: Option<Tag>,
    pub start_token: Token,
    filter: Arc<TraverserFilterChain>,
    tags: BitSet,
}

impl FilterFunction<Traverser> for Select {
    fn exec(&self, input: &Traverser) -> FnResult<bool> {
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
                crate::structure::reset_tlv_right_value(id);
            }
            Token::Label => {
                let label = start.label().clone();
                crate::structure::reset_tlv_right_value(label);
            }
            Token::Property(ref key) => {
                if let Some(v) = start.details().get_property(key) {
                    if let Some(value) = v.try_to_owned() {
                        crate::structure::reset_tlv_right_value(value);
                    } else {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        };

        let result = self.filter.test(input).unwrap_or(false);
        if result && !self.tags.is_empty() {
            info!("Now we don't support as() in where step");
        }
        Ok(result)
    }
}

impl FilterFuncGen for WherePredicateStep {
    fn gen(&self) -> DynResult<Box<dyn FilterFunction<Traverser>>> {
        let tags = self.get_tags();
        Ok(Box::new(Select {
            start_key: self.start_key.clone(),
            start_token: self.start_token.clone(),
            filter: self.filter.clone(),
            tags,
        }))
    }
}
