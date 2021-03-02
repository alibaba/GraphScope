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
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::filter::codec::from_pb;
use crate::structure::{with_tag, without_tag, Filter, IsSimple, Tag, Token, TraverserFilter};
use pegasus::api::function::FilterFunction;
use pegasus_common::downcast::*;
use std::collections::HashSet;

mod has;
mod where_predicate;

#[enum_dispatch]
pub trait FilterFuncGen {
    fn gen(&self) -> Box<dyn FilterFunction<Traverser>>;
}

pub use has::HasStep;
pub use where_predicate::WherePredicateStep;

#[enum_dispatch(FilterFuncGen, Step)]
pub enum FilterStep {
    Has(HasStep),
    WhereP(WherePredicateStep),
}

impl_as_any!(FilterStep);

impl From<pb::GremlinStep> for FilterStep {
    fn from(mut raw: pb::GremlinStep) -> Self {
        if let Some(opt) = raw.step.take() {
            match opt {
                pb::gremlin_step::Step::HasStep(mut opt) => {
                    let filter = if let Some(test) = opt.predicates.take() {
                        if let Some(test) = from_pb(&test).expect("todo") {
                            without_tag(test)
                        } else {
                            Filter::new()
                        }
                    } else {
                        Filter::new()
                    };
                    FilterStep::Has(HasStep::new(filter))
                }
                pb::gremlin_step::Step::WhereStep(mut opt) => {
                    let start_key =
                        if opt.start_tag.is_empty() { None } else { Some(opt.start_tag.clone()) };

                    let start_token = if let Some(key) = opt.start_token.take() {
                        match key.item {
                            Some(Item::Id(_)) => Token::Id,
                            Some(Item::Label(_)) => Token::Label,
                            Some(Item::Name(p)) => Token::Property(p),
                            Some(Item::NameId(_)) => unimplemented!(),
                            None => Token::Id,
                        }
                    } else {
                        Token::Id
                    };

                    let select_tags = std::mem::replace(&mut opt.tags, vec![]);
                    let filter = if let Some(test) = opt.predicates {
                        if let Some(filter) = from_pb(&test).expect("todo") {
                            let mut iter = select_tags.into_iter();
                            with_tag(&mut iter, filter)
                        } else {
                            Filter::new()
                        }
                    } else {
                        Filter::new()
                    };
                    let step = WherePredicateStep::new(start_key, start_token, filter);
                    FilterStep::WhereP(step)
                }
                pb::gremlin_step::Step::PathFilterStep(opt) => {
                    let filter = if opt.hint == 0 {
                        TraverserFilter::HasCycle(IsSimple::Simple)
                    } else {
                        TraverserFilter::HasCycle(IsSimple::Cyclic)
                    };
                    FilterStep::Has(HasStep::new(Filter::with(filter)))
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!()
        }
    }
}
