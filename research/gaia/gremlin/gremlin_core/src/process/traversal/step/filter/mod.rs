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
use crate::process::traversal::step::filter::is::IsStep;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::ParseError;
use crate::structure::filter::codec::pb_chain_to_filter;
use crate::structure::{
    with_tag, without_tag, Filter, IsSimple, Tag, Token, TraverserFilter, ValueFilter,
};
use crate::DynResult;
use crate::FromPb;
use bit_set::BitSet;
pub use has::HasStep;
use pegasus::api::function::FilterFunction;
use pegasus_common::downcast::*;
pub use where_predicate::WherePredicateStep;

mod has;
mod is;
mod where_predicate;

#[enum_dispatch]
pub trait FilterFuncGen {
    fn gen(&self) -> DynResult<Box<dyn FilterFunction<Traverser>>>;
}

#[enum_dispatch(FilterFuncGen, Step)]
pub enum FilterStep {
    Has(HasStep),
    WhereP(WherePredicateStep),
    Is(IsStep),
}

impl_as_any!(FilterStep);

impl FromPb<pb::GremlinStep> for FilterStep {
    fn from_pb(mut raw: pb::GremlinStep) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        if let Some(opt) = raw.step.take() {
            match opt {
                pb::gremlin_step::Step::HasStep(mut opt) => {
                    let filter = if let Some(test) = opt.predicates.take() {
                        if let Some(test) = pb_chain_to_filter(&test)? {
                            without_tag(test)
                        } else {
                            Filter::new()
                        }
                    } else {
                        Filter::new()
                    };
                    let mut step = HasStep::new(filter);
                    for tag in raw.tags {
                        step.add_tag(Tag::from_pb(tag)?);
                    }
                    Ok(FilterStep::Has(step))
                }
                pb::gremlin_step::Step::WhereStep(mut opt) => {
                    let start_key = if opt.start_tag.is_none() {
                        None
                    } else {
                        Some(Tag::from_pb(opt.start_tag.unwrap())?)
                    };
                    let start_token_pb = opt.start_token.take().ok_or("start token is none")?;
                    let start_token = Token::from_pb(start_token_pb)?;
                    let tags_pb = std::mem::replace(&mut opt.tags, vec![]);
                    let mut select_tags = vec![];
                    for tag_pb in tags_pb {
                        select_tags.push(Tag::from_pb(tag_pb)?);
                    }
                    let filter = if let Some(test) = opt.predicates {
                        if let Some(filter) = pb_chain_to_filter(&test)? {
                            let mut iter = select_tags.into_iter();
                            with_tag(&mut iter, filter)
                        } else {
                            Filter::new()
                        }
                    } else {
                        Filter::new()
                    };
                    let mut step = WherePredicateStep::new(start_key, start_token, filter);
                    for tag in raw.tags {
                        step.add_tag(Tag::from_pb(tag)?);
                    }
                    Ok(FilterStep::WhereP(step))
                }
                pb::gremlin_step::Step::PathFilterStep(opt) => {
                    let filter = if opt.hint == 0 {
                        TraverserFilter::HasCycle(IsSimple::Simple)
                    } else {
                        TraverserFilter::HasCycle(IsSimple::Cyclic)
                    };
                    Ok(FilterStep::Has(HasStep::new(Filter::with(filter))))
                }
                pb::gremlin_step::Step::IsStep(opt) => {
                    let value_filter_pb = opt.single.ok_or("filter is not set in is step")?;
                    let value_filter = ValueFilter::from_pb(value_filter_pb)?;
                    let traverser_filter = TraverserFilter::IsValue(value_filter);
                    Ok(FilterStep::Is(IsStep::new(Filter::with(traverser_filter))))
                }
                _ => Err(ParseError::InvalidData),
            }
        } else {
            Err(ParseError::InvalidData)
        }
    }
}
