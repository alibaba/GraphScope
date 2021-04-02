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
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::ParseError;
use crate::structure::filter::codec::pb_chain_to_filter;
use crate::structure::{Direction, Label, Tag};
use crate::DynResult;
use crate::FromPb;
use bit_set::BitSet;
pub use explore::{EdgeStep, VertexStep};
use graph_store::prelude::LabelId;
use pegasus::api::function::{DynIter, FlatMapFunction};
use pegasus_common::downcast::*;
pub use unfold::UnfoldStep;
pub use values::ValuesStep;

mod both_v;
mod explore;
mod unfold;
mod values;

// TODO(longbin) Should return results.
#[enum_dispatch]
pub trait FlatMapGen {
    fn gen(
        &self,
    ) -> DynResult<Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>>;
}

#[enum_dispatch(Step, FlatMapGen)]
pub enum FlatMapStep {
    VertexToVertex(VertexStep),
    VertexToEdge(EdgeStep),
    Values(ValuesStep),
    Unfold(UnfoldStep),
}

impl_as_any!(FlatMapStep);

impl FromPb<pb::GremlinStep> for FlatMapStep {
    fn from_pb(mut raw: pb::GremlinStep) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        if let Some(opt) = raw.step.take() {
            match opt {
                pb::gremlin_step::Step::VertexStep(mut opt) => {
                    if opt.return_type == 0 {
                        let direction_pb = unsafe { std::mem::transmute(opt.direction) };
                        let direction = Direction::from_pb(direction_pb)?;
                        let mut step = VertexStep::new(direction);
                        for tag in raw.tags {
                            step.add_tag(Tag::from_pb(tag)?);
                        }
                        let labels = std::mem::replace(&mut opt.edge_labels, vec![]);
                        step.params.labels =
                            labels.into_iter().map(|id| Label::Id(id as LabelId)).collect();

                        if let Some(test) = opt.predicates.take() {
                            if let Some(filter) = pb_chain_to_filter(&test)? {
                                step.params.set_filter(filter);
                            }
                        }

                        Ok(FlatMapStep::VertexToVertex(step))
                    } else {
                        let direction_pb = unsafe { std::mem::transmute(opt.direction) };
                        let direction = Direction::from_pb(direction_pb)?;
                        let mut step = EdgeStep::new(direction);
                        for tag in raw.tags {
                            step.add_tag(Tag::from_pb(tag)?);
                        }

                        let labels = std::mem::replace(&mut opt.edge_labels, vec![]);
                        step.params.labels =
                            labels.into_iter().map(|id| Label::Id(id as LabelId)).collect();

                        if let Some(test) = opt.predicates.take() {
                            if let Some(filter) = pb_chain_to_filter(&test)? {
                                step.params.set_filter(filter);
                            }
                        }
                        Ok(FlatMapStep::VertexToEdge(step))
                    }
                }
                pb::gremlin_step::Step::ValuesStep(v) => {
                    let properties = v.properties;
                    let mut values_step = ValuesStep::new(properties);
                    for tag in raw.tags {
                        values_step.add_tag(Tag::from_pb(tag)?);
                    }
                    Ok(FlatMapStep::Values(values_step))
                }
                pb::gremlin_step::Step::UnfoldStep(_v) => Ok(FlatMapStep::Unfold(UnfoldStep {})),
                _ => Err(ParseError::InvalidData),
            }
        } else {
            Err(ParseError::InvalidData)
        }
    }
}
