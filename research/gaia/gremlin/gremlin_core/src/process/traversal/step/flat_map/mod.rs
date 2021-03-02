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
use crate::structure::{Direction, Label, Tag};
use pegasus_common::downcast::*;
use std::collections::HashSet;

mod both_v;
mod explore;
mod values;

use crate::structure::filter::codec::from_pb;
pub use explore::{EdgeStep, VertexStep};
use graph_store::prelude::LabelId;
use pegasus::api::function::{DynIter, FlatMapFunction};
pub use values::ValuesStep;

#[enum_dispatch]
pub trait FlatMapGen {
    fn gen(&self) -> Box<dyn FlatMapFunction<Traverser, Traverser, Target = DynIter<Traverser>>>;
}

#[enum_dispatch(Step, FlatMapGen)]
pub enum FlatMapStep {
    VertexToVertex(VertexStep),
    VertexToEdge(EdgeStep),
    Values(ValuesStep),
}

impl_as_any!(FlatMapStep);

impl From<pb::GremlinStep> for FlatMapStep {
    fn from(mut raw: pb::GremlinStep) -> Self {
        if let Some(opt) = raw.step.take() {
            match opt {
                pb::gremlin_step::Step::VertexStep(mut opt) => {
                    if opt.return_type == 0 {
                        let mut step = match opt.direction {
                            0 => VertexStep::new(Direction::Out),
                            1 => VertexStep::new(Direction::In),
                            2 => VertexStep::new(Direction::Both),
                            _ => unreachable!(),
                        };
                        for tag in raw.tags {
                            step.add_tag(tag);
                        }
                        let labels = std::mem::replace(&mut opt.edge_labels, vec![]);
                        step.params.labels =
                            labels.into_iter().map(|id| Label::Id(id as LabelId)).collect();

                        if let Some(test) = opt.predicates.take() {
                            if let Some(filter) = from_pb(&test).expect("todo") {
                                step.params.set_filter(filter);
                            }
                        }

                        FlatMapStep::VertexToVertex(step)
                    } else {
                        let mut step = match opt.direction {
                            0 => EdgeStep::new(Direction::Out),
                            1 => EdgeStep::new(Direction::In),
                            2 => EdgeStep::new(Direction::Both),
                            _ => unreachable!(),
                        };
                        for tag in raw.tags {
                            step.add_tag(tag);
                        }

                        let labels = std::mem::replace(&mut opt.edge_labels, vec![]);
                        step.params.labels =
                            labels.into_iter().map(|id| Label::Id(id as LabelId)).collect();

                        if let Some(test) = opt.predicates.take() {
                            if let Some(filter) = from_pb(&test).expect("todo") {
                                step.params.set_filter(filter);
                            }
                        }
                        FlatMapStep::VertexToEdge(step)
                    }
                }
                pb::gremlin_step::Step::ValuesStep(v) => {
                    let properties = v.properties;
                    let mut values_step = ValuesStep::new(properties);
                    for tag in raw.tags {
                        values_step.add_tag(tag);
                    }
                    FlatMapStep::Values(values_step)
                }
                _ => unreachable!(),
            }
        } else {
            unreachable!()
        }
    }
}
