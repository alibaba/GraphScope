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
use crate::process::traversal::step::map::edge_v::EdgeVertexStep;
use crate::process::traversal::step::map::get_path::PathLocalCountStep;
use crate::process::traversal::step::map::identity::IdentityStep;
use crate::process::traversal::step::map::select_one::SelectOneStep;
use crate::process::traversal::step::map::transform_traverser::TransformTraverserStep;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::{Requirement, Traverser};
use crate::structure::Tag;
use crate::FromPb;
use crate::{str_to_dyn_error, DynResult};
pub use get_property::ResultProperty;
use pegasus::api::function::MapFunction;

#[enum_dispatch]
pub trait MapFuncGen {
    fn gen_map(self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>>;
}

mod edge_v;
mod get_path;
mod get_property;
mod identity;
mod select_one;
mod transform_traverser;

impl MapFuncGen for pb::GremlinStep {
    fn gen_map(self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>> {
        let tags = self.get_tags();
        let remove_tags = self.get_remove_tags();
        if let Some(step) = self.step {
            match step {
                pb::gremlin_step::Step::PathStep(path_step) => Ok(Box::new(path_step)),
                pb::gremlin_step::Step::SelectStep(select_step) => select_step.gen_map(),
                pb::gremlin_step::Step::IdentityStep(identity_step) => {
                    let identity_step = IdentityStep { step: identity_step, tags, remove_tags };
                    identity_step.gen_map()
                }
                pb::gremlin_step::Step::SelectOneWithoutBy(select_one_step) => {
                    let select_tag = Tag::from_pb(
                        select_one_step
                            .tag
                            .ok_or(str_to_dyn_error("tag is none in SelectOneWithoutBy"))?,
                    )?;
                    Ok(Box::new(SelectOneStep { select_tag, tags, remove_tags }))
                }
                pb::gremlin_step::Step::PathLocalCountStep(_s) => {
                    Ok(Box::new(PathLocalCountStep { tags, remove_tags }))
                }
                pb::gremlin_step::Step::EdgeVertexStep(edge_vertex_step) => {
                    let edge_vertex_step =
                        EdgeVertexStep { step: edge_vertex_step, tags, remove_tags };
                    edge_vertex_step.gen_map()
                }
                pb::gremlin_step::Step::TransformTraverserStep(s) => {
                    let requirements_pb = unsafe { std::mem::transmute(s.traverser_requirements) };
                    let requirements = Requirement::from_pb(requirements_pb)?;
                    Ok(Box::new(TransformTraverserStep { requirement: requirements, remove_tags }))
                }
                _ => Err(str_to_dyn_error("pb GremlinStep is not a Map Step")),
            }
        } else {
            Err(str_to_dyn_error("pb GremlinStep does not have a step"))
        }
    }
}
