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
use crate::process::traversal::pop::Pop;
use crate::process::traversal::step::by_key::TagKey;
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::{RemoveLabel, Step};
use crate::process::traversal::traverser::{Requirement, Traverser};
use crate::structure::codec::ParseError;
use crate::structure::{EndPointOpt, Tag};
use crate::DynResult;
use crate::FromPb;
use bit_set::BitSet;
pub use get_property::ResultProperty;
use pegasus::api::function::MapFunction;
use pegasus_common::downcast::*;

#[enum_dispatch]
pub trait MapFuncGen {
    fn gen(&self) -> DynResult<Box<dyn MapFunction<Traverser, Traverser>>>;
}

mod edge_v;
mod get_path;
mod get_property;
mod identity;
mod select_one;
mod transform_traverser;

#[enum_dispatch(Step, MapFuncGen)]
pub enum MapStep {
    EdgeVertex(edge_v::EdgeVertexStep),
    GetPath(get_path::GetPathStep),
    GetProperty(get_property::GetPropertyStep),
    Identity(identity::IdentityStep),
    SelectOne(select_one::SelectOneStep),
    PathLocalCount(get_path::PathLocalCount),
    TransformTraverser(transform_traverser::TransformTraverserStep),
}

impl FromPb<pb::GremlinStep> for MapStep {
    fn from_pb(step: pb::GremlinStep) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match step.step {
            Some(pb::gremlin_step::Step::PathStep(_p)) => {
                Ok(MapStep::GetPath(get_path::GetPathStep))
            }
            Some(pb::gremlin_step::Step::SelectStep(s)) => {
                let pop_pb = unsafe { std::mem::transmute(s.pop) };
                let pop = Pop::from_pb(pop_pb)?;
                let mut tag_keys = vec![];
                let tag_keys_pb = s.select_keys;
                for tag_key_pb in tag_keys_pb {
                    tag_keys.push(TagKey::from_pb(tag_key_pb)?);
                }
                let get_property_step = get_property::GetPropertyStep::new(tag_keys, pop);
                Ok(MapStep::GetProperty(get_property_step))
            }
            Some(pb::gremlin_step::Step::IdentityStep(i)) => {
                let is_all = i.is_all;
                let properties = i.properties;
                let mut identity_step = if is_all || !properties.is_empty() {
                    // the case when we need all properties or given properties
                    identity::IdentityStep::new(Some(properties))
                } else {
                    // the case when we do not need any property
                    identity::IdentityStep::new(None)
                };
                for tag in step.tags {
                    identity_step.add_tag(Tag::from_pb(tag)?);
                }
                for tag in step.remove_tags {
                    identity_step.remove_tag(Tag::from_pb(tag)?);
                }
                Ok(MapStep::Identity(identity_step))
            }
            Some(pb::gremlin_step::Step::SelectOneWithoutBy(s)) => {
                let mut select_step = select_one::SelectOneStep::new(Tag::from_pb(
                    s.tag.ok_or("tag is none in SelectOneWithoutBy")?,
                )?);
                for tag in step.tags {
                    select_step.add_tag(Tag::from_pb(tag)?);
                }
                for tag in step.remove_tags {
                    select_step.remove_tag(Tag::from_pb(tag)?);
                }
                Ok(MapStep::SelectOne(select_step))
            }
            Some(pb::gremlin_step::Step::PathLocalCountStep(_s)) => {
                let mut path_local_count_step = get_path::PathLocalCount::empty();
                for tag in step.tags {
                    path_local_count_step.add_tag(Tag::from_pb(tag)?);
                }
                for tag in step.remove_tags {
                    path_local_count_step.remove_tag(Tag::from_pb(tag)?);
                }
                Ok(MapStep::PathLocalCount(path_local_count_step))
            }
            Some(pb::gremlin_step::Step::EdgeVertexStep(s)) => {
                let opt_pb = unsafe { std::mem::transmute(s.endpoint_opt) };
                let opt = EndPointOpt::from_pb(opt_pb)?;
                let mut edge_vertex_step = edge_v::EdgeVertexStep::new(opt);
                for tag in step.tags {
                    edge_vertex_step.add_tag(Tag::from_pb(tag)?);
                }
                for tag in step.remove_tags {
                    edge_vertex_step.remove_tag(Tag::from_pb(tag)?);
                }
                Ok(MapStep::EdgeVertex(edge_vertex_step))
            }
            Some(pb::gremlin_step::Step::TransformTraverserStep(s)) => {
                let requirements_pb = unsafe { std::mem::transmute(s.traverser_requirements) };
                let requirements = Requirement::from_pb(requirements_pb)?;
                let mut transform_traverser_step =
                    transform_traverser::TransformTraverserStep::new(requirements);
                for tag in step.remove_tags {
                    transform_traverser_step.remove_tag(Tag::from_pb(tag)?);
                }
                Ok(MapStep::TransformTraverser(transform_traverser_step))
            }
            _ => Err(ParseError::InvalidData),
        }
    }
}

impl_as_any!(MapStep);
