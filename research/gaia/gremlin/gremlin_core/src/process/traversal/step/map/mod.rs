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
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::Traverser;
use crate::structure::Tag;
use pegasus::api::function::MapFunction;
use pegasus_common::downcast::*;
use std::collections::HashSet;

#[enum_dispatch]
pub trait MapFuncGen {
    fn gen(&self) -> Box<dyn MapFunction<Traverser, Traverser>>;
}

mod edge_v;
mod get_path;
mod get_property;
mod identity;
mod select_one;

pub use get_property::ResultProperty;

#[enum_dispatch(Step, MapFuncGen)]
pub enum MapStep {
    EdgeVertex(edge_v::EdgeVertexStep),
    GetPath(get_path::GetPathStep),
    GetProperty(get_property::GetPropertyStep),
    Identity(identity::IdentityStep),
    SelectOne(select_one::SelectOneStep),
    PathLocalCount(get_path::PathLocalCount),
}

impl From<pb::GremlinStep> for MapStep {
    fn from(step: pb::GremlinStep) -> Self {
        match step.step {
            Some(pb::gremlin_step::Step::PathStep(_p)) => MapStep::GetPath(get_path::GetPathStep),
            Some(pb::gremlin_step::Step::SelectStep(s)) => {
                let pop = match s.pop {
                    0 => Pop::First,
                    1 => Pop::Last,
                    2 => Pop::All,
                    3 => Pop::Mixed,
                    _ => unreachable!(),
                };
                let mut tag_keys = vec![];
                let tag_keys_pb = s.select_keys;
                for tag_key_pb in tag_keys_pb {
                    tag_keys.push(tag_key_pb.into());
                }
                let get_property_step = get_property::GetPropertyStep::new(tag_keys, pop);
                MapStep::GetProperty(get_property_step)
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
                    identity_step.add_tag(tag);
                }
                MapStep::Identity(identity_step)
            }
            Some(pb::gremlin_step::Step::SelectOneWithoutBy(s)) => {
                let select_tag = s.tag;
                let mut select_step = select_one::SelectOneStep::new(select_tag);
                for tag in step.tags {
                    select_step.add_tag(tag);
                }
                MapStep::SelectOne(select_step)
            }
            Some(pb::gremlin_step::Step::PathLocalCountStep(_s)) => {
                let mut path_local_count_step = get_path::PathLocalCount::empty();
                for tag in step.tags {
                    path_local_count_step.add_tag(tag);
                }
                MapStep::PathLocalCount(path_local_count_step)
            }
            Some(pb::gremlin_step::Step::EdgeVertexStep(s)) => {
                let mut edge_vertex_step = match s.direction {
                    0 => edge_v::EdgeVertexStep::out_v(),
                    1 => edge_v::EdgeVertexStep::in_v(),
                    2 => edge_v::EdgeVertexStep::other_v(),
                    _ => unreachable!(),
                };
                for tag in step.tags {
                    edge_vertex_step.add_tag(tag);
                }
                MapStep::EdgeVertex(edge_vertex_step)
            }
            _ => unimplemented!(),
        }
    }
}

impl_as_any!(MapStep);
