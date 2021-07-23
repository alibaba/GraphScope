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

mod element;
pub mod filter;
mod graph;
mod property;

use crate::generated::gremlin as pb;
use crate::structure::codec::ParseError;
use crate::FromPb;
pub use element::{Edge, Element, GraphElement, Label, LabelId, Vertex, VertexOrEdge, ID, ID_BITS};
pub use filter::*;
pub use graph::*;
pub use property::{DefaultDetails, Details, DynDetails, PropId, PropKey, Token};

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Direction {
    Out = 0,
    In = 1,
    Both = 2,
}

impl FromPb<pb::Direction> for Direction {
    fn from_pb(direction: pb::Direction) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match direction {
            pb::Direction::Out => Ok(Direction::Out),
            pb::Direction::In => Ok(Direction::In),
            pb::Direction::Both => Ok(Direction::Both),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum EndPointOpt {
    Out = 0,
    In = 1,
    Other = 2,
}

impl FromPb<pb::edge_vertex_step::EndpointOpt> for EndPointOpt {
    fn from_pb(opt: pb::edge_vertex_step::EndpointOpt) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match opt {
            pb::edge_vertex_step::EndpointOpt::Out => Ok(EndPointOpt::Out),
            pb::edge_vertex_step::EndpointOpt::In => Ok(EndPointOpt::In),
            pb::edge_vertex_step::EndpointOpt::Other => Ok(EndPointOpt::Other),
        }
    }
}

pub type Tag = u8;
pub const EMPTY_TAG: Tag = 0;
/// The initial number of tags, which happens to be a block of bitset.
pub const INIT_TAG_NUM: usize = 32;

impl FromPb<pb::StepTag> for Tag {
    fn from_pb(tag: pb::StepTag) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        if let Some(item) = tag.item {
            match item {
                pb::step_tag::Item::Tag(t) => Ok(t as Tag),
            }
        } else {
            Err("Tag is none in pb".into())
        }
    }
}
