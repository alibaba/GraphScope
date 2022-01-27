//
//! Copyright 2022 Alibaba Group Holding Limited.
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

use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};

use dyn_type::BorrowObject;
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::graph::element::{Edge, Element, GraphElement, Vertex};
use crate::graph::property::DynDetails;
use crate::graph::ID;

#[derive(Clone, Debug, Hash, PartialEq, PartialOrd)]
pub enum VertexOrEdge {
    V(Vertex),
    E(Edge),
}

impl From<Vertex> for VertexOrEdge {
    fn from(v: Vertex) -> Self {
        Self::V(v)
    }
}

impl From<Edge> for VertexOrEdge {
    fn from(e: Edge) -> Self {
        Self::E(e)
    }
}

#[derive(Clone, Debug, Hash)]
pub enum GraphPath {
    WHOLE((Vec<VertexOrEdge>, usize)),
    END((VertexOrEdge, usize)),
}

impl GraphPath {
    // is_whole_path: Whether to preserve the whole path or only the path_end
    pub fn new<E: Into<VertexOrEdge>>(entry: E, is_whole_path: bool) -> Self {
        if is_whole_path {
            let mut path = Vec::new();
            path.push(entry.into());
            GraphPath::WHOLE((path, 0))
        } else {
            GraphPath::END((entry.into(), 0))
        }
    }

    pub fn append<E: Into<VertexOrEdge>>(&mut self, entry: E) {
        match self {
            GraphPath::WHOLE((ref mut path, ref mut weight)) => {
                path.push(entry.into());
                *weight += 1;
            }
            GraphPath::END((ref mut e, ref mut weight)) => {
                *e = entry.into();
                *weight += 1;
            }
        }
    }

    pub fn get_path_end(&self) -> Option<&VertexOrEdge> {
        match self {
            GraphPath::WHOLE((ref w, _)) => w.last(),
            GraphPath::END((ref e, _)) => Some(e),
        }
    }

    pub fn take_path(self) -> Option<Vec<VertexOrEdge>> {
        match self {
            GraphPath::WHOLE((w, _)) => Some(w),
            GraphPath::END((_, _)) => None,
        }
    }
}

impl Element for VertexOrEdge {
    fn details(&self) -> Option<&DynDetails> {
        match self {
            VertexOrEdge::V(v) => v.details(),
            VertexOrEdge::E(e) => e.details(),
        }
    }
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        match self {
            VertexOrEdge::V(v) => v.as_graph_element(),
            VertexOrEdge::E(e) => e.as_graph_element(),
        }
    }
    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            VertexOrEdge::V(v) => v.as_borrow_object(),
            VertexOrEdge::E(e) => e.as_borrow_object(),
        }
    }
}

impl GraphElement for VertexOrEdge {
    fn id(&self) -> ID {
        match self {
            VertexOrEdge::V(v) => v.id(),
            VertexOrEdge::E(e) => e.id(),
        }
    }

    fn label(&self) -> Option<&NameOrId> {
        match self {
            VertexOrEdge::V(v) => v.label(),
            VertexOrEdge::E(e) => e.label(),
        }
    }

    fn len(&self) -> usize {
        match self {
            VertexOrEdge::V(v) => v.len(),
            VertexOrEdge::E(e) => e.len(),
        }
    }
}

impl Element for GraphPath {
    fn details(&self) -> Option<&DynDetails> {
        None
    }

    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl GraphElement for GraphPath {
    fn id(&self) -> ID {
        match self {
            GraphPath::WHOLE((path, _)) => {
                let ids: Vec<ID> = path.iter().map(|v| v.id()).collect();
                let mut hasher = DefaultHasher::new();
                ids.hash(&mut hasher);
                hasher.finish() as ID
            }
            GraphPath::END((path_end, _)) => path_end.id(),
        }
    }

    fn label(&self) -> Option<&NameOrId> {
        None
    }

    fn len(&self) -> usize {
        match self {
            GraphPath::WHOLE((_, weight)) => *weight,
            GraphPath::END((_, weight)) => *weight,
        }
    }
}

impl PartialEq for GraphPath {
    fn eq(&self, other: &Self) -> bool {
        // We define eq by structure, ignoring path weight
        match (self, other) {
            (GraphPath::WHOLE((p1, _)), GraphPath::WHOLE((p2, _))) => p1.eq(p2),
            (GraphPath::END((p1, _)), GraphPath::END((p2, _))) => p1.eq(p2),
            _ => false,
        }
    }
}
impl PartialOrd for GraphPath {
    // We define partial_cmp by structure, ignoring path weight
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (GraphPath::WHOLE((p1, _)), GraphPath::WHOLE((p2, _))) => p1.partial_cmp(p2),
            (GraphPath::END((p1, _)), GraphPath::END((p2, _))) => p1.partial_cmp(p2),
            _ => None,
        }
    }
}

impl Encode for VertexOrEdge {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            VertexOrEdge::V(v) => {
                writer.write_u8(0)?;
                v.write_to(writer)?;
            }
            VertexOrEdge::E(e) => {
                writer.write_u8(1)?;
                e.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for VertexOrEdge {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let v = <Vertex>::read_from(reader)?;
                Ok(VertexOrEdge::V(v))
            }
            1 => {
                let e = <Edge>::read_from(reader)?;
                Ok(VertexOrEdge::E(e))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Encode for GraphPath {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            GraphPath::WHOLE((path, weight)) => {
                writer.write_u8(0)?;
                writer.write_u64(path.len() as u64)?;
                for vertex_or_edge in path {
                    vertex_or_edge.write_to(writer)?;
                }
                writer.write_u64(*weight as u64)?;
            }
            GraphPath::END((path_end, weight)) => {
                writer.write_u8(1)?;
                path_end.write_to(writer)?;
                writer.write_u64(*weight as u64)?;
            }
        }
        Ok(())
    }
}

impl Decode for GraphPath {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let length = <u64>::read_from(reader)?;
                let mut path = Vec::with_capacity(length as usize);
                for _i in 0..length {
                    let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                    path.push(vertex_or_edge);
                }
                let weight = <u64>::read_from(reader)? as usize;
                Ok(GraphPath::WHOLE((path, weight)))
            }
            1 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                let weight = <u64>::read_from(reader)? as usize;
                Ok(GraphPath::END((vertex_or_edge, weight)))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl TryFrom<result_pb::graph_path::VertexOrEdge> for VertexOrEdge {
    type Error = ParsePbError;
    fn try_from(e: result_pb::graph_path::VertexOrEdge) -> Result<Self, Self::Error> {
        let vertex_or_edge = e
            .inner
            .ok_or(ParsePbError::EmptyFieldError("empty field of VertexOrEdge".to_string()))?;
        match vertex_or_edge {
            result_pb::graph_path::vertex_or_edge::Inner::Vertex(v) => {
                let vertex = v.try_into()?;
                Ok(VertexOrEdge::V(vertex))
            }
            result_pb::graph_path::vertex_or_edge::Inner::Edge(e) => {
                let edge = e.try_into()?;
                Ok(VertexOrEdge::E(edge))
            }
        }
    }
}

impl TryFrom<result_pb::GraphPath> for GraphPath {
    type Error = ParsePbError;
    fn try_from(e: result_pb::GraphPath) -> Result<Self, Self::Error> {
        let graph_path = e
            .path
            .into_iter()
            .map(|vertex_or_edge| vertex_or_edge.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        let graph_len = graph_path.len();
        Ok(GraphPath::WHOLE((graph_path, graph_len)))
    }
}
