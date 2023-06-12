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
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};

use ahash::HashMap;
use dyn_type::{BorrowObject, Object};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra::path_expand::PathOpt;
use ir_common::generated::algebra::path_expand::ResultOpt;
use ir_common::generated::results as result_pb;
use ir_common::{LabelId, NameOrId};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::Any;
use pegasus_common::downcast::AsAny;
use pegasus_common::impl_as_any;

use crate::apis::{Edge, Element, GraphElement, PropertyValue, Vertex, ID};

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

impl VertexOrEdge {
    pub fn as_vertex(&self) -> Option<&Vertex> {
        match self {
            VertexOrEdge::V(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_edge(&self) -> Option<&Edge> {
        match self {
            VertexOrEdge::E(e) => Some(e),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum GraphPath {
    /// Arbitrary path, which may contain both vertices and edges, or only vertices.
    AllPath(Vec<VertexOrEdge>),
    /// Simple path, which may contains both vertices and edges, or only vertices.
    SimpleAllPath(Vec<VertexOrEdge>),
    /// Arbitrary path with only end vertices preserved, which may contain both vertices and edges, or only vertices.
    EndV((VertexOrEdge, usize)),
    /// Simple path with only end vertex preserved, which may contains both vertices and edges, or only vertices.
    SimpleEndV((VertexOrEdge, Vec<ID>)),
}

impl GraphPath {
    pub fn new<E: Into<VertexOrEdge>>(entry: E, path_opt: PathOpt, result_opt: ResultOpt) -> Self {
        match result_opt {
            ResultOpt::EndV => match path_opt {
                PathOpt::Arbitrary => GraphPath::EndV((entry.into(), 1)),
                PathOpt::Simple => {
                    let entry = entry.into();
                    let id = entry.id();
                    GraphPath::SimpleEndV((entry, vec![id]))
                }
            },
            ResultOpt::AllV | ResultOpt::AllVe => match path_opt {
                PathOpt::Arbitrary => GraphPath::AllPath(vec![entry.into()]),
                PathOpt::Simple => GraphPath::SimpleAllPath(vec![entry.into()]),
            },
        }
    }

    // append an entry and return the flag of whether the entry has been appended or not.
    pub fn append<E: Into<VertexOrEdge>>(&mut self, entry: E) -> bool {
        match self {
            GraphPath::AllPath(ref mut path) => {
                path.push(entry.into());
                true
            }
            GraphPath::SimpleAllPath(ref mut path) => {
                let entry = entry.into();
                if path.contains(&entry) {
                    false
                } else {
                    path.push(entry);
                    true
                }
            }
            GraphPath::EndV((ref mut e, ref mut weight)) => {
                *e = entry.into();
                *weight += 1;
                true
            }
            GraphPath::SimpleEndV((ref mut e, ref mut path)) => {
                let entry = entry.into();
                if path.contains(&entry.id()) {
                    false
                } else {
                    path.push(entry.id());
                    *e = entry.into();
                    true
                }
            }
        }
    }

    pub fn get_path_end(&self) -> &VertexOrEdge {
        match self {
            GraphPath::AllPath(ref p) | GraphPath::SimpleAllPath(ref p) => p.last().unwrap(),
            GraphPath::EndV((ref e, _)) | GraphPath::SimpleEndV((ref e, _)) => e,
        }
    }

    pub fn take_path(self) -> Option<Vec<VertexOrEdge>> {
        match self {
            GraphPath::AllPath(p) | GraphPath::SimpleAllPath(p) => Some(p),
            GraphPath::EndV(_) | GraphPath::SimpleEndV(_) => None,
        }
    }
}

impl Element for VertexOrEdge {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        match self {
            VertexOrEdge::V(v) => v.as_graph_element(),
            VertexOrEdge::E(e) => e.as_graph_element(),
        }
    }

    fn len(&self) -> usize {
        match self {
            VertexOrEdge::V(v) => v.len(),
            VertexOrEdge::E(e) => e.len(),
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

    fn label(&self) -> Option<LabelId> {
        match self {
            VertexOrEdge::V(v) => v.label(),
            VertexOrEdge::E(e) => e.label(),
        }
    }

    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        match self {
            VertexOrEdge::V(v) => v.get_property(key),
            VertexOrEdge::E(e) => e.get_property(key),
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        match self {
            VertexOrEdge::V(v) => v.get_all_properties(),
            VertexOrEdge::E(e) => e.get_all_properties(),
        }
    }
}

impl Element for GraphPath {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
    }

    // the path len is the number of edges in the path;
    fn len(&self) -> usize {
        match self {
            GraphPath::AllPath(p) | GraphPath::SimpleAllPath(p) => p.len() - 1,
            GraphPath::EndV((_, weight)) => *weight - 1,
            GraphPath::SimpleEndV((_, p)) => p.len() - 1,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

// When take `GraphPath` as GraphElement, we actually take the PathEnd Vertex.
// This is always used we have condition in PathExpand,
// on which case we need to evaluate the GraphPath to see if the end vertex satisfies specific conditions.
impl GraphElement for GraphPath {
    fn id(&self) -> ID {
        self.get_path_end().id()
    }

    fn label(&self) -> Option<LabelId> {
        self.get_path_end().label()
    }

    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        self.get_path_end().get_property(key)
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        self.get_path_end().get_all_properties()
    }
}

impl PartialEq for GraphPath {
    fn eq(&self, other: &Self) -> bool {
        // We define eq by structure, ignoring path weight
        match (self, other) {
            (GraphPath::AllPath(p1), GraphPath::AllPath(p2))
            | (GraphPath::AllPath(p1), GraphPath::SimpleAllPath(p2))
            | (GraphPath::SimpleAllPath(p1), GraphPath::AllPath(p2))
            | (GraphPath::SimpleAllPath(p1), GraphPath::SimpleAllPath(p2)) => p1.eq(p2),
            (GraphPath::EndV((p1, _)), GraphPath::EndV((p2, _)))
            | (GraphPath::EndV((p1, _)), GraphPath::SimpleEndV((p2, _)))
            | (GraphPath::SimpleEndV((p1, _)), GraphPath::EndV((p2, _)))
            | (GraphPath::SimpleEndV((p1, _)), GraphPath::SimpleEndV((p2, _))) => p1.eq(p2),
            _ => false,
        }
    }
}
impl PartialOrd for GraphPath {
    // We define partial_cmp by structure, ignoring path weight
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (GraphPath::AllPath(p1), GraphPath::AllPath(p2))
            | (GraphPath::AllPath(p1), GraphPath::SimpleAllPath(p2))
            | (GraphPath::SimpleAllPath(p1), GraphPath::AllPath(p2))
            | (GraphPath::SimpleAllPath(p1), GraphPath::SimpleAllPath(p2)) => p1.partial_cmp(p2),
            (GraphPath::EndV((p1, _)), GraphPath::EndV((p2, _)))
            | (GraphPath::EndV((p1, _)), GraphPath::SimpleEndV((p2, _)))
            | (GraphPath::SimpleEndV((p1, _)), GraphPath::EndV((p2, _)))
            | (GraphPath::SimpleEndV((p1, _)), GraphPath::SimpleEndV((p2, _))) => p1.partial_cmp(p2),
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
            GraphPath::AllPath(path) => {
                writer.write_u8(0)?;
                path.write_to(writer)?;
            }
            GraphPath::EndV((path_end, weight)) => {
                writer.write_u8(1)?;
                path_end.write_to(writer)?;
                writer.write_u64(*weight as u64)?;
            }
            GraphPath::SimpleAllPath(path) => {
                writer.write_u8(2)?;
                path.write_to(writer)?;
            }
            GraphPath::SimpleEndV((path_end, path)) => {
                writer.write_u8(3)?;
                path_end.write_to(writer)?;
                path.write_to(writer)?;
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
                let path = <Vec<VertexOrEdge>>::read_from(reader)?;
                Ok(GraphPath::AllPath(path))
            }
            1 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                let weight = <u64>::read_from(reader)? as usize;
                Ok(GraphPath::EndV((vertex_or_edge, weight)))
            }
            2 => {
                let path = <Vec<VertexOrEdge>>::read_from(reader)?;
                Ok(GraphPath::SimpleAllPath(path))
            }
            3 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                let path = <Vec<ID>>::read_from(reader)?;
                Ok(GraphPath::SimpleEndV((vertex_or_edge, path)))
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
        Ok(GraphPath::AllPath(graph_path))
    }
}

impl Hash for GraphPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            GraphPath::AllPath(p) | GraphPath::SimpleAllPath(p) => p.hash(state),
            GraphPath::EndV((e, _)) | GraphPath::SimpleEndV((e, _)) => e.hash(state),
        }
    }
}

impl_as_any!(GraphPath);
