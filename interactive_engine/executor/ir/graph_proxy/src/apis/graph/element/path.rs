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
use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};

use dyn_type::BorrowObject;
use ir_common::error::ParsePbError;
use ir_common::generated::algebra::path_expand::PathOpt;
use ir_common::generated::algebra::path_expand::ResultOpt;
use ir_common::generated::results as result_pb;
use ir_common::LabelId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::apis::{read_id, write_id, DynDetails, Edge, Element, GraphElement, Vertex, ID};

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

#[derive(Clone, Debug)]
pub enum GraphPath {
    AllV(Vec<VertexOrEdge>),
    SimpleAllV((Vec<VertexOrEdge>, HashSet<ID>)),
    EndV((VertexOrEdge, usize)),
    SimpleEndV((VertexOrEdge, HashSet<ID>)),
}

impl GraphPath {
    pub fn new<E: Into<VertexOrEdge>>(entry: E, path_opt: PathOpt, result_opt: ResultOpt) -> Self {
        match result_opt {
            ResultOpt::EndV => match path_opt {
                PathOpt::Arbitrary => GraphPath::EndV((entry.into(), 1)),
                PathOpt::Simple => {
                    let mut set = HashSet::new();
                    let entry = entry.into();
                    set.insert(entry.id());
                    GraphPath::SimpleEndV((entry, set))
                }
            },
            ResultOpt::AllV => match path_opt {
                PathOpt::Arbitrary => GraphPath::AllV(vec![entry.into()]),
                PathOpt::Simple => {
                    let mut set = HashSet::new();
                    let entry = entry.into();
                    set.insert(entry.id());
                    GraphPath::SimpleAllV((vec![entry], set))
                }
            },
        }
    }

    // append an entry and return the flag of whether the entry has been appended or not.
    pub fn append<E: Into<VertexOrEdge>>(&mut self, entry: E) -> bool {
        match self {
            GraphPath::AllV(ref mut path) => {
                path.push(entry.into());
                true
            }
            GraphPath::SimpleAllV((ref mut path, ref mut set)) => {
                let entry = entry.into();
                if set.contains(&entry.id()) {
                    false
                } else {
                    set.insert(entry.id());
                    path.push(entry);
                    true
                }
            }
            GraphPath::EndV((ref mut e, ref mut weight)) => {
                *e = entry.into();
                *weight += 1;
                true
            }
            GraphPath::SimpleEndV((ref mut e, ref mut set)) => {
                let entry = entry.into();
                if set.contains(&entry.id()) {
                    false
                } else {
                    set.insert(entry.id());
                    *e = entry.into();
                    true
                }
            }
        }
    }

    pub fn get_path_end(&self) -> Option<&VertexOrEdge> {
        match self {
            GraphPath::AllV(ref p) | GraphPath::SimpleAllV((ref p, _)) => p.last(),
            GraphPath::EndV((ref e, _)) | GraphPath::SimpleEndV((ref e, _)) => Some(e),
        }
    }

    pub fn take_path(self) -> Option<Vec<VertexOrEdge>> {
        match self {
            GraphPath::AllV(p) | GraphPath::SimpleAllV((p, _)) => Some(p),
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
    fn details(&self) -> Option<&DynDetails> {
        match self {
            VertexOrEdge::V(v) => v.details(),
            VertexOrEdge::E(e) => e.details(),
        }
    }
}

impl Element for GraphPath {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
    }

    // the path len is the number of vertices in the path;
    fn len(&self) -> usize {
        match self {
            GraphPath::AllV(p) | GraphPath::SimpleAllV((p, _)) => p.len(),
            GraphPath::EndV((_, weight)) => *weight,
            GraphPath::SimpleEndV((_, set)) => set.len(),
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl GraphElement for GraphPath {
    fn id(&self) -> ID {
        match self {
            GraphPath::AllV(path) | GraphPath::SimpleAllV((path, _)) => {
                let ids: Vec<ID> = path.iter().map(|v| v.id()).collect();
                let mut hasher = DefaultHasher::new();
                ids.hash(&mut hasher);
                hasher.finish() as ID
            }
            GraphPath::EndV((path_end, _)) | GraphPath::SimpleEndV((path_end, _)) => path_end.id(),
        }
    }

    fn label(&self) -> Option<LabelId> {
        None
    }
}

impl PartialEq for GraphPath {
    fn eq(&self, other: &Self) -> bool {
        // We define eq by structure, ignoring path weight
        match (self, other) {
            (GraphPath::AllV(p1), GraphPath::AllV(p2))
            | (GraphPath::AllV(p1), GraphPath::SimpleAllV((p2, _)))
            | (GraphPath::SimpleAllV((p1, _)), GraphPath::AllV(p2))
            | (GraphPath::SimpleAllV((p1, _)), GraphPath::SimpleAllV((p2, _))) => p1.eq(p2),
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
            (GraphPath::AllV(p1), GraphPath::AllV(p2))
            | (GraphPath::AllV(p1), GraphPath::SimpleAllV((p2, _)))
            | (GraphPath::SimpleAllV((p1, _)), GraphPath::AllV(p2))
            | (GraphPath::SimpleAllV((p1, _)), GraphPath::SimpleAllV((p2, _))) => p1.partial_cmp(p2),
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
            GraphPath::AllV(path) => {
                writer.write_u8(0)?;
                path.write_to(writer)?;
            }
            GraphPath::EndV((path_end, weight)) => {
                writer.write_u8(1)?;
                path_end.write_to(writer)?;
                writer.write_u64(*weight as u64)?;
            }
            GraphPath::SimpleAllV((path, set)) => {
                writer.write_u8(2)?;
                path.write_to(writer)?;
                writer.write_u64(set.len() as u64)?;
                for id in set {
                    write_id(writer, *id)?;
                }
            }
            GraphPath::SimpleEndV((path_end, set)) => {
                writer.write_u8(3)?;
                path_end.write_to(writer)?;
                writer.write_u64(set.len() as u64)?;
                for id in set {
                    write_id(writer, *id)?;
                }
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
                Ok(GraphPath::AllV(path))
            }
            1 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                let weight = <u64>::read_from(reader)? as usize;
                Ok(GraphPath::EndV((vertex_or_edge, weight)))
            }
            2 => {
                let path = <Vec<VertexOrEdge>>::read_from(reader)?;
                let length = <u64>::read_from(reader)?;
                let mut set = HashSet::with_capacity(length as usize);
                for _i in 0..length {
                    let id = read_id(reader)?;
                    set.insert(id);
                }
                Ok(GraphPath::SimpleAllV((path, set)))
            }
            3 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                let length = <u64>::read_from(reader)?;
                let mut set = HashSet::with_capacity(length as usize);
                for _i in 0..length {
                    let id = read_id(reader)?;
                    set.insert(id);
                }
                Ok(GraphPath::SimpleEndV((vertex_or_edge, set)))
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
        Ok(GraphPath::AllV(graph_path))
    }
}

impl Hash for GraphPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            GraphPath::AllV(p) => p.hash(state),
            GraphPath::SimpleAllV((p, _)) => p.hash(state),
            GraphPath::EndV((e, _)) => e.hash(state),
            GraphPath::SimpleEndV((e, _)) => e.hash(state),
        }
    }
}
