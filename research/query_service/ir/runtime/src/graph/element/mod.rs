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

mod edge;
mod path;
mod vertex;

use std::convert::{TryFrom, TryInto};
use std::fmt::Debug;
use std::hash::Hash;
use std::io;

use dyn_type::{BorrowObject, Object};
pub use edge::Edge;
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
pub use path::GraphPath;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
pub use vertex::Vertex;

use crate::graph::property::{DefaultDetails, DynDetails};
use crate::graph::ID;

/// A field that is an element
pub trait Element {
    fn details(&self) -> Option<&DynDetails> {
        None
    }
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        None
    }
    fn as_borrow_object(&self) -> BorrowObject;
}

/// A field that is further a graph element
pub trait GraphElement: Element {
    fn id(&self) -> ID;
    fn label(&self) -> Option<&NameOrId>;
    fn len(&self) -> usize;
}

impl Element for () {
    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl Element for Object {
    fn as_borrow_object(&self) -> BorrowObject {
        self.as_borrow()
    }
}

impl<'a> Element for BorrowObject<'a> {
    fn as_borrow_object(&self) -> BorrowObject<'a> {
        *self
    }
}

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

impl Element for VertexOrEdge {
    fn details(&self) -> Option<&DynDetails> {
        match self {
            VertexOrEdge::V(v) => v.details(),
            VertexOrEdge::E(e) => e.details(),
        }
    }
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
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
        0
    }
}

impl Encode for VertexOrEdge {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
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
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
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
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl TryFrom<result_pb::Vertex> for VertexOrEdge {
    type Error = ParsePbError;
    fn try_from(v: result_pb::Vertex) -> Result<Self, Self::Error> {
        let vertex = Vertex::new(
            v.id as ID,
            v.label
                .map(|label| label.try_into())
                .transpose()?,
            DynDetails::new(DefaultDetails::default()),
        );
        Ok(VertexOrEdge::V(vertex))
    }
}

impl TryFrom<result_pb::Edge> for VertexOrEdge {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Edge) -> Result<Self, Self::Error> {
        let mut edge = Edge::new(
            e.id as ID,
            e.label
                .map(|label| label.try_into())
                .transpose()?,
            e.src_id as ID,
            e.dst_id as ID,
            DynDetails::new(DefaultDetails::default()),
        );
        edge.set_src_label(
            e.src_label
                .map(|label| label.try_into())
                .transpose()?,
        );
        edge.set_dst_label(
            e.dst_label
                .map(|label| label.try_into())
                .transpose()?,
        );
        Ok(VertexOrEdge::E(edge))
    }
}

#[derive(Clone, Debug, Hash, PartialEq, PartialOrd)]
pub enum GraphObject {
    VOrE(VertexOrEdge),
    P(GraphPath),
}

impl From<Vertex> for GraphObject {
    fn from(v: Vertex) -> Self {
        GraphObject::VOrE(v.into())
    }
}

impl From<Edge> for GraphObject {
    fn from(e: Edge) -> Self {
        GraphObject::VOrE(e.into())
    }
}

impl From<VertexOrEdge> for GraphObject {
    fn from(v: VertexOrEdge) -> Self {
        GraphObject::VOrE(v)
    }
}

impl From<GraphPath> for GraphObject {
    fn from(p: GraphPath) -> Self {
        GraphObject::P(p)
    }
}

impl Element for GraphObject {
    fn details(&self) -> Option<&DynDetails> {
        match self {
            GraphObject::VOrE(v_or_e) => v_or_e.details(),
            GraphObject::P(_p) => None,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            GraphObject::VOrE(v_or_e) => v_or_e.as_borrow_object(),
            GraphObject::P(_p) => BorrowObject::None,
        }
    }
}

impl GraphElement for GraphObject {
    fn id(&self) -> u64 {
        match self {
            GraphObject::VOrE(v_or_e) => v_or_e.id(),
            GraphObject::P(p) => p.id(),
        }
    }

    fn label(&self) -> Option<&NameOrId> {
        match self {
            GraphObject::VOrE(v_or_e) => v_or_e.label(),
            GraphObject::P(p) => p.label(),
        }
    }

    fn len(&self) -> usize {
        match self {
            GraphObject::VOrE(v_or_e) => v_or_e.len(),
            GraphObject::P(p) => p.len(),
        }
    }
}

impl Encode for GraphObject {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            GraphObject::VOrE(v_or_e) => {
                writer.write_u8(0)?;
                v_or_e.write_to(writer)?;
            }
            GraphObject::P(p) => {
                writer.write_u8(1)?;
                p.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for GraphObject {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                Ok(GraphObject::VOrE(vertex_or_edge))
            }
            1 => {
                let path = <GraphPath>::read_from(reader)?;
                Ok(GraphObject::P(path))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}
