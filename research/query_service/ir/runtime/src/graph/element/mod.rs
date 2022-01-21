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

use std::fmt::Debug;
use std::hash::Hash;

use dyn_type::{BorrowObject, Object};
pub use edge::Edge;
use ir_common::NameOrId;
pub use path::GraphPath;
pub use path::VertexOrEdge;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
pub use vertex::Vertex;

use crate::graph::property::DynDetails;
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
pub enum GraphObject {
    V(Vertex),
    E(Edge),
    P(GraphPath),
}

impl From<Vertex> for GraphObject {
    fn from(v: Vertex) -> Self {
        GraphObject::V(v.into())
    }
}

impl From<Edge> for GraphObject {
    fn from(e: Edge) -> Self {
        GraphObject::E(e.into())
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
            GraphObject::V(v) => v.details(),
            GraphObject::E(e) => e.details(),
            GraphObject::P(p) => p.details(),
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            GraphObject::V(v) => v.as_borrow_object(),
            GraphObject::E(e) => e.as_borrow_object(),
            GraphObject::P(p) => p.as_borrow_object(),
        }
    }
}

impl GraphElement for GraphObject {
    fn id(&self) -> u64 {
        match self {
            GraphObject::V(v) => v.id(),
            GraphObject::E(e) => e.id(),
            GraphObject::P(p) => p.id(),
        }
    }

    fn label(&self) -> Option<&NameOrId> {
        match self {
            GraphObject::V(v) => v.label(),
            GraphObject::E(e) => e.label(),
            GraphObject::P(p) => p.label(),
        }
    }

    fn len(&self) -> usize {
        match self {
            GraphObject::V(v) => v.len(),
            GraphObject::E(e) => e.len(),
            GraphObject::P(p) => p.len(),
        }
    }
}

impl Encode for GraphObject {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            GraphObject::V(v) => {
                writer.write_u8(0)?;
                v.write_to(writer)?;
            }
            GraphObject::E(e) => {
                writer.write_u8(1)?;
                e.write_to(writer)?;
            }
            GraphObject::P(p) => {
                writer.write_u8(2)?;
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
                let v = <Vertex>::read_from(reader)?;
                Ok(GraphObject::V(v))
            }
            1 => {
                let e = <Edge>::read_from(reader)?;
                Ok(GraphObject::E(e))
            }
            2 => {
                let path = <GraphPath>::read_from(reader)?;
                Ok(GraphObject::P(path))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}
