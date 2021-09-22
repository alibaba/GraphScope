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

pub use edge::Edge;
pub use vertex::Vertex;

use crate::graph::property::{DynDetails, Label, ID};
use dyn_type::{BorrowObject, Object};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use std::fmt::Debug;
use std::io;

/// A field that is further a graph element
pub trait Element {
    fn id(&self) -> Option<ID> {
        None
    }

    fn label(&self) -> Option<&Label> {
        None
    }

    fn details(&self) -> Option<&DynDetails> {
        None
    }

    fn as_borrow_object(&self) -> BorrowObject;
}

impl Element for () {
    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::String("")
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

mod edge;
mod vertex;

#[derive(Clone)]
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
    fn id(&self) -> Option<ID> {
        match self {
            VertexOrEdge::V(v) => v.id(),
            VertexOrEdge::E(e) => e.id(),
        }
    }

    fn label(&self) -> Option<&Label> {
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

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            VertexOrEdge::V(v) => v.as_borrow_object(),
            VertexOrEdge::E(e) => e.as_borrow_object(),
        }
    }
}

impl Debug for VertexOrEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VertexOrEdge::V(v) => write!(f, "v[{:?}]", v.id().unwrap()),
            VertexOrEdge::E(e) => write!(f, "e[{:?}]", e.id().unwrap()),
        }
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
