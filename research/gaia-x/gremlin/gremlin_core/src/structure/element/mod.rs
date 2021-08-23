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

use crate::structure::property::DynDetails;
use dyn_type::object::Primitives;
use dyn_type::Object;
pub use edge::Edge;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::fmt::Debug;
use std::io;
use std::ops::{Deref, DerefMut};
pub use vertex::Vertex;

/// The type of either vertex or edge id
pub type ID = u128;
/// The type of LabelId defined in Runtime
pub type LabelId = u8;

/// The number of bits in an `ID`
pub const ID_BITS: usize = std::mem::size_of::<ID>() * 8;

pub fn write_id<W: WriteExt>(id: ID, writer: &mut W) -> io::Result<()> {
    writer.write_u128(id)
}
pub fn read_id<R: ReadExt>(reader: &mut R) -> io::Result<ID> {
    reader.read_u128()
}

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum Label {
    Str(String),
    Id(LabelId),
}

impl Label {
    pub fn as_object(&self) -> Object {
        match self {
            Label::Str(s) => Object::String(s.to_string()),
            Label::Id(id) => Object::Primitive(Primitives::Integer(*id as i32)),
        }
    }
}

impl Encode for Label {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Label::Id(id) => {
                writer.write_u8(0)?;
                writer.write_u8(*id)?;
            }
            Label::Str(str) => {
                writer.write_u8(1)?;
                str.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for Label {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let e = reader.read_u8()?;
        match e {
            0 => {
                let label_id = reader.read_u8()?;
                Ok(Label::Id(label_id))
            }
            1 => {
                let str = <String>::read_from(reader)?;
                Ok(Label::Str(str))
            }
            _ => Err(io::Error::new(io::ErrorKind::Other, "unreachable")),
        }
    }
}

#[enum_dispatch]
pub trait Element {
    fn id(&self) -> ID;

    fn label(&self) -> &Label;

    fn details(&self) -> &DynDetails;
}

mod edge;
mod vertex;

#[enum_dispatch(Element)]
#[derive(Clone)]
pub enum VertexOrEdge {
    V(Vertex),
    E(Edge),
}

impl Debug for VertexOrEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VertexOrEdge::V(v) => write!(f, "v[{}]", v.id),
            VertexOrEdge::E(e) => write!(f, "e[{}]", e.id),
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

#[derive(Clone)]
pub struct GraphElement {
    element: VertexOrEdge,
    attached: Option<Object>,
}

impl Element for GraphElement {
    fn id(&self) -> ID {
        self.element.id()
    }

    fn label(&self) -> &Label {
        self.element.label()
    }

    fn details(&self) -> &DynDetails {
        self.element.details()
    }
}

impl Deref for GraphElement {
    type Target = VertexOrEdge;

    fn deref(&self) -> &Self::Target {
        &self.element
    }
}

impl DerefMut for GraphElement {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.element
    }
}

impl GraphElement {
    pub fn get(&self) -> &VertexOrEdge {
        &self.element
    }

    pub fn get_mut(&mut self) -> &mut VertexOrEdge {
        &mut self.element
    }

    #[inline]
    pub fn get_attached(&self) -> Option<&Object> {
        self.attached.as_ref()
    }

    #[inline]
    pub fn get_attached_mut(&mut self) -> Option<&mut Object> {
        self.attached.as_mut()
    }

    #[inline]
    pub fn attach<O: Into<Object>>(&mut self, obj: O) {
        self.attached = Some(obj.into())
    }
}

impl Debug for GraphElement {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self.element {
            VertexOrEdge::V(v) => write!(f, "v[{}]", v.id),
            VertexOrEdge::E(e) => write!(f, "e[{}]", e.id),
        }
    }
}

impl From<Vertex> for GraphElement {
    fn from(v: Vertex) -> Self {
        GraphElement { element: v.into(), attached: None }
    }
}

impl From<Edge> for GraphElement {
    fn from(v: Edge) -> Self {
        GraphElement { element: v.into(), attached: None }
    }
}

impl PartialEq<Vertex> for GraphElement {
    fn eq(&self, other: &Vertex) -> bool {
        match self.get() {
            VertexOrEdge::V(v) => v.id == other.id,
            VertexOrEdge::E(_) => false,
        }
    }
}

impl PartialEq<Edge> for GraphElement {
    fn eq(&self, other: &Edge) -> bool {
        match self.get() {
            VertexOrEdge::V(_) => false,
            VertexOrEdge::E(e) => e.id == other.id,
        }
    }
}

impl PartialEq for GraphElement {
    fn eq(&self, other: &Self) -> bool {
        match self.get() {
            VertexOrEdge::V(e) => match other.get() {
                VertexOrEdge::V(o) => e.id == o.id,
                VertexOrEdge::E(_) => false,
            },
            VertexOrEdge::E(e) => match other.get() {
                VertexOrEdge::V(_) => false,
                VertexOrEdge::E(o) => e.id == o.id,
            },
        }
    }
}

impl Encode for GraphElement {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        self.element.write_to(writer)?;
        self.attached.write_to(writer)?;
        Ok(())
    }
}

impl Decode for GraphElement {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let element = <VertexOrEdge>::read_from(reader)?;
        let attached = <Option<Object>>::read_from(reader)?;
        Ok(GraphElement { element, attached })
    }
}
