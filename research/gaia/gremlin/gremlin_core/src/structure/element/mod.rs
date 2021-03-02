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
use crate::Object;
pub use edge::Edge;
use graph_store::common::LabelId;
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};
pub use vertex::Vertex;

pub type ID = u128;

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub enum Label {
    Str(String),
    Id(LabelId),
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

#[derive(Clone)]
pub struct GraphElement {
    element: VertexOrEdge,
    attached: Option<Object>,
}

impl Element for GraphElement {
    fn id(&self) -> u128 {
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
