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

use std::any::Any;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ahash::HashMap;
use dyn_type::{BorrowObject, Object};
use graph_proxy::apis::{Edge, Element, GraphElement, GraphPath, PropertyValue, Vertex, ID};
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::process::operator::map::IntersectionEntry;

#[derive(Debug, PartialEq)]
pub enum EntryType {
    /// Graph Vertex
    Vertex,
    /// Graph Edge
    Edge,
    /// Graph Path
    Path,
    /// Common data type of `Object`, including Primitives, String, etc.
    Object,
    /// A specific type used in `ExtendIntersect`, for an optimized implementation of `Intersection`
    Intersection,
    /// Type of collection consisting of entries
    Collection,
}

pub trait Entry: Debug + Send + Sync + AsAny + Element {
    fn get_type(&self) -> EntryType;

    fn as_vertex(&self) -> Option<&Vertex> {
        None
    }
    fn as_edge(&self) -> Option<&Edge> {
        None
    }
    fn as_graph_path(&self) -> Option<&GraphPath> {
        None
    }
    fn as_object(&self) -> Option<&Object> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct DynEntry {
    inner: Arc<dyn Entry>,
}

impl AsAny for DynEntry {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        // If you want to make self.inner as mutable,try Arc::get_mut(&mut self.inner) first. i.e.,
        // Arc::get_mut(&mut self.inner)
        //     .unwrap()
        //     .as_any_mut()
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self.inner.as_any_ref()
    }
}

impl DynEntry {
    pub fn new<E: Entry + 'static>(entry: E) -> Self {
        DynEntry { inner: Arc::new(entry) }
    }

    pub fn get_mut(&mut self) -> Option<&mut dyn Entry> {
        Arc::get_mut(&mut self.inner)
    }

    pub fn is_none(&self) -> bool {
        match self.get_type() {
            EntryType::Object => self
                .as_object()
                .map(|obj| obj.eq(&Object::None))
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl Entry for DynEntry {
    fn get_type(&self) -> EntryType {
        self.inner.get_type()
    }

    fn as_vertex(&self) -> Option<&Vertex> {
        self.inner.as_vertex()
    }

    fn as_edge(&self) -> Option<&Edge> {
        self.inner.as_edge()
    }

    fn as_graph_path(&self) -> Option<&GraphPath> {
        self.inner.as_graph_path()
    }

    fn as_object(&self) -> Option<&Object> {
        self.inner.as_object()
    }
}

impl Encode for DynEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        let entry_type = self.get_type();
        match entry_type {
            EntryType::Vertex => {
                writer.write_u8(1)?;
                self.as_vertex().unwrap().write_to(writer)?;
            }
            EntryType::Edge => {
                writer.write_u8(2)?;
                self.as_edge().unwrap().write_to(writer)?;
            }
            EntryType::Path => {
                writer.write_u8(3)?;
                self.as_graph_path().unwrap().write_to(writer)?;
            }
            EntryType::Object => {
                writer.write_u8(4)?;
                self.as_object().unwrap().write_to(writer)?;
            }
            EntryType::Intersection => {
                writer.write_u8(5)?;
                self.inner
                    .as_any_ref()
                    .downcast_ref::<IntersectionEntry>()
                    .unwrap()
                    .write_to(writer)?;
            }
            EntryType::Collection => {
                writer.write_u8(6)?;
                self.inner
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .unwrap()
                    .write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for DynEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let entry_type = reader.read_u8()?;
        match entry_type {
            1 => {
                let vertex = Vertex::read_from(reader)?;
                Ok(DynEntry::new(vertex))
            }
            2 => {
                let edge = Edge::read_from(reader)?;
                Ok(DynEntry::new(edge))
            }
            3 => {
                let path = GraphPath::read_from(reader)?;
                Ok(DynEntry::new(path))
            }
            4 => {
                let obj = Object::read_from(reader)?;
                Ok(DynEntry::new(obj))
            }
            5 => {
                let intersect = IntersectionEntry::read_from(reader)?;
                Ok(DynEntry::new(intersect))
            }
            6 => {
                let collection = CollectionEntry::read_from(reader)?;
                Ok(DynEntry::new(collection))
            }
            _ => unreachable!(),
        }
    }
}

impl Element for DynEntry {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        self.inner.as_graph_element()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn as_borrow_object(&self) -> BorrowObject {
        self.inner.as_borrow_object()
    }
}

impl GraphElement for DynEntry {
    fn id(&self) -> ID {
        match self.get_type() {
            EntryType::Vertex | EntryType::Edge => self.inner.as_graph_element().unwrap().id(),
            _ => unreachable!(),
        }
    }

    fn label(&self) -> Option<i32> {
        match self.get_type() {
            EntryType::Vertex | EntryType::Edge => self.inner.as_graph_element().unwrap().label(),
            _ => None,
        }
    }

    fn get_property(&self, key: &NameOrId) -> Option<PropertyValue> {
        match self.get_type() {
            EntryType::Vertex | EntryType::Edge => self
                .inner
                .as_graph_element()
                .unwrap()
                .get_property(key),
            _ => unreachable!(),
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        match self.get_type() {
            EntryType::Vertex | EntryType::Edge => self
                .inner
                .as_graph_element()
                .unwrap()
                .get_all_properties(),
            _ => unreachable!(),
        }
    }
}

// demanded when need to key the entry
impl Hash for DynEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.get_type() {
            EntryType::Vertex => self.as_vertex().hash(state),
            EntryType::Edge => self.as_edge().hash(state),
            EntryType::Path => self.as_graph_path().hash(state),
            EntryType::Object => self.as_object().hash(state),
            EntryType::Intersection => self
                .as_any_ref()
                .downcast_ref::<IntersectionEntry>()
                .hash(state),
            EntryType::Collection => self
                .as_any_ref()
                .downcast_ref::<CollectionEntry>()
                .hash(state),
        }
    }
}

// demanded when need to key the entry; and order the entry;
impl PartialEq for DynEntry {
    fn eq(&self, other: &Self) -> bool {
        if (self.get_type()).eq(&other.get_type()) {
            match self.get_type() {
                EntryType::Vertex => self.as_vertex().eq(&other.as_vertex()),
                EntryType::Edge => self.as_edge().eq(&other.as_edge()),
                EntryType::Path => self.as_graph_path().eq(&other.as_graph_path()),
                EntryType::Object => self.as_object().eq(&other.as_object()),
                EntryType::Intersection => self
                    .as_any_ref()
                    .downcast_ref::<IntersectionEntry>()
                    .eq(&other
                        .as_any_ref()
                        .downcast_ref::<IntersectionEntry>()),
                EntryType::Collection => self
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .eq(&other
                        .as_any_ref()
                        .downcast_ref::<CollectionEntry>()),
            }
        } else {
            false
        }
    }
}

// demanded when need to  order the entry;
impl PartialOrd for DynEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if (self.get_type()).eq(&other.get_type()) {
            match self.get_type() {
                EntryType::Vertex => self.as_vertex().partial_cmp(&other.as_vertex()),
                EntryType::Edge => self.as_edge().partial_cmp(&other.as_edge()),
                EntryType::Path => self
                    .as_graph_path()
                    .partial_cmp(&other.as_graph_path()),
                EntryType::Object => self.as_object().partial_cmp(&other.as_object()),
                EntryType::Intersection => self
                    .as_any_ref()
                    .downcast_ref::<IntersectionEntry>()
                    .partial_cmp(
                        &other
                            .as_any_ref()
                            .downcast_ref::<IntersectionEntry>(),
                    ),
                EntryType::Collection => self
                    .as_any_ref()
                    .downcast_ref::<CollectionEntry>()
                    .partial_cmp(
                        &other
                            .as_any_ref()
                            .downcast_ref::<CollectionEntry>(),
                    ),
            }
        } else {
            None
        }
    }
}

// demanded when need to group (ToSet) the entry;
impl Eq for DynEntry {}

impl Entry for Vertex {
    fn get_type(&self) -> EntryType {
        EntryType::Vertex
    }

    fn as_vertex(&self) -> Option<&Vertex> {
        Some(self)
    }
}

impl Entry for Edge {
    fn get_type(&self) -> EntryType {
        EntryType::Edge
    }

    fn as_edge(&self) -> Option<&Edge> {
        Some(self)
    }
}

impl Entry for Object {
    fn get_type(&self) -> EntryType {
        EntryType::Object
    }

    fn as_object(&self) -> Option<&Object> {
        Some(self)
    }
}

impl Entry for IntersectionEntry {
    fn get_type(&self) -> EntryType {
        EntryType::Intersection
    }
}

impl Entry for GraphPath {
    fn get_type(&self) -> EntryType {
        EntryType::Path
    }

    fn as_graph_path(&self) -> Option<&GraphPath> {
        Some(self)
    }
}

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Eq, Hash)]
pub struct CollectionEntry {
    pub inner: Vec<DynEntry>,
}

impl_as_any!(CollectionEntry);

impl Entry for CollectionEntry {
    fn get_type(&self) -> EntryType {
        EntryType::Collection
    }
}

impl Element for CollectionEntry {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        None
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl Encode for CollectionEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.inner.write_to(writer)
    }
}

impl Decode for CollectionEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let inner = <Vec<DynEntry>>::read_from(reader)?;
        Ok(CollectionEntry { inner })
    }
}

impl TryFrom<result_pb::Element> for DynEntry {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Element) -> Result<Self, Self::Error> {
        if let Some(inner) = e.inner {
            match inner {
                result_pb::element::Inner::Vertex(v) => {
                    let vertex = Vertex::try_from(v)?;
                    Ok(DynEntry::new(vertex))
                }
                result_pb::element::Inner::Edge(e) => {
                    let edge = Edge::try_from(e)?;
                    Ok(DynEntry::new(edge))
                }
                result_pb::element::Inner::GraphPath(p) => {
                    let path = GraphPath::try_from(p)?;
                    Ok(DynEntry::new(path))
                }
                result_pb::element::Inner::Object(o) => {
                    let obj = Object::try_from(o)?;
                    Ok(DynEntry::new(obj))
                }
            }
        } else {
            Err(ParsePbError::EmptyFieldError("element inner is empty".to_string()))?
        }
    }
}

impl TryFrom<result_pb::Entry> for DynEntry {
    type Error = ParsePbError;

    fn try_from(entry_pb: result_pb::Entry) -> Result<Self, Self::Error> {
        if let Some(inner) = entry_pb.inner {
            match inner {
                result_pb::entry::Inner::Element(e) => Ok(e.try_into()?),
                result_pb::entry::Inner::Collection(c) => {
                    let collection = CollectionEntry {
                        inner: c
                            .collection
                            .into_iter()
                            .map(|e| e.try_into())
                            .collect::<Result<Vec<_>, Self::Error>>()?,
                    };
                    Ok(DynEntry::new(collection))
                }
            }
        } else {
            Err(ParsePbError::EmptyFieldError("entry inner is empty".to_string()))?
        }
    }
}

impl From<Vertex> for DynEntry {
    fn from(v: Vertex) -> Self {
        DynEntry::new(v)
    }
}

impl From<Edge> for DynEntry {
    fn from(e: Edge) -> Self {
        DynEntry::new(e)
    }
}

impl From<GraphPath> for DynEntry {
    fn from(p: GraphPath) -> Self {
        DynEntry::new(p)
    }
}

impl From<Object> for DynEntry {
    fn from(o: Object) -> Self {
        DynEntry::new(o)
    }
}

impl From<Vec<DynEntry>> for DynEntry {
    fn from(vec: Vec<DynEntry>) -> Self {
        let c = CollectionEntry { inner: vec };
        DynEntry::new(c)
    }
}

impl From<CollectionEntry> for DynEntry {
    fn from(c: CollectionEntry) -> Self {
        DynEntry::new(c)
    }
}
