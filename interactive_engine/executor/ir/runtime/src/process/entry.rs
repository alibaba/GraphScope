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

use dyn_type::{BorrowObject, Object};
use graph_proxy::apis::{
    read_id, write_id, DynDetails, Edge, Element, GraphElement, GraphPath, Vertex, ID,
};
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::process::operator::map::Intersection;

#[derive(Debug)]
pub enum EntryDataType {
    // TODO: A global id to denote current data; Currently, it is mostly vertex global id??
    Id,
    V,
    E,
    P,
    Obj,
    Intersect,
    Collection,
}

impl PartialEq for EntryDataType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (EntryDataType::Id, EntryDataType::Id)
            | (EntryDataType::Id, EntryDataType::V)
            | (EntryDataType::V, EntryDataType::Id)
            | (EntryDataType::V, EntryDataType::V)
            | (EntryDataType::E, EntryDataType::E)
            | (EntryDataType::P, EntryDataType::P)
            | (EntryDataType::Obj, EntryDataType::Obj)
            | (EntryDataType::Intersect, EntryDataType::Intersect)
            | (EntryDataType::Collection, EntryDataType::Collection) => true,
            _ => false,
        }
    }
}

pub trait Entry: Debug + Send + Sync + AsAny + Element {
    fn get_type(&self) -> EntryDataType;
    fn as_id(&self) -> Option<ID>;
    fn as_graph_vertex(&self) -> Option<&Vertex> {
        None
    }
    fn as_graph_edge(&self) -> Option<&Edge> {
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
            EntryDataType::Obj => self
                .as_object()
                .map(|obj| obj.eq(&Object::None))
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl Entry for DynEntry {
    fn get_type(&self) -> EntryDataType {
        self.inner.get_type()
    }

    fn as_id(&self) -> Option<u64> {
        self.inner.as_id()
    }

    fn as_graph_vertex(&self) -> Option<&Vertex> {
        self.inner.as_graph_vertex()
    }

    fn as_graph_edge(&self) -> Option<&Edge> {
        self.inner.as_graph_edge()
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
            EntryDataType::Id => {
                writer.write_u8(0)?;
                write_id(writer, self.as_id().unwrap())?;
            }
            EntryDataType::V => {
                writer.write_u8(1)?;
                self.as_graph_vertex()
                    .unwrap()
                    .write_to(writer)?;
            }
            EntryDataType::E => {
                writer.write_u8(2)?;
                self.as_graph_edge().unwrap().write_to(writer)?;
            }
            EntryDataType::P => {
                writer.write_u8(3)?;
                self.as_graph_path().unwrap().write_to(writer)?;
            }
            EntryDataType::Obj => {
                writer.write_u8(4)?;
                self.as_object().unwrap().write_to(writer)?;
            }
            EntryDataType::Intersect => {
                writer.write_u8(5)?;
                self.inner
                    .as_any_ref()
                    .downcast_ref::<Intersection>()
                    .unwrap()
                    .write_to(writer)?;
            }
            EntryDataType::Collection => {
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
            0 => {
                let id = read_id(reader)?;
                Ok(DynEntry::new(IDEntry { id }))
            }
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
                let intersect = Intersection::read_from(reader)?;
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
            EntryDataType::Id => self.as_id().unwrap(),
            EntryDataType::V => self.as_graph_vertex().unwrap().id(),
            EntryDataType::E => self.as_graph_edge().unwrap().id(),
            _ => unreachable!(),
        }
    }

    fn label(&self) -> Option<i32> {
        match self.get_type() {
            EntryDataType::Id => None,
            EntryDataType::V => self
                .as_graph_vertex()
                .map(|v| v.label())
                .unwrap_or(None),
            EntryDataType::E => self
                .as_graph_edge()
                .map(|v| v.label())
                .unwrap_or(None),
            _ => unreachable!(),
        }
    }

    fn details(&self) -> Option<&DynDetails> {
        match self.get_type() {
            EntryDataType::Id => None,
            EntryDataType::V => self
                .as_graph_vertex()
                .map(|v| v.details())
                .unwrap_or(None),
            EntryDataType::E => self
                .as_graph_edge()
                .map(|v| v.details())
                .unwrap_or(None),
            _ => unreachable!(),
        }
    }
}

// demanded when need to key the entry
impl Hash for DynEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.get_type() {
            EntryDataType::Id | EntryDataType::V => self.as_id().hash(state),
            EntryDataType::E => self.as_graph_edge().hash(state),
            EntryDataType::P => self.as_graph_path().hash(state),
            EntryDataType::Obj => self.as_object().hash(state),
            EntryDataType::Intersect => self
                .as_any_ref()
                .downcast_ref::<Intersection>()
                .hash(state),
            EntryDataType::Collection => self
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
                EntryDataType::Id | EntryDataType::V => self.as_id().eq(&other.as_id()),
                EntryDataType::E => self.as_graph_edge().eq(&other.as_graph_edge()),
                EntryDataType::P => self.as_graph_path().eq(&other.as_graph_path()),
                EntryDataType::Obj => self.as_object().eq(&other.as_object()),
                EntryDataType::Intersect => self
                    .as_any_ref()
                    .downcast_ref::<Intersection>()
                    .eq(&other
                        .as_any_ref()
                        .downcast_ref::<Intersection>()),
                EntryDataType::Collection => self
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
                EntryDataType::Id | EntryDataType::V => self.as_id().partial_cmp(&other.as_id()),
                EntryDataType::E => self
                    .as_graph_edge()
                    .partial_cmp(&other.as_graph_edge()),
                EntryDataType::P => self
                    .as_graph_path()
                    .partial_cmp(&other.as_graph_path()),
                EntryDataType::Obj => self.as_object().partial_cmp(&other.as_object()),
                EntryDataType::Intersect => self
                    .as_any_ref()
                    .downcast_ref::<Intersection>()
                    .partial_cmp(
                        &other
                            .as_any_ref()
                            .downcast_ref::<Intersection>(),
                    ),
                EntryDataType::Collection => self
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

// demanded when need to group (ToSum) the entry;
impl std::ops::Add for DynEntry {
    type Output = DynEntry;

    fn add(self, rhs: Self) -> Self::Output {
        if (self.get_type()).eq(&rhs.get_type()) {
            if EntryDataType::Obj.eq(&self.get_type()) {
                match (self.as_object(), rhs.as_object()) {
                    (Some(Object::Primitive(p1)), Some(Object::Primitive(p2))) => {
                        return DynEntry::new(Object::Primitive(p1.add(p2.clone())));
                    }
                    _ => {}
                }
            }
        }
        DynEntry::new(Object::None)
    }
}

#[derive(Debug, Clone, Default)]
pub struct IDEntry {
    id: ID,
}

impl_as_any!(IDEntry);

impl Entry for IDEntry {
    fn get_type(&self) -> EntryDataType {
        EntryDataType::Id
    }

    fn as_id(&self) -> Option<ID> {
        Some(self.id)
    }
}

impl Encode for IDEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        write_id(writer, self.id)
    }
}

impl Decode for IDEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let id = read_id(reader)?;
        Ok(IDEntry { id })
    }
}

impl Element for IDEntry {
    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        Some(self)
    }

    fn len(&self) -> usize {
        1
    }

    fn as_borrow_object(&self) -> BorrowObject {
        self.id.into()
    }
}

impl GraphElement for IDEntry {
    fn id(&self) -> ID {
        self.id
    }

    fn label(&self) -> Option<i32> {
        None
    }
}

impl Entry for Vertex {
    fn get_type(&self) -> EntryDataType {
        EntryDataType::V
    }

    fn as_id(&self) -> Option<u64> {
        Some(self.id())
    }

    fn as_graph_vertex(&self) -> Option<&Vertex> {
        Some(self)
    }
}

impl Entry for Edge {
    fn get_type(&self) -> EntryDataType {
        EntryDataType::E
    }

    fn as_id(&self) -> Option<ID> {
        Some(self.id())
    }

    fn as_graph_edge(&self) -> Option<&Edge> {
        Some(self)
    }
}

impl Entry for Object {
    fn get_type(&self) -> EntryDataType {
        EntryDataType::Obj
    }

    fn as_id(&self) -> Option<ID> {
        None
    }

    fn as_object(&self) -> Option<&Object> {
        Some(self)
    }
}

impl Entry for Intersection {
    fn get_type(&self) -> EntryDataType {
        EntryDataType::Intersect
    }

    fn as_id(&self) -> Option<u64> {
        None
    }
}

impl Entry for GraphPath {
    fn get_type(&self) -> EntryDataType {
        EntryDataType::P
    }

    fn as_id(&self) -> Option<u64> {
        None
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
    fn get_type(&self) -> EntryDataType {
        EntryDataType::Collection
    }

    fn as_id(&self) -> Option<u64> {
        None
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

impl From<CollectionEntry> for DynEntry {
    fn from(c: CollectionEntry) -> Self {
        DynEntry::new(c)
    }
}
