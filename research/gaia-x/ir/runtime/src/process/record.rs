//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use crate::error::DynIter;
use crate::expr::eval::Context;
use crate::graph::element::{Edge, Element, Vertex, VertexOrEdge};
use crate::graph::property::DynDetails;
use dyn_type::{BorrowObject, Object};
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub enum ObjectElement {
    // TODO: common-used object elements
    None,
    /// projected property
    Prop(Object),
    /// count
    Count(u64),
    /// aggregate of sum/max/min/avg
    Agg(Object),
}

#[derive(Debug, Clone)]
pub enum RecordElement {
    OnGraph(VertexOrEdge),
    OutGraph(ObjectElement),
}

#[derive(Debug, Clone)]
pub enum Entry {
    Element(RecordElement),
    Collection(Vec<RecordElement>),
}

// TODO
pub trait Columns {
    fn get(tag: &NameOrId) -> Option<&Entry>;
}

#[derive(Debug, Clone, Default)]
pub struct Record {
    curr: Option<Entry>,
    // TODO: optimized as VecMap<Entry>
    columns: HashMap<NameOrId, Entry>,
}

impl Record {
    pub fn new<E: Into<Entry>>(entry: E, tag: Option<NameOrId>) -> Self {
        if let Some(tag) = tag {
            let mut columns = HashMap::new();
            columns.insert(tag, entry.into());
            Record {
                curr: None,
                columns,
            }
        } else {
            Record {
                curr: Some(entry.into()),
                ..Self::default()
            }
        }
    }

    pub fn append<E: Into<Entry>>(&mut self, entry: E, tag: Option<NameOrId>) {
        if let Some(tag) = tag {
            self.columns.insert(tag, entry.into());
        } else {
            self.curr = Some(entry.into());
        }
    }

    pub fn take(&mut self, tag: Option<&NameOrId>) -> Option<Entry> {
        if let Some(tag) = tag {
            self.columns.remove(tag)
        } else {
            self.curr.take()
        }
    }

    pub fn get(&self, tag: Option<&NameOrId>) -> Option<&Entry> {
        if let Some(tag) = tag {
            self.columns.get(tag)
        } else {
            self.curr.as_ref()
        }
    }

    pub fn take_graph_entry(mut self, tag: Option<&NameOrId>) -> Option<VertexOrEdge> {
        let entry = self.take(tag);
        match entry {
            Some(Entry::Element(RecordElement::OnGraph(element))) => Some(element),
            _ => None,
        }
    }

    pub fn get_graph_entry(&self, tag: Option<&NameOrId>) -> Option<&VertexOrEdge> {
        let entry = self.get(tag);
        match entry {
            Some(Entry::Element(RecordElement::OnGraph(element))) => Some(element),
            _ => None,
        }
    }

    pub fn join(mut self, mut other: Record) -> Record {
        // TODO: check if head is also needed?
        for column in other.columns.drain() {
            self.columns.insert(column.0, column.1);
        }
        self
    }
}

impl Into<Entry> for Vertex {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(self.into()))
    }
}

impl Into<Entry> for Edge {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(self.into()))
    }
}

impl Into<Entry> for VertexOrEdge {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(self))
    }
}

impl Into<Entry> for ObjectElement {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OutGraph(self))
    }
}

impl Into<Entry> for RecordElement {
    fn into(self) -> Entry {
        Entry::Element(self)
    }
}

impl Context<RecordElement> for Record {
    fn get(&self, tag: Option<&NameOrId>) -> Option<&RecordElement> {
        self.get(tag)
            .map(|entry| match entry {
                Entry::Element(element) => Some(element),
                Entry::Collection(_) => None,
            })
            .unwrap_or(None)
    }
}

impl Element for RecordElement {
    fn id(&self) -> Option<u128> {
        match self {
            RecordElement::OnGraph(vertex_or_edge) => vertex_or_edge.id(),
            RecordElement::OutGraph(_) => None,
        }
    }

    fn label(&self) -> Option<&NameOrId> {
        match self {
            RecordElement::OnGraph(vertex_or_edge) => vertex_or_edge.label(),
            RecordElement::OutGraph(_) => None,
        }
    }

    fn details(&self) -> Option<&DynDetails> {
        match self {
            RecordElement::OnGraph(vertex_or_edge) => vertex_or_edge.details(),
            RecordElement::OutGraph(_) => None,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            RecordElement::OnGraph(vertex_or_edge) => vertex_or_edge.as_borrow_object(),
            RecordElement::OutGraph(obj_element) => match obj_element {
                ObjectElement::None => BorrowObject::String(""),
                ObjectElement::Prop(obj) | ObjectElement::Agg(obj) => obj.as_borrow(),
                ObjectElement::Count(cnt) => (*cnt).into(),
            },
        }
    }
}

/// RecordKey is the key fields of a Record, with each key corresponding to a request column_tag
#[derive(Clone, Debug)]
pub struct RecordKey {
    key_fields: Vec<RecordElement>,
}

impl RecordKey {
    pub fn new(key_fields: Vec<RecordElement>) -> Self {
        RecordKey { key_fields }
    }
}

impl Hash for RecordElement {
    fn hash<H: Hasher>(&self, mut state: &mut H) {
        match self {
            RecordElement::OnGraph(v) => v
                .id()
                .expect("id of VertexOrEdge cannot be None")
                .hash(&mut state),
            RecordElement::OutGraph(o) => match o {
                ObjectElement::None => "".hash(&mut state),
                ObjectElement::Prop(o) => o.hash(&mut state),
                ObjectElement::Count(o) => o.hash(&mut state),
                ObjectElement::Agg(o) => o.hash(&mut state),
            },
        }
    }
}

impl Hash for RecordKey {
    fn hash<H: Hasher>(&self, mut state: &mut H) {
        self.key_fields.hash(&mut state)
    }
}

impl PartialEq for RecordElement {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                RecordElement::OnGraph(VertexOrEdge::V(v1)),
                RecordElement::OnGraph(VertexOrEdge::V(v2)),
            ) => v1.id() == v2.id(),
            (
                RecordElement::OnGraph(VertexOrEdge::E(e1)),
                RecordElement::OnGraph(VertexOrEdge::E(e2)),
            ) => e1.id() == e2.id(),
            (
                RecordElement::OutGraph(ObjectElement::Prop(o1)),
                RecordElement::OutGraph(ObjectElement::Prop(o2)),
            ) => o1 == o2,
            (
                RecordElement::OutGraph(ObjectElement::Count(o1)),
                RecordElement::OutGraph(ObjectElement::Count(o2)),
            ) => o1 == o2,
            (
                RecordElement::OutGraph(ObjectElement::Agg(o1)),
                RecordElement::OutGraph(ObjectElement::Agg(o2)),
            ) => o1 == o2,
            (
                RecordElement::OutGraph(ObjectElement::None),
                RecordElement::OutGraph(ObjectElement::None),
            ) => true,
            _ => false,
        }
    }
}

impl PartialEq for RecordKey {
    fn eq(&self, other: &Self) -> bool {
        self.key_fields == other.key_fields
    }
}

impl Eq for RecordKey {}

pub struct RecordExpandIter<E> {
    tag: Option<NameOrId>,
    origin: Record,
    children: DynIter<E>,
}

impl<E> RecordExpandIter<E> {
    pub fn new(origin: Record, tag: Option<&NameOrId>, children: DynIter<E>) -> Self {
        RecordExpandIter {
            tag: tag.map(|e| e.clone()),
            origin,
            children,
        }
    }
}

impl<E: Into<VertexOrEdge>> Iterator for RecordExpandIter<E> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.origin.clone();
        match self.children.next() {
            Some(elem) => {
                record.append(elem.into(), self.tag.clone());
                Some(record)
            }
            None => None,
        }
    }
}

impl Encode for ObjectElement {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            ObjectElement::None => {
                writer.write_u8(0)?;
            }
            ObjectElement::Prop(prop) => {
                writer.write_u8(1)?;
                prop.write_to(writer)?;
            }
            ObjectElement::Count(cnt) => {
                writer.write_u8(2)?;
                writer.write_u64(*cnt)?;
            }
            ObjectElement::Agg(agg) => {
                writer.write_u8(3)?;
                agg.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for ObjectElement {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => Ok(ObjectElement::None),
            1 => {
                let object = <Object>::read_from(reader)?;
                Ok(ObjectElement::Prop(object))
            }
            2 => {
                let cnt = <u64>::read_from(reader)?;
                Ok(ObjectElement::Count(cnt))
            }
            3 => {
                let object = <Object>::read_from(reader)?;
                Ok(ObjectElement::Agg(object))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unreachable",
            )),
        }
    }
}

impl Encode for RecordElement {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RecordElement::OnGraph(vertex_or_edge) => {
                writer.write_u8(0)?;
                vertex_or_edge.write_to(writer)?;
            }
            RecordElement::OutGraph(object_element) => {
                writer.write_u8(1)?;
                object_element.write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl Decode for RecordElement {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let vertex_or_edge = <VertexOrEdge>::read_from(reader)?;
                Ok(RecordElement::OnGraph(vertex_or_edge))
            }
            1 => {
                let object_element = <ObjectElement>::read_from(reader)?;
                Ok(RecordElement::OutGraph(object_element))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unreachable",
            )),
        }
    }
}

impl Encode for Entry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Entry::Element(element) => {
                writer.write_u8(0)?;
                element.write_to(writer)?
            }
            Entry::Collection(collection) => {
                writer.write_u8(1)?;
                collection.write_to(writer)?
            }
        }
        Ok(())
    }
}

impl Decode for Entry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => {
                let record = <RecordElement>::read_from(reader)?;
                Ok(Entry::Element(record))
            }
            1 => {
                let collection = <Vec<RecordElement>>::read_from(reader)?;
                Ok(Entry::Collection(collection))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "unreachable",
            )),
        }
    }
}

impl Encode for Record {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match &self.curr {
            None => {
                writer.write_u8(0)?;
            }
            Some(entry) => {
                writer.write_u8(1)?;
                entry.write_to(writer)?;
            }
        }
        writer.write_u64(self.columns.len() as u64)?;
        for (k, v) in self.columns.iter() {
            k.write_to(writer)?;
            v.write_to(writer)?;
        }
        Ok(())
    }
}

impl Decode for Record {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        let curr = if opt == 0 {
            None
        } else {
            Some(<Entry>::read_from(reader)?)
        };
        let size = <u64>::read_from(reader)? as usize;
        let mut columns = HashMap::with_capacity(size);
        for _i in 0..size {
            let k = <NameOrId>::read_from(reader)?;
            let v = <Entry>::read_from(reader)?;
            columns.insert(k, v);
        }
        Ok(Record { curr, columns })
    }
}

impl Encode for RecordKey {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        Ok(self.key_fields.write_to(writer)?)
    }
}

impl Decode for RecordKey {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let key_fields = <Vec<RecordElement>>::read_from(reader)?;
        Ok(RecordKey { key_fields })
    }
}
