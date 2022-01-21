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

use std::cmp::Ordering;
use std::convert::{TryFrom, TryInto};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use dyn_type::{BorrowObject, Object};
use indexmap::map::IndexMap;
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::NameOrId;
use pegasus::api::function::DynIter;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::expr::eval::Context;
use crate::graph::element::{Edge, Element, GraphObject, GraphPath, Vertex, VertexOrEdge};
use crate::graph::property::DynDetails;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
// TODO: rename as CommonObject
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
    OnGraph(GraphObject),
    OffGraph(ObjectElement),
}

impl RecordElement {
    pub fn as_vertex_or_edge(&self) -> Option<&VertexOrEdge> {
        match self {
            RecordElement::OnGraph(GraphObject::VOrE(vertex_or_edge)) => Some(vertex_or_edge),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Entry {
    Element(RecordElement),
    Collection(Vec<RecordElement>),
}

impl Entry {
    // TODO: rename as as_vertex_or_edge
    pub fn as_graph_element(&self) -> Option<&VertexOrEdge> {
        match self {
            Entry::Element(record_element) => record_element.as_vertex_or_edge(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Record {
    curr: Option<Arc<Entry>>,
    // TODO: optimized as VecMap<Entry>
    columns: IndexMap<NameOrId, Arc<Entry>>,
}

impl Record {
    pub fn new<E: Into<Entry>>(entry: E, tag: Option<NameOrId>) -> Self {
        let entry = Arc::new(entry.into());
        let mut columns = IndexMap::new();
        if let Some(tag) = tag {
            columns.insert(tag.clone(), entry.clone());
        }
        Record { curr: Some(entry), columns }
    }

    // TODO: consider to maintain the record without any alias, which also needed to be stored;
    // We may: 1. define a None type in NameOrId; or 2. define different Record types, for the gremlin path requirement
    pub fn append<E: Into<Entry>>(&mut self, entry: E, alias: Option<NameOrId>) {
        self.append_arc_entry(Arc::new(entry.into()), alias)
    }

    pub fn append_arc_entry(&mut self, entry: Arc<Entry>, alias: Option<NameOrId>) {
        self.curr = Some(entry.clone());
        if let Some(alias) = alias {
            self.columns.insert(alias.clone(), entry);
        }
    }

    // append without moving curr entry
    pub fn append_without_moving_curr(&mut self, entry: Arc<Entry>, alias: Option<NameOrId>) {
        if let Some(alias) = alias {
            self.columns.insert(alias, entry);
        }
    }

    // append a path element on curr entry; notice that curr entry should be GraphPath
    // if curr is not a path, initial the path; else, insert the entry into the path
    pub fn append_path_element(&mut self, _entry: VertexOrEdge) {
        todo!()
    }

    pub fn get(&self, tag: Option<&NameOrId>) -> Option<&Arc<Entry>> {
        if let Some(tag) = tag {
            self.columns.get(tag)
        } else {
            self.curr.as_ref()
        }
    }

    /// To join this record with `other` record. After the join, the columns
    /// from both sides will be merged (and deduplicated). The head of the joined
    /// record will be specified according to `HeadJoinOpt`.
    pub fn join(mut self, mut other: Record, opt: Option<HeadJoinOpt>) -> Record {
        for column in other.columns.drain(..) {
            if !self.columns.contains_key(&column.0) {
                self.columns.insert(column.0, column.1);
            }
        }

        self.curr = match opt {
            None => None,
            Some(HeadJoinOpt::Left) => self.curr,
            Some(HeadJoinOpt::Right) => other.curr,
        };

        self
    }
}

/// `HeadJoinOpt` specifies which head of the recores are to be applied for the joined record
pub enum HeadJoinOpt {
    /// Store the head as the head of the left record of the join
    Left,
    /// Store the head as the head of the right record of the join
    Right,
}

impl Into<Entry> for Vertex {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(GraphObject::VOrE(self.into())))
    }
}

impl Into<Entry> for Edge {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(GraphObject::VOrE(self.into())))
    }
}

impl Into<Entry> for VertexOrEdge {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(GraphObject::VOrE(self)))
    }
}

impl Into<Entry> for GraphPath {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OnGraph(GraphObject::P(self)))
    }
}

impl Into<Entry> for ObjectElement {
    fn into(self) -> Entry {
        Entry::Element(RecordElement::OffGraph(self))
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
            .map(|entry| match entry.as_ref() {
                Entry::Element(element) => Some(element),
                Entry::Collection(_) => None,
            })
            .unwrap_or(None)
    }
}

impl Element for RecordElement {
    fn details(&self) -> Option<&DynDetails> {
        match self {
            RecordElement::OnGraph(GraphObject::VOrE(vertex_or_edge)) => vertex_or_edge.details(),
            RecordElement::OnGraph(GraphObject::P(_)) => None,
            RecordElement::OffGraph(_) => None,
        }
    }

    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        match self {
            RecordElement::OnGraph(graph) => Some(graph),
            RecordElement::OffGraph(_) => None,
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            RecordElement::OnGraph(GraphObject::VOrE(vertex_or_edge)) => vertex_or_edge.as_borrow_object(),
            // TODO(bingqing): confirm if path can be as_borrow_object?
            RecordElement::OnGraph(GraphObject::P(_)) => BorrowObject::None,
            RecordElement::OffGraph(obj_element) => match obj_element {
                ObjectElement::None => BorrowObject::None,
                ObjectElement::Prop(obj) | ObjectElement::Agg(obj) => obj.as_borrow(),
                ObjectElement::Count(cnt) => (*cnt).into(),
            },
        }
    }
}

/// RecordKey is the key fields of a Record, with each key corresponding to a request column_tag
#[derive(Clone, Debug)]
pub struct RecordKey {
    key_fields: Vec<Arc<Entry>>,
}

impl RecordKey {
    pub fn new(key_fields: Vec<Arc<Entry>>) -> Self {
        RecordKey { key_fields }
    }
    pub fn take(self) -> Vec<Arc<Entry>> {
        self.key_fields
    }
}

impl Hash for RecordElement {
    fn hash<H: Hasher>(&self, mut state: &mut H) {
        match self {
            RecordElement::OnGraph(graph_obj) => graph_obj.hash(&mut state),
            RecordElement::OffGraph(o) => match o {
                ObjectElement::None => None::<Object>.hash(&mut state),
                ObjectElement::Prop(o) => o.hash(&mut state),
                ObjectElement::Count(o) => o.hash(&mut state),
                ObjectElement::Agg(o) => o.hash(&mut state),
            },
        }
    }
}

impl Hash for Entry {
    fn hash<H: Hasher>(&self, mut state: &mut H) {
        match self {
            Entry::Element(e) => e.hash(&mut state),
            Entry::Collection(c) => c.hash(&mut state),
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
            (RecordElement::OnGraph(v1), RecordElement::OnGraph(v2)) => v1.eq(v2),
            (RecordElement::OffGraph(o1), RecordElement::OffGraph(o2)) => o1.eq(o2),
            _ => false,
        }
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Entry::Element(e1), Entry::Element(e2)) => e1 == e2,
            (Entry::Collection(c1), Entry::Collection(c2)) => c1 == c2,
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

impl PartialOrd for RecordElement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (RecordElement::OnGraph(v1), RecordElement::OnGraph(v2)) => v1.partial_cmp(v2),
            (RecordElement::OffGraph(o1), RecordElement::OffGraph(o2)) => o1.partial_cmp(o2),
            _ => None,
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Entry::Element(e1), Entry::Element(e2)) => e1.partial_cmp(e2),
            (Entry::Collection(c1), Entry::Collection(c2)) => c1.partial_cmp(c2),
            _ => None,
        }
    }
}

impl Eq for Entry {}

pub struct RecordExpandIter<E> {
    tag: Option<NameOrId>,
    origin: Record,
    children: DynIter<E>,
}

impl<E> RecordExpandIter<E> {
    pub fn new(origin: Record, tag: Option<&NameOrId>, children: DynIter<E>) -> Self {
        RecordExpandIter { tag: tag.map(|e| e.clone()), origin, children }
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
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Encode for RecordElement {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            RecordElement::OnGraph(graph_obj) => {
                writer.write_u8(0)?;
                graph_obj.write_to(writer)?;
            }
            RecordElement::OffGraph(object_element) => {
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
                let graph_obj = <GraphObject>::read_from(reader)?;
                Ok(RecordElement::OnGraph(graph_obj))
            }
            1 => {
                let object_element = <ObjectElement>::read_from(reader)?;
                Ok(RecordElement::OffGraph(object_element))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
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
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
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
        let curr = if opt == 0 { None } else { Some(Arc::new(<Entry>::read_from(reader)?)) };
        let size = <u64>::read_from(reader)? as usize;
        let mut columns = IndexMap::with_capacity(size);
        for _i in 0..size {
            let k = <NameOrId>::read_from(reader)?;
            let v = <Entry>::read_from(reader)?;
            columns.insert(k, Arc::new(v));
        }
        Ok(Record { curr, columns })
    }
}

impl Encode for RecordKey {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.key_fields.len() as u32)?;
        for key in self.key_fields.iter() {
            (&**key).write_to(writer)?
        }
        Ok(())
    }
}

impl Decode for RecordKey {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u32()?;
        let mut key_fields = Vec::with_capacity(len as usize);
        for _i in 0..len {
            let entry = <Entry>::read_from(reader)?;
            key_fields.push(Arc::new(entry))
        }
        Ok(RecordKey { key_fields })
    }
}

impl TryFrom<result_pb::Entry> for Entry {
    type Error = ParsePbError;

    fn try_from(entry_pb: result_pb::Entry) -> Result<Self, Self::Error> {
        if let Some(inner) = entry_pb.inner {
            match inner {
                result_pb::entry::Inner::Element(e) => Ok(Entry::Element(e.try_into()?)),
                result_pb::entry::Inner::Collection(c) => Ok(Entry::Collection(
                    c.collection
                        .into_iter()
                        .map(|e| e.try_into())
                        .collect::<Result<Vec<_>, Self::Error>>()?,
                )),
            }
        } else {
            Err(ParsePbError::EmptyFieldError("entry inner is empty".to_string()))?
        }
    }
}

impl TryFrom<result_pb::Element> for RecordElement {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Element) -> Result<Self, Self::Error> {
        if let Some(inner) = e.inner {
            match inner {
                result_pb::element::Inner::Vertex(v) => {
                    Ok(RecordElement::OnGraph(GraphObject::VOrE(v.try_into()?)))
                }
                result_pb::element::Inner::Edge(e) => {
                    Ok(RecordElement::OnGraph(GraphObject::VOrE(e.try_into()?)))
                }
                // TODO(bingqing): may need add a path type in result pb
                result_pb::element::Inner::Object(o) => {
                    Ok(RecordElement::OffGraph(ObjectElement::Prop(o.try_into()?)))
                }
            }
        } else {
            Err(ParsePbError::EmptyFieldError("element inner is empty".to_string()))?
        }
    }
}
