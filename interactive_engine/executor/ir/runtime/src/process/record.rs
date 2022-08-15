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

use std::borrow::BorrowMut;
use std::convert::{TryFrom, TryInto};
use std::hash::Hash;
use std::ops::Add;
use std::sync::Arc;

use dyn_type::{BorrowObject, Object};
use graph_proxy::apis::{DynDetails, Edge, Element, GraphElement, GraphPath, Vertex, VertexOrEdge};
use graph_proxy::utils::expr::eval::Context;
use ir_common::error::ParsePbError;
use ir_common::generated::results as result_pb;
use ir_common::{KeyId, NameOrId};
use pegasus::api::function::DynIter;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use vec_map::VecMap;

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub enum CommonObject {
    /// a None value used when:
    /// 1) project a non-exist tag of the record;
    /// 2) project a non-exist label/property of the record of graph_element;
    /// 3) the property value returned from store is Object::None (TODO: may need to distinguish this case)
    None,
    /// projected property
    Prop(Object),
    /// count
    Count(u64),
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub enum Entry {
    V(Vertex),
    E(Edge),
    P(GraphPath),
    OffGraph(CommonObject),
    Collection(Vec<Entry>),
}

impl Entry {
    pub fn as_graph_vertex(&self) -> Option<&Vertex> {
        match self {
            Entry::V(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_graph_edge(&self) -> Option<&Edge> {
        match self {
            Entry::E(e) => Some(e),
            _ => None,
        }
    }

    pub fn as_graph_path(&self) -> Option<&GraphPath> {
        match self {
            Entry::P(p) => Some(p),
            _ => None,
        }
    }

    pub fn as_common_object(&self) -> Option<&CommonObject> {
        match self {
            Entry::OffGraph(common_object) => Some(common_object),
            _ => None,
        }
    }

    pub fn as_mut_graph_path(&mut self) -> Option<&mut GraphPath> {
        match self {
            Entry::P(graph_path) => Some(graph_path),
            _ => None,
        }
    }

    pub fn is_none(&self) -> bool {
        match self {
            Entry::OffGraph(CommonObject::None) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Record {
    curr: Option<Arc<Entry>>,
    columns: VecMap<Arc<Entry>>,
}

impl Record {
    pub fn new<E: Into<Entry>>(entry: E, tag: Option<KeyId>) -> Self {
        let entry = Arc::new(entry.into());
        let mut columns = VecMap::new();
        if let Some(tag) = tag {
            columns.insert(tag as usize, entry.clone());
        }
        Record { curr: Some(entry), columns }
    }

    /// A handy api to append entry of different types that can be turned into `Entry`
    pub fn append<E: Into<Entry>>(&mut self, entry: E, alias: Option<KeyId>) {
        self.append_arc_entry(Arc::new(entry.into()), alias)
    }

    pub fn append_arc_entry(&mut self, entry: Arc<Entry>, alias: Option<KeyId>) {
        self.curr = Some(entry.clone());
        if let Some(alias) = alias {
            self.columns.insert(alias as usize, entry);
        }
    }

    /// Set new current entry for the record
    pub fn set_curr_entry(&mut self, entry: Option<Arc<Entry>>) {
        self.curr = entry;
    }

    pub fn get_columns_mut(&mut self) -> &mut VecMap<Arc<Entry>> {
        self.columns.borrow_mut()
    }

    pub fn get(&self, tag: Option<KeyId>) -> Option<&Arc<Entry>> {
        if let Some(tag) = tag {
            self.columns.get(tag as usize)
        } else {
            self.curr.as_ref()
        }
    }

    pub fn take(&mut self, tag: Option<&KeyId>) -> Option<Arc<Entry>> {
        if let Some(tag) = tag {
            self.columns.remove(*tag as usize)
        } else {
            self.curr.take()
        }
    }

    /// To join this record with `other` record. After the join, the columns
    /// from both sides will be merged (and deduplicated). The `curr` entry of the joined
    /// record will be specified according to `is_left_opt`, namely, if
    /// * `is_left_opt = None` -> set as `None`,
    /// * `is_left_opt = Some(true)` -> set as left record,
    /// * `is_left_opt = Some(false)` -> set as right record.
    pub fn join(mut self, mut other: Record, is_left_opt: Option<bool>) -> Record {
        for column in other.columns.drain() {
            if !self.columns.contains_key(column.0) {
                self.columns.insert(column.0, column.1);
            }
        }

        if let Some(is_left) = is_left_opt {
            if !is_left {
                self.curr = other.curr;
            }
        } else {
            self.curr = None;
        }

        self
    }
}

impl Into<Entry> for Vertex {
    fn into(self) -> Entry {
        Entry::V(self)
    }
}

impl Into<Entry> for Edge {
    fn into(self) -> Entry {
        Entry::E(self)
    }
}

impl Into<Entry> for GraphPath {
    fn into(self) -> Entry {
        Entry::P(self)
    }
}

impl Into<Entry> for VertexOrEdge {
    fn into(self) -> Entry {
        match self {
            VertexOrEdge::V(v) => v.into(),
            VertexOrEdge::E(e) => e.into(),
        }
    }
}

impl Into<Entry> for CommonObject {
    fn into(self) -> Entry {
        Entry::OffGraph(self)
    }
}

impl Context<Entry> for Record {
    fn get(&self, tag: Option<&NameOrId>) -> Option<&Entry> {
        let tag = if let Some(tag) = tag {
            match tag {
                // TODO: may better throw an unsupported error if tag is a string_tag
                NameOrId::Str(_) => None,
                NameOrId::Id(id) => Some(*id),
            }
        } else {
            None
        };
        self.get(tag)
            .map(|entry| match entry.as_ref() {
                Entry::Collection(_) => None,
                _ => Some(entry.as_ref()),
            })
            .unwrap_or(None)
    }
}

impl Element for Entry {
    fn details(&self) -> Option<&DynDetails> {
        match self {
            Entry::V(v) => v.details(),
            Entry::E(e) => e.details(),
            Entry::P(p) => p.details(),
            Entry::OffGraph(_) => None,
            Entry::Collection(_) => None,
        }
    }

    fn as_graph_element(&self) -> Option<&dyn GraphElement> {
        match self {
            Entry::V(v) => v.as_graph_element(),
            Entry::E(e) => e.as_graph_element(),
            Entry::P(p) => p.as_graph_element(),
            Entry::OffGraph(_) => None,
            Entry::Collection(_) => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            Entry::V(v) => v.len(),
            Entry::E(e) => e.len(),
            Entry::P(p) => p.len(),
            Entry::OffGraph(obj) => match obj {
                CommonObject::None => 0,
                CommonObject::Prop(obj) => obj.len(),
                CommonObject::Count(_) => 1,
            },
            Entry::Collection(c) => c.len(),
        }
    }

    fn as_borrow_object(&self) -> BorrowObject {
        match self {
            Entry::V(v) => v.as_borrow_object(),
            Entry::E(e) => e.as_borrow_object(),
            Entry::P(p) => p.as_borrow_object(),
            Entry::OffGraph(obj_element) => match obj_element {
                CommonObject::None => BorrowObject::None,
                CommonObject::Prop(obj) => obj.as_borrow(),
                CommonObject::Count(cnt) => (*cnt).into(),
            },
            Entry::Collection(_) => unreachable!(),
        }
    }
}

/// RecordKey is the key fields of a Record, with each key corresponding to a request column_tag
#[derive(Clone, Debug, Hash, PartialEq)]
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

impl Eq for RecordKey {}
impl Eq for Entry {}

pub struct RecordExpandIter<E> {
    tag: Option<KeyId>,
    origin: Record,
    children: DynIter<E>,
}

impl<E> RecordExpandIter<E> {
    pub fn new(origin: Record, tag: Option<&KeyId>, children: DynIter<E>) -> Self {
        RecordExpandIter { tag: tag.map(|e| e.clone()), origin, children }
    }
}

impl<E: Into<Entry>> Iterator for RecordExpandIter<E> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.origin.clone();
        match self.children.next() {
            Some(elem) => {
                record.append(elem, self.tag.clone());
                Some(record)
            }
            None => None,
        }
    }
}

pub struct RecordPathExpandIter<E> {
    origin: Record,
    curr_path: GraphPath,
    children: DynIter<E>,
}

impl<E> RecordPathExpandIter<E> {
    pub fn new(origin: Record, curr_path: GraphPath, children: DynIter<E>) -> Self {
        RecordPathExpandIter { origin, curr_path, children }
    }
}

impl<E: Into<Entry>> Iterator for RecordPathExpandIter<E> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.origin.clone();
        let mut curr_path = self.curr_path.clone();
        match self.children.next() {
            Some(elem) => {
                let graph_obj = elem.into();
                match graph_obj {
                    Entry::V(v) => {
                        curr_path.append(v);
                        record.append(curr_path, None);
                        Some(record)
                    }
                    Entry::E(e) => {
                        curr_path.append(e);
                        record.append(curr_path, None);
                        Some(record)
                    }
                    _ => None,
                }
            }
            None => None,
        }
    }
}

impl Encode for CommonObject {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            CommonObject::None => {
                writer.write_u8(0)?;
            }
            CommonObject::Prop(prop) => {
                writer.write_u8(1)?;
                prop.write_to(writer)?;
            }
            CommonObject::Count(cnt) => {
                writer.write_u8(2)?;
                writer.write_u64(*cnt)?;
            }
        }
        Ok(())
    }
}

impl Decode for CommonObject {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let opt = reader.read_u8()?;
        match opt {
            0 => Ok(CommonObject::None),
            1 => {
                let object = <Object>::read_from(reader)?;
                Ok(CommonObject::Prop(object))
            }
            2 => {
                let cnt = <u64>::read_from(reader)?;
                Ok(CommonObject::Count(cnt))
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "unreachable")),
        }
    }
}

impl Encode for Entry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        match self {
            Entry::V(v) => {
                writer.write_u8(0)?;
                v.write_to(writer)?;
            }
            Entry::E(e) => {
                writer.write_u8(1)?;
                e.write_to(writer)?;
            }
            Entry::P(p) => {
                writer.write_u8(2)?;
                p.write_to(writer)?;
            }
            Entry::OffGraph(object_element) => {
                writer.write_u8(3)?;
                object_element.write_to(writer)?;
            }
            Entry::Collection(collection) => {
                writer.write_u8(4)?;
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
                let v = <Vertex>::read_from(reader)?;
                Ok(Entry::V(v))
            }
            1 => {
                let e = <Edge>::read_from(reader)?;
                Ok(Entry::E(e))
            }
            2 => {
                let p = <GraphPath>::read_from(reader)?;
                Ok(Entry::P(p))
            }
            3 => {
                let object_element = <CommonObject>::read_from(reader)?;
                Ok(Entry::OffGraph(object_element))
            }
            4 => {
                let collection = <Vec<Entry>>::read_from(reader)?;
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
            (k as KeyId).write_to(writer)?;
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
        let mut columns = VecMap::with_capacity(size);
        for _i in 0..size {
            let k = <KeyId>::read_from(reader)? as usize;
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
                result_pb::entry::Inner::Element(e) => Ok(e.try_into()?),
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

impl TryFrom<result_pb::Element> for Entry {
    type Error = ParsePbError;
    fn try_from(e: result_pb::Element) -> Result<Self, Self::Error> {
        if let Some(inner) = e.inner {
            match inner {
                result_pb::element::Inner::Vertex(v) => Ok(Entry::V(v.try_into()?)),
                result_pb::element::Inner::Edge(e) => Ok(Entry::E(e.try_into()?)),
                result_pb::element::Inner::GraphPath(p) => Ok(Entry::P(p.try_into()?)),
                result_pb::element::Inner::Object(o) => {
                    Ok(Entry::OffGraph(CommonObject::Prop(o.try_into()?)))
                }
            }
        } else {
            Err(ParsePbError::EmptyFieldError("element inner is empty".to_string()))?
        }
    }
}

impl Add for CommonObject {
    type Output = CommonObject;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (CommonObject::Prop(o1), CommonObject::Prop(o2)) => match (o1, o2) {
                (Object::Primitive(p1), Object::Primitive(p2)) => {
                    CommonObject::Prop(Object::Primitive(p1.add(p2)))
                }
                (o1, Object::None) => CommonObject::Prop(o1),
                (Object::None, o2) => CommonObject::Prop(o2),
                _ => CommonObject::Prop(Object::None),
            },
            (CommonObject::Count(c1), CommonObject::Count(c2)) => CommonObject::Count(c1 + c2),
            (o1, CommonObject::None) => o1,
            (CommonObject::None, o2) => o2,
            _ => CommonObject::None,
        }
    }
}

impl Add for Entry {
    type Output = Entry;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Entry::OffGraph(o1), Entry::OffGraph(o2)) => o1.add(o2).into(),
            _ => CommonObject::None.into(),
        }
    }
}
