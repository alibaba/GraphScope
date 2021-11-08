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

use crate::expr::eval::Context;
use crate::graph::element::{Edge, Element, Vertex, VertexOrEdge};
use crate::graph::property::DynDetails;
use dyn_type::{BorrowObject, Object};
use ir_common::error::DynIter;
use ir_common::NameOrId;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// The specific tag for head
pub const HEAD_TAG: &str = "HEAD";

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
    // The tags that refer to keys, while the values (of keys) are saved in columns
    keys: Vec<NameOrId>,
}

impl Record {
    pub fn new<E: Into<Entry>>(entry: E, tag: Option<NameOrId>) -> Self {
        if let Some(tag) = tag {
            let mut columns = HashMap::new();
            columns.insert(tag, entry.into());
            Record {
                curr: None,
                columns,
                keys: vec![],
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

    pub fn set_keys(&mut self, keys: Vec<NameOrId>) {
        self.keys = keys;
    }

    pub fn insert_key(&mut self, tag: NameOrId) {
        self.keys.push(tag)
    }

    pub fn join(mut self, mut other: Record) -> Record {
        // TODO: check if duplicated tag exists? and what about head alias if head is needed?
        for column in other.columns.drain() {
            self.columns.insert(column.0, column.1);
        }
        self
    }
}

impl Encode for Entry {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl Decode for Entry {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}

impl PartialEq for Entry {
    fn eq(&self, _other: &Self) -> bool {
        todo!()
    }
}

impl Eq for Entry {}

impl Hash for Entry {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        todo!()
    }
}

impl Encode for Record {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl Decode for Record {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
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
    fn get(&self, tag: &NameOrId) -> Option<&RecordElement> {
        let entry = if tag.eq(&NameOrId::Str(HEAD_TAG.to_string())) {
            self.get(None)
        } else {
            self.get(Some(tag))
        };
        entry
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

impl Hash for RecordKey {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        todo!()
    }
}

impl PartialEq for RecordKey {
    fn eq(&self, _other: &Self) -> bool {
        todo!()
    }
}

impl Eq for RecordKey {}

impl Encode for RecordKey {
    fn write_to<W: WriteExt>(&self, _writer: &mut W) -> std::io::Result<()> {
        todo!()
    }
}

impl Decode for RecordKey {
    fn read_from<R: ReadExt>(_reader: &mut R) -> std::io::Result<Self> {
        todo!()
    }
}

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
