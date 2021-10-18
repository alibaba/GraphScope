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
use crate::graph::element::{Edge, Vertex, VertexOrEdge};
use dyn_type::Object;
use ir_common::error::DynIter;
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

pub trait Columns {
    fn get(tag: &NameOrId) -> Option<&Entry>;
}

#[derive(Debug, Clone, Default)]
pub struct Record {
    curr: Option<Entry>,
    // TODO: optimized as VecMap<Entry>
    columns: HashMap<NameOrId, Entry>,
    // The tags that refer to keys, while the values (of keys) are saved in columns
    // TODO: this field may not be necessary
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
            // TODO(bingqing): confirm
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

impl PartialEq for Record {
    fn eq(&self, _other: &Self) -> bool {
        todo!()
    }
}

impl Eq for Record {}

impl Hash for Record {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        // TODO: hash by key if exists
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

impl Context<VertexOrEdge> for Record {
    fn get(&self, _tag: &NameOrId) -> Option<&VertexOrEdge> {
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
