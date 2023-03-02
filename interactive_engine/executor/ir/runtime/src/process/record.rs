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
use std::hash::Hash;

use graph_proxy::apis::{GraphPath, Vertex};
use graph_proxy::utils::expr::eval::Context;
use ir_common::{KeyId, NameOrId};
use pegasus::api::function::DynIter;
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use vec_map::VecMap;

use crate::process::entry::{DynEntry, Entry, EntryType};

#[derive(Debug, Clone, Default)]
pub struct Record {
    curr: Option<DynEntry>,
    columns: VecMap<DynEntry>,
}

unsafe impl Send for Record {}
unsafe impl Sync for Record {}

impl Record {
    pub fn new<E: Entry + 'static>(entry: E, tag: Option<KeyId>) -> Self {
        let mut columns = VecMap::new();
        let entry = DynEntry::new(entry);
        if let Some(tag) = tag {
            columns.insert(tag as usize, entry.clone());
        }
        Record { curr: Some(entry), columns }
    }

    /// A handy api to append entry of different types that can be turned into `Entry`
    pub fn append<E: Entry + 'static>(&mut self, entry: E, alias: Option<KeyId>) {
        let entry = DynEntry::new(entry);
        self.append_arc_entry(entry, alias)
    }

    pub fn append_arc_entry(&mut self, entry: DynEntry, alias: Option<KeyId>) {
        if let Some(alias) = alias {
            self.columns
                .insert(alias as usize, entry.clone());
        }
        self.curr = Some(entry);
    }

    /// Set new current entry for the record
    pub fn set_curr_entry(&mut self, entry: Option<DynEntry>) {
        self.curr = entry;
    }

    pub fn get_column_mut(&mut self, tag: &KeyId) -> Option<&mut dyn Entry> {
        self.columns
            .get_mut(*tag as usize)
            .map(|e| e.get_mut())
            .unwrap_or(None)
    }

    pub fn get_columns_mut(&mut self) -> &mut VecMap<DynEntry> {
        self.columns.borrow_mut()
    }

    pub fn get(&self, tag: Option<KeyId>) -> Option<&DynEntry> {
        if let Some(tag) = tag {
            self.columns.get(tag as usize)
        } else {
            self.curr.as_ref()
        }
    }

    pub fn take(&mut self, tag: Option<&KeyId>) -> Option<DynEntry> {
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

impl Context<DynEntry> for Record {
    fn get(&self, tag: Option<&NameOrId>) -> Option<&DynEntry> {
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
            .map(|entry| {
                let entry_type = entry.get_type();
                match entry_type {
                    EntryType::Collection => None,
                    EntryType::Intersection => None,
                    _ => Some(entry),
                }
            })
            .unwrap_or(None)
    }
}

/// RecordKey is the key fields of a Record, with each key corresponding to a request column_tag
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct RecordKey {
    key_fields: Vec<DynEntry>,
}

impl RecordKey {
    pub fn new(key_fields: Vec<DynEntry>) -> Self {
        RecordKey { key_fields }
    }
    pub fn take(self) -> Vec<DynEntry> {
        self.key_fields
    }
}

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

impl<E: Entry + 'static> Iterator for RecordExpandIter<E> {
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

impl<E: Entry + 'static> Iterator for RecordPathExpandIter<E> {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.origin.clone();
        let mut curr_path = self.curr_path.clone();
        loop {
            match self.children.next() {
                Some(mut elem) => {
                    // currently, we only support GraphPath containing vertices.
                    if let Some(vertex) = elem.as_any_mut().downcast_mut::<Vertex>() {
                        let v = std::mem::replace(vertex, Default::default());
                        if curr_path.append(v) {
                            record.append(curr_path, None);
                            return Some(record);
                        }
                    }
                }
                None => return None,
            }
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
        let curr = if opt == 0 { None } else { Some(<DynEntry>::read_from(reader)?) };
        let size = <u64>::read_from(reader)? as usize;
        let mut columns = VecMap::with_capacity(size);
        for _i in 0..size {
            let k = <KeyId>::read_from(reader)? as usize;
            let v = <DynEntry>::read_from(reader)?;
            columns.insert(k, v);
        }
        Ok(Record { curr, columns })
    }
}

impl Encode for RecordKey {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u32(self.key_fields.len() as u32)?;
        for key in self.key_fields.iter() {
            key.write_to(writer)?
        }
        Ok(())
    }
}

impl Decode for RecordKey {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let len = reader.read_u32()?;
        let mut key_fields = Vec::with_capacity(len as usize);
        for _i in 0..len {
            let entry = <DynEntry>::read_from(reader)?;
            key_fields.push(entry)
        }
        Ok(RecordKey { key_fields })
    }
}
