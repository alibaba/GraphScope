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

use core::slice;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use fnv::FnvHashMap;
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::GDBResult;
use crate::graph::IndexType;
use crate::ldbc_parser::LDBCVertexParser;
use crate::types::*;

pub struct VertexMap<G: Send + Sync + IndexType, I: Send + Sync + IndexType> {
    global_id_to_index: FnvHashMap<G, I>,
    labeled_num: Vec<usize>,
    pub index_to_global_id: Vec<Vec<G>>,
    labeled_corner_num: Vec<usize>,
    pub index_to_corner_global_id: Vec<Vec<G>>,
    label_num: LabelId,
    internal_id_mask: Vec<I>,
}

impl<G, I> VertexMap<G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn new(num_labels: usize) -> Self {
        let mut labeled_num = Vec::with_capacity(num_labels);
        let mut index_to_global_id = Vec::with_capacity(num_labels);
        let mut labeled_corner_num = Vec::with_capacity(num_labels);
        let mut index_to_corner_global_id = Vec::with_capacity(num_labels);
        let mut internal_id_mask = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            labeled_num.push(0_usize);
            index_to_global_id.push(Vec::new());
            labeled_corner_num.push(0_usize);
            index_to_corner_global_id.push(Vec::new());
            internal_id_mask.push(<I as IndexType>::max());
        }
        Self {
            global_id_to_index: FnvHashMap::default(),
            labeled_num,
            index_to_global_id,
            labeled_corner_num,
            index_to_corner_global_id,
            label_num: num_labels as LabelId,
            internal_id_mask,
        }
    }

    pub fn set_internal_id_mask(&mut self, label_id: usize, mask: I) {
        self.internal_id_mask[label_id] = mask;
    }

    pub fn add_vertex(&mut self, global_id: G, label: LabelId) -> I {
        assert_eq!(label, LDBCVertexParser::get_label_id(global_id));

        if let Some(vertex) = self.global_id_to_index.get(&global_id) {
            assert!(vertex.index() < self.labeled_num[label as usize]);
            vertex.clone()
        } else {
            let v = I::new(self.labeled_num[label as usize]);
            self.labeled_num[label as usize] += 1;
            self.index_to_global_id[label as usize].push(global_id);
            self.global_id_to_index.insert(global_id, v);
            v
        }
    }

    pub fn add_native_vertex_beta(&mut self, global_id: G, label: LabelId) {
        let v = I::new(self.labeled_num[label as usize]);
        self.labeled_num[label as usize] += 1;
        self.index_to_global_id[label as usize].push(global_id);
        self.global_id_to_index.insert(global_id, v);
    }

    pub fn add_corner_vertex(&mut self, global_id: G, label: LabelId) -> I {
        assert_eq!(label, LDBCVertexParser::get_label_id(global_id));

        if let Some(vertex) = self.global_id_to_index.get(&global_id) {
            vertex.clone()
        } else {
            let v = I::new(
                self.internal_id_mask[label as usize].index() - self.labeled_corner_num[label as usize] - 1,
            );
            self.labeled_corner_num[label as usize] += 1;
            self.index_to_corner_global_id[label as usize].push(global_id);
            self.global_id_to_index.insert(global_id, v);
            v
        }
    }

    pub fn add_corner_vertex_beta(&mut self, global_id: G, label: LabelId) {
        assert_eq!(label, LDBCVertexParser::get_label_id(global_id));

        self.labeled_corner_num[label as usize] += 1;
        self.index_to_corner_global_id[label as usize].push(global_id);
    }

    pub fn get_internal_id(&self, global_id: G) -> Option<(LabelId, I)> {
        if let Some(internal_id) = self.global_id_to_index.get(&global_id) {
            Some((LDBCVertexParser::get_label_id(global_id), *internal_id))
        } else {
            None
        }
    }

    pub fn get_masked_internal_id(&self, label: LabelId, internal_id: I) -> I {
        internal_id.bw_and(self.internal_id_mask[label as usize])
    }

    pub fn get_global_id(&self, label: LabelId, internal_id: I) -> Option<G> {
        let masked_internal_id = internal_id
            .bw_and(self.internal_id_mask[label as usize])
            .index();
        if masked_internal_id < self.labeled_num[label as usize] {
            self.index_to_global_id[label as usize]
                .get(masked_internal_id)
                .cloned()
        } else {
            self.index_to_corner_global_id[label as usize]
                .get(self.internal_id_mask[label as usize].index() - masked_internal_id - 1)
                .cloned()
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.global_id_to_index.shrink_to_fit();
        for list in &mut self.index_to_global_id {
            list.shrink_to_fit();
        }
        for list in &mut self.index_to_corner_global_id {
            list.shrink_to_fit();
        }
    }

    pub fn label_num(&self) -> LabelId {
        self.label_num
    }

    pub fn vertex_num(&self, label: LabelId) -> usize {
        self.labeled_num[label as usize]
    }

    pub fn corner_vertex_num(&self, label: LabelId) -> usize {
        self.labeled_corner_num[label as usize]
    }

    pub fn total_vertex_num(&self) -> usize {
        self.labeled_num.iter().sum()
    }

    pub fn export_native<P: AsRef<Path>>(&self, path: P) -> GDBResult<()> {
        let mut writer = File::create(path)?;
        for v in self.internal_id_mask.iter() {
            writer.write_u64(v.index() as u64)?;
        }
        for v in self.labeled_num.iter() {
            writer.write_u64(*v as u64)?;
        }
        for vec in self.index_to_global_id.iter() {
            for v in vec.iter() {
                writer.write_u64(v.index() as u64)?;
            }
        }
        writer.flush()?;
        Ok(())
    }

    pub fn export_corner<P: AsRef<Path>>(&self, path: P) -> GDBResult<()> {
        let mut writer = File::create(path)?;
        for v in self.labeled_corner_num.iter() {
            writer.write_u64(*v as u64)?;
        }
        for vec in self.index_to_corner_global_id.iter() {
            for v in vec.iter() {
                writer.write_u64(v.index() as u64)?;
            }
        }
        writer.flush()?;
        Ok(())
    }

    pub fn import_native<P: AsRef<Path>>(&mut self, path: P) -> GDBResult<()> {
        let mut reader = File::open(path)?;
        let mut labeled_num = vec![];
        self.internal_id_mask.clear();
        for _ in 0..self.label_num {
            self.internal_id_mask
                .push(I::new(reader.read_u64()? as usize));
        }
        for _ in 0..self.label_num {
            labeled_num.push(reader.read_u64()? as usize);
        }
        for i in 0..self.label_num {
            for _ in 0..labeled_num[i as usize] {
                // self.add_vertex(G::new(reader.read_u64()? as usize), i);
                self.add_native_vertex_beta(G::new(reader.read_u64().unwrap() as usize), i);
            }
        }

        Ok(())
    }

    pub fn import_corner<P: AsRef<Path>>(&mut self, path: P) -> GDBResult<()> {
        let mut reader = File::open(path)?;
        let mut labeled_num = vec![];
        for _ in 0..self.label_num {
            labeled_num.push(reader.read_u64()? as usize);
        }
        for i in 0..self.label_num {
            for _ in 0..labeled_num[i as usize] {
                self.add_corner_vertex_beta(G::new(reader.read_u64()? as usize), i);
            }
        }
        Ok(())
    }

    pub fn serialize(&self, path: &String) {
        let mut f = File::create(path).unwrap();
        f.write_u8(self.label_num).unwrap();

        for n in self.labeled_num.iter() {
            f.write_u64(*n as u64).unwrap();
        }
        for n in self.labeled_corner_num.iter() {
            f.write_u64(*n as u64).unwrap();
        }
        for n in self.internal_id_mask.iter() {
            f.write_u64(n.index() as u64).unwrap();
        }

        for i in 0..self.label_num {
            assert_eq!(self.index_to_global_id[i as usize].len(), self.labeled_num[i as usize]);
            assert_eq!(
                self.index_to_corner_global_id[i as usize].len(),
                self.labeled_corner_num[i as usize]
            );

            unsafe {
                let native_ids_slice = slice::from_raw_parts(
                    self.index_to_global_id[i as usize].as_ptr() as *const u8,
                    self.index_to_global_id[i as usize].len() * std::mem::size_of::<G>(),
                );
                f.write_all(native_ids_slice).unwrap();
                let corner_ids_slice = slice::from_raw_parts(
                    self.index_to_corner_global_id[i as usize].as_ptr() as *const u8,
                    self.index_to_corner_global_id[i as usize].len() * std::mem::size_of::<G>(),
                );
                f.write_all(corner_ids_slice).unwrap();
            }
        }

        f.flush().unwrap();
    }

    pub fn deserialize(&mut self, path: &String) {
        let mut f = File::open(path).unwrap();
        self.label_num = f.read_u8().unwrap();

        self.labeled_num.clear();
        self.labeled_corner_num.clear();
        self.internal_id_mask.clear();
        for _ in 0..self.label_num {
            self.labeled_num
                .push(f.read_u64().unwrap() as usize);
        }
        for _ in 0..self.label_num {
            self.labeled_corner_num
                .push(f.read_u64().unwrap() as usize);
        }
        for _ in 0..self.label_num {
            self.internal_id_mask
                .push(I::new(f.read_u64().unwrap() as usize));
        }

        self.index_to_global_id.clear();
        self.index_to_corner_global_id.clear();
        for i in 0..self.label_num {
            let mut native_ids = Vec::<G>::new();
            native_ids.resize(self.labeled_num[i as usize], G::new(0_usize));
            let mut corner_ids = Vec::<G>::new();
            corner_ids.resize(self.labeled_corner_num[i as usize], G::new(0_usize));

            unsafe {
                let native_ids_slice = slice::from_raw_parts_mut(
                    native_ids.as_mut_ptr() as *mut u8,
                    self.labeled_num[i as usize] * std::mem::size_of::<G>(),
                );
                f.read_exact(native_ids_slice).unwrap();

                let corner_ids_slice = slice::from_raw_parts_mut(
                    corner_ids.as_mut_ptr() as *mut u8,
                    self.labeled_corner_num[i as usize] * std::mem::size_of::<G>(),
                );
                f.read_exact(corner_ids_slice).unwrap();
            }

            self.index_to_global_id.push(native_ids);
            self.index_to_corner_global_id.push(corner_ids);
        }

        self.global_id_to_index.clear();
        for i in 0..self.label_num {
            let mut index = 0_usize;
            for v in self.index_to_global_id[i as usize].iter() {
                self.global_id_to_index
                    .insert(*v, I::new(index));
                index += 1;
            }
        }
    }

    pub fn desc(&self) {
        info!("label_num = {}, entry num = {}", self.label_num, self.global_id_to_index.len());
        for i in 0..self.label_num {
            info!(
                "label-{}: native: {}, corner: {}",
                i,
                self.index_to_global_id[i as usize].len(),
                self.index_to_corner_global_id[i as usize].len()
            )
        }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.global_id_to_index != other.global_id_to_index {
            return false;
        }
        if self.label_num != other.label_num {
            return false;
        }
        if self.index_to_global_id != other.index_to_global_id {
            return false;
        }
        if self.labeled_corner_num != other.labeled_corner_num {
            return false;
        }
        if self.index_to_corner_global_id != other.index_to_corner_global_id {
            return false;
        }
        if self.label_num != other.label_num {
            return false;
        }
        if self.internal_id_mask != other.internal_id_mask {
            return false;
        }

        return true;
    }
}

impl<G: Send + Sync + IndexType, I: Send + Sync + IndexType> Encode for VertexMap<G, I> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.write_u64(self.label_num as u64)?;
        assert_eq!(self.labeled_num.len(), self.label_num as usize);
        for v in self.labeled_num.iter() {
            writer.write_u64(*v as u64)?;
        }
        assert_eq!(self.labeled_corner_num.len(), self.label_num as usize);
        for v in self.labeled_corner_num.iter() {
            writer.write_u64(*v as u64)?;
        }
        for v in self.internal_id_mask.iter() {
            writer.write_u64(v.index() as u64)?;
        }
        for (label, vec) in self.index_to_global_id.iter().enumerate() {
            assert_eq!(vec.len(), self.labeled_num[label]);
            for v in vec.iter() {
                writer.write_u64(v.index() as u64)?;
            }
        }
        for (label, vec) in self
            .index_to_corner_global_id
            .iter()
            .enumerate()
        {
            assert_eq!(vec.len(), self.labeled_corner_num[label]);
            for v in vec.iter() {
                writer.write_u64(v.index() as u64)?;
            }
        }

        Ok(())
    }
}

impl<G: Send + Sync + IndexType, I: Send + Sync + IndexType> Decode for VertexMap<G, I> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let label_num = reader.read_u64()? as usize;
        let mut ret = Self::new(label_num);
        let mut labeled_num = vec![];
        let mut labeled_corner_num = vec![];
        for _ in 0..label_num {
            labeled_num.push(reader.read_u64()? as usize);
        }
        for _ in 0..label_num {
            labeled_corner_num.push(reader.read_u64()? as usize);
        }
        ret.internal_id_mask.clear();
        for _ in 0..label_num {
            ret.internal_id_mask
                .push(I::new(reader.read_u64()? as usize));
        }

        for i in 0..label_num {
            for _ in 0..labeled_num[i] {
                ret.add_vertex(G::new(reader.read_u64()? as usize), i as LabelId);
            }
        }
        for i in 0..label_num {
            for _ in 0..labeled_corner_num[i] {
                ret.add_corner_vertex_beta(G::new(reader.read_u64()? as usize), i as LabelId);
            }
        }

        Ok(ret)
    }
}

impl<G: Send + Sync + IndexType, I: Send + Sync + IndexType> Serialize for VertexMap<G, I> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut bytes = Vec::new();
        if self.write_to(&mut bytes).is_ok() {
            bytes.serialize(serializer)
        } else {
            Result::Err(S::Error::custom("Serialize vertex map failed!"))
        }
    }
}

impl<'de, G, I> Deserialize<'de> for VertexMap<G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<u8>::deserialize(deserializer)?;
        let mut bytes = vec.as_slice();
        VertexMap::<G, I>::read_from(&mut bytes)
            .map_err(|_| D::Error::custom("Deserialize vertex map failed!"))
    }
}
