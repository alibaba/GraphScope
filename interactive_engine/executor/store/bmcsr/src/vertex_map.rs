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

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use fnv::FnvHashMap;

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
        for _ in 0..num_labels {
            labeled_num.push(0_usize);
            index_to_global_id.push(Vec::new());
            labeled_corner_num.push(0_usize);
            index_to_corner_global_id.push(Vec::new());
        }
        Self {
            global_id_to_index: FnvHashMap::default(),
            labeled_num,
            index_to_global_id,
            labeled_corner_num,
            index_to_corner_global_id,
            label_num: num_labels as LabelId,
        }
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

    pub fn add_corner_vertex(&mut self, global_id: G, label: LabelId) -> I {
        assert_eq!(label, LDBCVertexParser::get_label_id(global_id));

        if let Some(vertex) = self.global_id_to_index.get(&global_id) {
            vertex.clone()
        } else {
            let v = I::new(<I as IndexType>::max().index() - self.labeled_corner_num[label as usize] - 1);
            self.labeled_corner_num[label as usize] += 1;
            self.index_to_corner_global_id[label as usize].push(global_id);
            self.global_id_to_index.insert(global_id, v);
            v
        }
    }

    pub fn get_internal_id(&self, global_id: G) -> Option<(LabelId, I)> {
        if let Some(internal_id) = self.global_id_to_index.get(&global_id) {
            Some((LDBCVertexParser::get_label_id(global_id), *internal_id))
        } else {
            None
        }
    }

    pub fn get_global_id(&self, label: LabelId, internal_id: I) -> Option<G> {
        let internal_id = internal_id.index();
        if internal_id < self.labeled_num[label as usize] {
            self.index_to_global_id[label as usize]
                .get(internal_id)
                .cloned()
        } else {
            self.index_to_corner_global_id[label as usize]
                .get(<I as IndexType>::max().index() - internal_id - 1)
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

    pub fn serialize(&self, path: &String) {
        let f = File::create(path).unwrap();
        let mut writer = BufWriter::new(f);
        writer.write_u8(self.label_num).unwrap();

        for n in self.labeled_num.iter() {
            writer
                .write_u64::<LittleEndian>(*n as u64)
                .unwrap();
        }
        for n in self.labeled_corner_num.iter() {
            writer
                .write_u64::<LittleEndian>(*n as u64)
                .unwrap();
        }

        for i in 0..self.label_num {
            assert_eq!(self.index_to_global_id[i as usize].len(), self.labeled_num[i as usize]);
            assert_eq!(
                self.index_to_corner_global_id[i as usize].len(),
                self.labeled_corner_num[i as usize]
            );

            for v in self.index_to_global_id[i as usize].iter() {
                v.write(&mut writer).unwrap();
            }
            for v in self.index_to_corner_global_id[i as usize].iter() {
                v.write(&mut writer).unwrap();
            }
        }

        writer.flush().unwrap();
    }

    pub fn deserialize(&mut self, path: &String) {
        let f = File::open(path).unwrap();
        let mut reader = BufReader::new(f);
        self.label_num = reader.read_u8().unwrap();

        self.labeled_num.clear();
        self.labeled_corner_num.clear();
        for _ in 0..self.label_num {
            self.labeled_num
                .push(reader.read_u64::<LittleEndian>().unwrap() as usize);
        }
        for _ in 0..self.label_num {
            self.labeled_corner_num
                .push(reader.read_u64::<LittleEndian>().unwrap() as usize);
        }

        self.index_to_global_id.clear();
        self.index_to_corner_global_id.clear();
        for i in 0..self.label_num {
            let iv_num = self.labeled_num[i as usize];
            let mut native_ids = Vec::<G>::with_capacity(iv_num);
            for _ in 0..iv_num {
                native_ids.push(G::read(&mut reader).unwrap());
            }

            let ov_num = self.labeled_corner_num[i as usize];
            let mut corner_ids = Vec::<G>::with_capacity(ov_num);
            for _ in 0..ov_num {
                corner_ids.push(G::read(&mut reader).unwrap());
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

        return true;
    }
}
