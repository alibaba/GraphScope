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
use std::any::Any;
use std::fs::File;
use std::io::{Read, Write};

use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::graph::IndexType;
use crate::graph_db::{CsrTrait, Nbr, NbrIter};

pub struct SingleCsrEdgeIter<'a, I: IndexType> {
    cur_vertex: usize,
    exist_list: &'a Vec<bool>,
    nbr_list: &'a Vec<Nbr<I>>,
}

impl<'a, I: IndexType> Iterator for SingleCsrEdgeIter<'a, I> {
    type Item = (I, &'a Nbr<I>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.cur_vertex == self.nbr_list.len() {
                return None;
            }
            let cur = self.cur_vertex.index();
            self.cur_vertex += 1;
            if self.exist_list[cur] {
                return Some((I::new(cur), &self.nbr_list[cur]));
            }
        }
    }
}

unsafe impl<I: IndexType> Send for SingleCsrEdgeIter<'_, I> {}

unsafe impl<I: IndexType> Sync for SingleCsrEdgeIter<'_, I> {}

pub struct SingleCsr<I> {
    nbr_list: Vec<Nbr<I>>,
    nbr_exist: Vec<bool>,
    edge_num: usize,
}

impl<I: IndexType> SingleCsr<I> {
    pub fn new() -> Self {
        SingleCsr { nbr_list: vec![], nbr_exist: vec![], edge_num: 0_usize }
    }

    pub fn edge_num(&self) -> usize {
        self.edge_num
    }

    pub fn resize_vertices(&mut self, vnum: I) {
        if vnum == self.vertex_num() {
            return;
        }
        self.nbr_list
            .resize(vnum.index(), Nbr { neighbor: I::new(0_usize), offset: 0 });
        self.nbr_exist.resize(vnum.index(), false);
    }

    pub fn put_edge(&mut self, src: I, dst: I, offset: usize) {
        if src.index() < self.nbr_list.len() {
            assert!(!self.nbr_exist[src.index()]);
            self.nbr_list[src.index()] = Nbr { neighbor: dst, offset };
            self.nbr_exist[src.index()] = true;
            self.edge_num += 1;
        }
    }

    pub fn desc(&self) {
        let num = self.nbr_list.len();
        let mut empty_num = 0;
        for i in 0..num {
            if !self.nbr_exist[i] {
                empty_num += 1;
            }
        }

        info!("vertex-num = {}, edge_num = {}, empty_nbr_num = {}", num, self.edge_num, empty_num);
    }

    pub fn deserialize(&mut self, path: &String) {
        let mut f = File::open(path).unwrap();
        let vnum = f.read_u64().unwrap() as usize;
        self.edge_num = f.read_u64().unwrap() as usize;

        self.nbr_list
            .resize(vnum, Nbr { neighbor: I::new(0_usize), offset: 0 });
        self.nbr_exist.resize(vnum, false);

        let nbr_list_size = vnum * std::mem::size_of::<Nbr<I>>();
        let nbr_exist_size = vnum * std::mem::size_of::<bool>();
        unsafe {
            let nbr_list_slice =
                slice::from_raw_parts_mut(self.nbr_list.as_mut_ptr() as *mut u8, nbr_list_size);
            f.read_exact(nbr_list_slice).unwrap();

            let nbr_exist_slice =
                slice::from_raw_parts_mut(self.nbr_exist.as_mut_ptr() as *mut u8, nbr_exist_size);
            f.read_exact(nbr_exist_slice).unwrap();
        }
    }

    pub fn is_same(&self, other: &Self) -> bool {
        if self.nbr_list.len() != other.nbr_list.len() {
            return false;
        }
        if self.nbr_exist.len() != other.nbr_exist.len() {
            return false;
        }
        if self.edge_num != other.edge_num {
            return false;
        }

        let vnum = self.nbr_list.len();
        for i in 0..vnum {
            if self.nbr_list[i].neighbor.index() != other.nbr_list[i].neighbor.index() {
                return false;
            }
            if self.nbr_exist[i] != other.nbr_exist[i] {
                return false;
            }
        }

        return true;
    }
}

impl<I: IndexType> CsrTrait<I> for SingleCsr<I> {
    fn vertex_num(&self) -> I {
        I::new(self.nbr_list.len())
    }

    fn edge_num(&self) -> usize {
        self.edge_num
    }

    fn degree(&self, src: I) -> i64 {
        if self.nbr_exist[src.index()] {
            1
        } else {
            0
        }
    }

    fn get_edges(&self, src: I) -> Option<NbrIter<'_, I>> {
        if src.index() < self.nbr_list.len() {
            if !self.nbr_exist[src.index()] {
                Some(NbrIter::new_empty())
            } else {
                Some(NbrIter::new_single(unsafe { self.nbr_list.as_ptr().add(src.index()) }))
            }
        } else {
            None
        }
    }

    fn get_all_edges<'a>(&'a self) -> Box<dyn Iterator<Item = (I, &'a Nbr<I>)> + 'a + Send> {
        Box::new(SingleCsrEdgeIter { cur_vertex: 0, exist_list: &self.nbr_exist, nbr_list: &self.nbr_list })
    }

    fn serialize(&self, path: &String) {
        let mut f = File::create(path).unwrap();
        let vnum = self.nbr_list.len();
        f.write_u64(vnum as u64).unwrap();
        f.write_u64(self.edge_num as u64).unwrap();

        let nbr_list_size = vnum * std::mem::size_of::<Nbr<I>>();
        let nbr_exist_size = vnum * std::mem::size_of::<bool>();

        unsafe {
            let nbr_list_slice = slice::from_raw_parts(self.nbr_list.as_ptr() as *const u8, nbr_list_size);
            f.write_all(nbr_list_slice).unwrap();

            let nbr_exist_slice =
                slice::from_raw_parts(self.nbr_exist.as_ptr() as *const u8, nbr_exist_size);
            f.write_all(nbr_exist_slice).unwrap();
        }

        f.flush().unwrap();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

unsafe impl<I: IndexType> Send for SingleCsr<I> {}

unsafe impl<I: IndexType> Sync for SingleCsr<I> {}

impl<I: IndexType> Encode for SingleCsr<I> {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        let vnum = self.nbr_list.len();
        writer.write_u64(vnum as u64)?;
        for i in 0..vnum {
            if self.nbr_exist[i] {
                writer.write_i64(1_i64)?;
            } else {
                writer.write_i64(0_i64)?;
            }
        }

        for i in 0..vnum {
            if self.nbr_exist[i] {
                self.nbr_list[i].neighbor.write_to(writer)?;
                self.nbr_list[i].offset.write_to(writer)?;
            }
        }

        info!("write 1 u64, {} i64, {} nbr", vnum, self.edge_num);

        Ok(())
    }
}

impl<I: IndexType> Decode for SingleCsr<I> {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let mut ret = Self::new();
        let vnum = reader.read_u64()? as usize;
        ret.resize_vertices(I::new(vnum));
        let mut degree_vec = Vec::with_capacity(vnum);
        for _ in 0..vnum {
            degree_vec.push(reader.read_i64()?);
        }
        for i in 0..vnum {
            if degree_vec[i] == 1_i64 {
                let neighbor = I::read_from(reader)?;
                let offset = usize::read_from(reader)?;
                ret.put_edge(I::new(i), neighbor, offset);
            }
        }

        Ok(ret)
    }
}
