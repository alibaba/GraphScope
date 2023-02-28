use core::slice;
use std::fs::File;
use std::io::{Read, Write};

use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use serde::de::Error as DeError;
use serde::ser::Error as SerError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::col_table::ColTable;
use crate::columns::Item;
use crate::graph::IndexType;
use crate::mcsr::{Nbr, NbrIter};

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

pub struct SingleCsr<I> {
    nbr_list: Vec<Nbr<I>>,
    nbr_exist: Vec<bool>,
    edge_property: Option<ColTable>,
    edge_num: usize,
}

impl<I: IndexType> SingleCsr<I> {
    pub fn new() -> Self {
        SingleCsr { nbr_list: vec![], nbr_exist: vec![], edge_property: None, edge_num: 0_usize }
    }

    pub fn vertex_num(&self) -> I {
        I::new(self.nbr_list.len())
    }

    pub fn edge_num(&self) -> usize {
        self.edge_num
    }

    pub fn resize_vertices(&mut self, vnum: I) {
        if vnum == self.vertex_num() {
            return;
        }
        self.nbr_list
            .resize(vnum.index(), Nbr { neighbor: I::new(0_usize) });
        self.nbr_exist.resize(vnum.index(), false);
    }

    pub fn put_edge(&mut self, src: I, dst: I) {
        if src.index() < self.nbr_list.len() {
            assert!(!self.nbr_exist[src.index()]);
            self.nbr_list[src.index()] = Nbr { neighbor: dst };
            self.nbr_exist[src.index()] = true;
            self.edge_num += 1;
        }
    }

    pub fn put_edge_properties(&mut self, src: I, dst: I, properties: &Vec<Item>) {
        if src.index() < self.nbr_list.len() {
            assert!(!self.nbr_exist[src.index()]);
            self.nbr_list[src.index()] = Nbr { neighbor: dst };
            self.nbr_exist[src.index()] = true;
            match self.edge_property.as_mut() {
                Some(edge_property) => edge_property.insert(src.index(), properties),
                None => {}
            }
            self.edge_num += 1;
        }
    }

    pub fn get_edges(&self, src: I) -> Option<NbrIter<'_, I>> {
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

    pub fn get_edge(&self, src: I) -> Option<Nbr<I>> {
        let index = src.index();
        if index < self.nbr_list.len() {
            if self.nbr_exist[index] {
                Some(self.nbr_list[index].clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn get_all_edges(&self) -> SingleCsrEdgeIter<'_, I> {
        SingleCsrEdgeIter { cur_vertex: 0, exist_list: &self.nbr_exist, nbr_list: &self.nbr_list }
    }

    pub fn desc(&self) {
        let num = self.nbr_list.len();
        let mut empty_num = 0;
        for i in 0..num {
            if !self.nbr_exist[i] {
                empty_num += 1;
            }
        }

        println!("vertex-num = {}, edge_num = {}, empty_nbr_num = {}", num, self.edge_num, empty_num);
    }

    pub fn serialize(&self, path: &String) {
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

    pub fn deserialize(&mut self, path: &String) {
        let mut f = File::open(path).unwrap();
        let vnum = f.read_u64().unwrap() as usize;
        self.edge_num = f.read_u64().unwrap() as usize;

        self.nbr_list
            .resize(vnum, Nbr { neighbor: I::new(0_usize) });
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

    pub fn put_col_table(&mut self, col_table: ColTable) {
        self.edge_property = Some(col_table);
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
            }
        }

        match self.edge_property.as_ref() {
            Some(edge_property) => {
                writer.write_i64(1_i64)?;
                edge_property.write_to(writer)?;
            }
            None => {
                writer.write_i64(0_i64)?;
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
                ret.put_edge(I::new(i), neighbor);
            }
        }

        if reader.read_i64().unwrap() == 1 {
            ret.put_col_table(ColTable::read_from(reader).unwrap());
        }

        Ok(ret)
    }
}
impl<I: IndexType> Serialize for SingleCsr<I> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut bytes = Vec::new();
        if self.write_to(&mut bytes).is_ok() {
            info!("writing {} bytes...", bytes.len());
            bytes.serialize(serializer)
        } else {
            Result::Err(S::Error::custom("Serialize mutable csr failed!"))
        }
    }
}

impl<'de, I> Deserialize<'de> for SingleCsr<I>
where
    I: IndexType,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let vec = Vec::<u8>::deserialize(deserializer)?;
        let mut bytes = vec.as_slice();
        SingleCsr::<I>::read_from(&mut bytes)
            .map_err(|_| D::Error::custom("Deserialize mutable csr failed!"))
    }
}
