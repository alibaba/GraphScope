use std::any::Any;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use crate::col_table::ColTable;
use crate::csr::{CsrBuildError, CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct BatchMutableSingleCsr<I> {
    nbr_list: Vec<I>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
}

pub struct BatchMutableSingleCsrBuilder<I> {
    nbr_list: Vec<I>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
}

impl<I: IndexType> BatchMutableSingleCsrBuilder<I> {
    pub fn new() -> Self {
        BatchMutableSingleCsrBuilder {
            nbr_list: Vec::new(),
            vertex_num: 0,
            edge_num: 0,
            vertex_capacity: 0,
        }
    }

    pub fn init(&mut self, degree: &Vec<i64>, reserve_rate: f64) {
        let vertex_num = degree.len();
        let mut edge_num = 0_usize;
        for i in 0..vertex_num {
            edge_num += degree[i] as usize;
        }

        self.vertex_num = vertex_num;
        self.edge_num = edge_num;

        self.vertex_capacity = vertex_num * reserve_rate as usize;

        self.nbr_list
            .resize(self.vertex_capacity, <I as IndexType>::max());
    }

    pub fn put_edge(&mut self, src: I, dst: I) -> Result<usize, CsrBuildError> {
        self.nbr_list[src.index()] = dst;
        Ok(src.index())
    }

    pub fn finish(self) -> Result<BatchMutableSingleCsr<I>, CsrBuildError> {
        Ok(BatchMutableSingleCsr {
            nbr_list: self.nbr_list,
            vertex_num: self.vertex_num,
            edge_num: self.edge_num,
            vertex_capacity: self.vertex_capacity,
        })
    }
}

impl<I: IndexType> BatchMutableSingleCsr<I> {
    pub fn new() -> Self {
        BatchMutableSingleCsr { nbr_list: Vec::new(), vertex_num: 0, edge_num: 0, vertex_capacity: 0 }
    }

    pub fn resize_vertex(&mut self, vertex_num: usize) {
        if vertex_num < self.vertex_num {
            self.vertex_num = vertex_num;
        } else if vertex_num == self.vertex_num {
            return;
        } else if vertex_num < self.vertex_capacity {
            for i in self.vertex_num..vertex_num {
                self.nbr_list[i] = <I as IndexType>::max();
            }
            self.vertex_num = vertex_num;
        } else {
            // warn!("resize vertex capacity from {} to {}", self.vertex_capacity, vertex_num);
            self.nbr_list
                .resize(vertex_num, <I as IndexType>::max());
            self.vertex_num = vertex_num;
            self.vertex_capacity = vertex_num;
        }
    }

    pub fn put_edge(&mut self, src: I, dst: I) {
        self.nbr_list[src.index()] = dst;
    }

    pub fn remove_vertex(&mut self, vertex: I) {
        self.nbr_list[vertex.index()] = <I as IndexType>::max();
    }

    pub fn remove_edge(&mut self, src: I, dst: I) {
        if self.nbr_list[src.index()] == dst {
            self.nbr_list[src.index()] = <I as IndexType>::max();
        }
    }

    pub fn get_edge(&self, src: I) -> Option<I> {
        if self.nbr_list[src.index()] == <I as IndexType>::max() {
            None
        } else {
            Some(self.nbr_list[src.index()])
        }
    }

    pub fn get_edge_with_offset(&self, src: I) -> Option<(I, usize)> {
        if self.nbr_list[src.index()] == <I as IndexType>::max() {
            None
        } else {
            Some((self.nbr_list[src.index()], src.index()))
        }
    }

    pub fn insert_edge(&mut self, src: I, dst: I) {
        self.nbr_list[src.index()] = dst;
    }
}

unsafe impl<I: IndexType> Send for BatchMutableSingleCsr<I> {}
unsafe impl<I: IndexType> Sync for BatchMutableSingleCsr<I> {}

impl<I: IndexType> CsrTrait<I> for BatchMutableSingleCsr<I> {
    fn vertex_num(&self) -> I {
        I::new(self.vertex_num)
    }

    fn edge_num(&self) -> usize {
        self.edge_num
    }

    fn max_edge_offset(&self) -> usize {
        self.vertex_num
    }

    fn degree(&self, u: I) -> usize {
        (self.nbr_list[u.index()] == <I as IndexType>::max()) as usize
    }

    fn serialize(&self, path: &String) {
        let file = File::create(path).unwrap();
        let mut writer = BufWriter::new(file);
        writer
            .write_u64::<LittleEndian>(self.vertex_num as u64)
            .unwrap();
        writer
            .write_u64::<LittleEndian>(self.edge_num as u64)
            .unwrap();
        writer
            .write_u64::<LittleEndian>(self.vertex_capacity as u64)
            .unwrap();
        writer
            .write_u64::<LittleEndian>(self.nbr_list.len() as u64)
            .unwrap();
        for i in 0..self.nbr_list.len() {
            self.nbr_list[i].write(&mut writer).unwrap();
        }
        writer.flush().unwrap();
    }

    fn deserialize(&mut self, path: &String) {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);

        self.vertex_num = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.edge_num = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.vertex_capacity = reader.read_u64::<LittleEndian>().unwrap() as usize;
        let len = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.nbr_list = Vec::with_capacity(len);
        for _ in 0..len {
            self.nbr_list
                .push(I::read(&mut reader).unwrap());
        }
    }

    fn get_edges(&self, src: I) -> Option<NbrIter<I>> {
        if self.nbr_list[src.index()] == <I as IndexType>::max() {
            None
        } else {
            Some(NbrIter::new(&self.nbr_list, src.index(), src.index() + 1))
        }
    }

    fn get_edges_with_offset(&self, src: I) -> Option<NbrOffsetIter<I>> {
        if self.nbr_list[src.index()] == <I as IndexType>::max() {
            None
        } else {
            Some(NbrOffsetIter::new(&self.nbr_list, src.index(), src.index() + 1))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn delete_vertices(&mut self, vertices: &HashSet<I>) {
        for vertex in vertices {
            self.remove_vertex(*vertex);
        }
    }

    fn delete_edges(&mut self, edges: &HashSet<(I, I)>, reverse: bool) {
        if reverse {
            for (dst, src) in edges {
                self.remove_edge(*src, *dst);
            }
        } else {
            for (src, dst) in edges {
                self.remove_edge(*src, *dst);
            }
        }
    }

    fn delete_edges_with_props(&mut self, edges: &HashSet<(I, I)>, reverse: bool, _: &mut ColTable) {
        self.delete_edges(edges, reverse);
    }
}
