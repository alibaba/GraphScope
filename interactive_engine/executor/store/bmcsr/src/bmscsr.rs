use std::any::Any;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use crate::col_table::ColTable;
use crate::csr::{CsrBuildError, CsrTrait, NbrIter, NbrIterBeta, NbrOffsetIter, SafeMutPtr, SafePtr};
use crate::graph::IndexType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use huge_container::HugeVec;

type ArrayType<T> = HugeVec<T>;

pub struct BatchMutableSingleCsr<I> {
    nbr_list: ArrayType<I>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
}

pub struct BatchMutableSingleCsrBuilder<I> {
    nbr_list: ArrayType<I>,

    vertex_num: usize,
    edge_num: usize,

    vertex_capacity: usize,
}

impl<I: IndexType> BatchMutableSingleCsrBuilder<I> {
    pub fn new() -> Self {
        BatchMutableSingleCsrBuilder {
            nbr_list: ArrayType::new(),
            vertex_num: 0,
            edge_num: 0,
            vertex_capacity: 0,
        }
    }

    pub fn init(&mut self, degree: &Vec<i32>, reserve_rate: f64) {
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
        BatchMutableSingleCsr { nbr_list: ArrayType::new(), vertex_num: 0, edge_num: 0, vertex_capacity: 0 }
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
        self.nbr_list = ArrayType::with_capacity(len);
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

    fn get_edges_beta(&self, u: I) -> NbrIterBeta<I> {
        if self.nbr_list[u.index()] == <I as IndexType>::max() {
            NbrIterBeta::new(self.nbr_list.as_ptr(), self.nbr_list.as_ptr())
        } else {
            NbrIterBeta::new(unsafe { self.nbr_list.as_ptr().add(u.index()) }, unsafe {
                self.nbr_list.as_ptr().add(u.index() + 1)
            })
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

    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, p: u32) {
        let edges_num = edges.len();

        let safe_nbr_list_ptr = SafeMutPtr::new(&mut self.nbr_list);
        let safe_edges_ptr = SafePtr::new(edges);

        let num_threads = p as usize;
        let chunk_size = (edges_num + num_threads - 1) / num_threads;
        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = edges_num.min(start_idx + chunk_size);
                s.spawn(move |_| {
                    let edges_ref = safe_edges_ptr.get_ref();
                    let nbr_list_ref = safe_nbr_list_ptr.get_mut();
                    if reverse {
                        for k in start_idx..end_idx {
                            let v = edges_ref[k].1;
                            nbr_list_ref[v.index()] = <I as IndexType>::max();
                        }
                    } else {
                        for k in start_idx..end_idx {
                            let v = edges_ref[k].0;
                            nbr_list_ref[v.index()] = <I as IndexType>::max();
                        }
                    }
                });
            }
        });
    }

    fn parallel_delete_edges_with_props(
        &mut self, edges: &Vec<(I, I)>, reverse: bool, _: &mut ColTable, p: u32,
    ) {
        self.parallel_delete_edges(edges, reverse, p);
    }

    fn insert_edges(&mut self, vertex_num: usize, edges: &Vec<(I, I)>, reverse: bool, p: u32) {
        self.resize_vertex(vertex_num);

        let num_threads = p as usize;
        let chunk_size = (edges.len() + num_threads - 1) / num_threads;

        let safe_nbr_list_ptr = SafeMutPtr::new(&mut self.nbr_list);
        let safe_edges_ptr = SafePtr::new(edges);

        let edge_num = edges.len();

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = (start_idx + chunk_size).min(edge_num);
                s.spawn(move |_| {
                    let nbr_list_ref = safe_nbr_list_ptr.get_mut();
                    let edges_ref = safe_edges_ptr.get_ref();
                    if reverse {
                        for idx in start_idx..end_idx {
                            let (dst, src) = edges_ref.get(idx).unwrap();
                            nbr_list_ref[src.index()] = *dst;
                        }
                    } else {
                        for idx in start_idx..end_idx {
                            let (src, dst) = edges_ref.get(idx).unwrap();
                            nbr_list_ref[src.index()] = *dst;
                        }
                    }
                });
            }
        });
    }

    fn insert_edges_with_prop(
        &mut self, vertex_num: usize, edges: &Vec<(I, I)>, edges_prop: &ColTable, reverse: bool, p: u32,
        mut table: ColTable,
    ) -> ColTable {
        self.resize_vertex(vertex_num);
        table.resize(vertex_num);

        let num_threads = p as usize;
        let chunk_size = (edges.len() + num_threads - 1) / num_threads;

        let safe_nbr_list_ptr = SafeMutPtr::new(&mut self.nbr_list);
        let safe_edges_ptr = SafePtr::new(edges);
        let safe_table_ptr = SafeMutPtr::new(&mut table);

        let edge_num = edges.len();

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = (start_idx + chunk_size).min(edge_num);
                s.spawn(move |_| {
                    let nbr_list_ref = safe_nbr_list_ptr.get_mut();
                    let edges_ref = safe_edges_ptr.get_ref();
                    let table_ref = safe_table_ptr.get_mut();

                    if reverse {
                        for idx in start_idx..end_idx {
                            let (dst, src) = edges_ref.get(idx).unwrap();
                            nbr_list_ref[src.index()] = *dst;
                            table_ref.set_table_row(src.index(), edges_prop, idx);
                        }
                    } else {
                        for idx in start_idx..end_idx {
                            let (src, dst) = edges_ref.get(idx).unwrap();
                            nbr_list_ref[src.index()] = *dst;
                            table_ref.set_table_row(src.index(), edges_prop, idx);
                        }
                    }
                });
            }
        });

        table
    }
}
