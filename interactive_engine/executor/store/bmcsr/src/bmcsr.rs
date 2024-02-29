use std::any::Any;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};

use crate::csr::{CsrBuildError, CsrTrait, NbrIter, NbrOffsetIter};
use crate::graph::IndexType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub struct BatchMutableCsr<I> {
    pub neighbors: Vec<I>,
    pub offsets: Vec<usize>,
    pub degree: Vec<i32>,

    edge_num: usize,
}

pub struct BatchMutableCsrBuilder<I> {
    neighbors: Vec<I>,
    offsets: Vec<usize>,
    insert_offsets: Vec<i32>,

    edge_num: usize,
}

impl<I: IndexType> BatchMutableCsrBuilder<I> {
    pub fn new() -> Self {
        BatchMutableCsrBuilder {
            neighbors: Vec::new(),
            offsets: Vec::new(),
            insert_offsets: Vec::new(),
            edge_num: 0,
        }
    }

    pub fn init(&mut self, degree: &Vec<i64>, _: f64) {
        let vertex_num = degree.len();
        let mut edge_num = 0_usize;
        for i in 0..vertex_num {
            edge_num += degree[i] as usize;
        }
        self.edge_num = 0;

        self.neighbors.resize(edge_num, I::new(0));
        self.offsets.resize(vertex_num, 0);
        self.insert_offsets.resize(vertex_num, 0);

        let mut offset = 0_usize;
        for i in 0..vertex_num {
            self.insert_offsets[i] = 0;
            self.offsets[i] = offset;
            offset += degree[i] as usize;
        }
    }

    pub fn put_edge(&mut self, src: I, dst: I) -> Result<usize, CsrBuildError> {
        let offset = self.offsets[src.index()] + self.insert_offsets[src.index()] as usize;
        self.neighbors[offset] = dst;
        self.insert_offsets[src.index()] += 1;
        self.edge_num += 1;
        Ok(offset)
    }

    pub fn finish(self) -> Result<BatchMutableCsr<I>, CsrBuildError> {
        Ok(BatchMutableCsr {
            neighbors: self.neighbors,
            offsets: self.offsets,
            degree: self.insert_offsets,
            edge_num: self.edge_num,
        })
    }
}

impl<I: IndexType> BatchMutableCsr<I> {
    pub fn new() -> Self {
        BatchMutableCsr { neighbors: Vec::new(), offsets: Vec::new(), degree: Vec::new(), edge_num: 0 }
    }
}

unsafe impl<I: IndexType> Send for BatchMutableCsr<I> {}
unsafe impl<I: IndexType> Sync for BatchMutableCsr<I> {}

impl<I: IndexType> CsrTrait<I> for BatchMutableCsr<I> {
    fn vertex_num(&self) -> I {
        I::new(self.offsets.len())
    }

    fn edge_num(&self) -> usize {
        self.edge_num
    }

    fn degree(&self, u: I) -> usize {
        let u = u.index();
        if u >= self.degree.len() {
            0
        } else {
            self.degree[u] as usize
        }
    }

    fn serialize(&self, path: &String) {
        let file = File::create(path).unwrap();
        let mut writer = BufWriter::new(file);
        writer
            .write_u64::<LittleEndian>(self.edge_num as u64)
            .unwrap();

        writer
            .write_u64::<LittleEndian>(self.neighbors.len() as u64)
            .unwrap();
        for i in 0..self.neighbors.len() {
            self.neighbors[i].write(&mut writer).unwrap();
        }
        writer
            .write_u64::<LittleEndian>(self.offsets.len() as u64)
            .unwrap();
        for i in 0..self.offsets.len() {
            writer
                .write_u64::<LittleEndian>(self.offsets[i] as u64)
                .unwrap();
        }
        writer
            .write_u64::<LittleEndian>(self.degree.len() as u64)
            .unwrap();
        for i in 0..self.degree.len() {
            writer
                .write_i32::<LittleEndian>(self.degree[i])
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn deserialize(&mut self, path: &String) {
        let file = File::open(path).unwrap();
        let mut reader = BufReader::new(file);

        self.edge_num = reader.read_u64::<LittleEndian>().unwrap() as usize;

        let neighbor_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.neighbors = Vec::with_capacity(neighbor_size);
        for _ in 0..neighbor_size {
            self.neighbors
                .push(I::read(&mut reader).unwrap());
        }

        let offset_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.offsets = Vec::with_capacity(offset_size);
        for _ in 0..offset_size {
            self.offsets
                .push(reader.read_u64::<LittleEndian>().unwrap() as usize);
        }

        let degree_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        self.degree = Vec::with_capacity(degree_size);
        for _ in 0..degree_size {
            self.degree
                .push(reader.read_i32::<LittleEndian>().unwrap());
        }
    }

    fn get_edges(&self, u: I) -> Option<NbrIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start = self.offsets[u];
            let end = self.offsets[u] + self.degree[u] as usize;
            Some(NbrIter::new(&self.neighbors, start, end))
        }
    }

    fn get_edges_with_offset(&self, u: I) -> Option<NbrOffsetIter<I>> {
        let u = u.index();
        if u >= self.offsets.len() {
            None
        } else {
            let start = self.offsets[u];
            let end = self.offsets[u] + self.degree[u] as usize;
            Some(NbrOffsetIter::new(&self.neighbors, start, end))
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
            let vertex = vertex.index();
            if vertex >= self.degree.len() {
                continue;
            }
            self.edge_num -= self.degree[vertex] as usize;
            self.degree[vertex] = 0;
        }
    }

    fn delete_edges(&mut self, edges: &HashSet<(I, I)>, reverse: bool) {
        if reverse {
            let mut src_set = HashSet::new();
            for (_, src) in edges {
                let src = src.index();
                if src >= self.vertex_num().index() {
                    continue;
                }
                src_set.insert(src);
            }
            for src in src_set {
                let mut offset = self.offsets[src];
                let mut end = self.offsets[src] + self.degree[src] as usize;
                while offset < (end - 1) {
                    if edges.contains(&(self.neighbors[offset], I::new(src))) {
                        self.neighbors[offset] = self.neighbors[end - 1];
                        end -= 1;
                    } else {
                        offset += 1;
                    }
                }
                if edges.contains(&(self.neighbors[end - 1], I::new(src))) {
                    end -= 1;
                }
                let new_degree = end - self.offsets[src];
                let degree_diff = self.degree[src] as usize - new_degree;
                self.degree[src] = new_degree as i32;
                self.edge_num -= degree_diff;
            }
        } else {
            let mut src_set = HashSet::new();
            for (src, _) in edges {
                let src = src.index();
                if src >= self.vertex_num().index() {
                    continue;
                }
                src_set.insert(src);
            }
            for src in src_set {
                let mut offset = self.offsets[src];
                let mut end = self.offsets[src] + self.degree[src] as usize;
                while offset < (end - 1) {
                    if edges.contains(&(I::new(src), self.neighbors[offset])) {
                        self.neighbors[offset] = self.neighbors[end - 1];
                        end -= 1;
                    } else {
                        offset += 1;
                    }
                }
                if edges.contains(&(I::new(src), self.neighbors[end - 1])) {
                    end -= 1;
                }
                let new_degree = end - self.offsets[src];
                let degree_diff = self.degree[src] as usize - new_degree;
                self.degree[src] = new_degree as i32;
                self.edge_num -= degree_diff;
            }
        }
    }
}
