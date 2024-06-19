use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::col_table::ColTable;
use crate::csr::{CsrBuildError, CsrTrait, NbrIter, NbrIterBeta, NbrOffsetIter, SafeMutPtr, SafePtr};
use crate::graph::IndexType;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

#[cfg(feature = "hugepage_csr")]
use huge_container::HugeVec;

#[cfg(feature = "hugepage_csr")]
type ArrayType<T> = HugeVec<T>;

#[cfg(not(feature = "hugepage_csr"))]
type ArrayType<T> = Vec<T>;

pub struct BatchMutableCsr<I> {
    pub neighbors: ArrayType<I>,
    pub offsets: ArrayType<usize>,
    pub degree: ArrayType<i32>,

    edge_num: usize,
}

pub struct BatchMutableCsrBuilder<I> {
    neighbors: ArrayType<I>,
    offsets: ArrayType<usize>,
    insert_offsets: ArrayType<i32>,

    edge_num: usize,
}

impl<I: IndexType> BatchMutableCsrBuilder<I> {
    pub fn new() -> Self {
        BatchMutableCsrBuilder {
            neighbors: ArrayType::new(),
            offsets: ArrayType::new(),
            insert_offsets: ArrayType::new(),
            edge_num: 0,
        }
    }

    pub fn init(&mut self, degree: &Vec<i32>, _: f64) {
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
        BatchMutableCsr {
            neighbors: ArrayType::new(),
            offsets: ArrayType::new(),
            degree: ArrayType::new(),
            edge_num: 0,
        }
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

    fn max_edge_offset(&self) -> usize {
        self.neighbors.len()
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
        info!("edge_num = {}", self.edge_num);
        writer
            .write_u64::<LittleEndian>(self.edge_num as u64)
            .unwrap();

        info!("neighbor_size = {}", self.neighbors.len());
        writer
            .write_u64::<LittleEndian>(self.neighbors.len() as u64)
            .unwrap();
        for i in 0..self.neighbors.len() {
            self.neighbors[i].write(&mut writer).unwrap();
        }
        info!("offset_size = {}", self.offsets.len());
        writer
            .write_u64::<LittleEndian>(self.offsets.len() as u64)
            .unwrap();
        for i in 0..self.offsets.len() {
            writer
                .write_u64::<LittleEndian>(self.offsets[i] as u64)
                .unwrap();
        }
        info!("degree_size = {}", self.degree.len());
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
        info!("edge_num = {}", self.edge_num);

        let neighbor_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("neighbor_size = {}", neighbor_size);
        self.neighbors = ArrayType::with_capacity(neighbor_size);
        for _ in 0..neighbor_size {
            self.neighbors
                .push(I::read(&mut reader).unwrap());
        }

        let offset_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("offset_size = {}", offset_size);
        self.offsets = ArrayType::with_capacity(offset_size);
        for _ in 0..offset_size {
            self.offsets
                .push(reader.read_u64::<LittleEndian>().unwrap() as usize);
        }

        let degree_size = reader.read_u64::<LittleEndian>().unwrap() as usize;
        info!("degree_size = {}", degree_size);
        let degree_capacity = degree_size + degree_size / 2;
        self.degree = ArrayType::with_capacity(degree_capacity);
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

    fn get_edges_beta(&self, u: I) -> NbrIterBeta<I> {
        let u = u.index();
        if u >= self.offsets.len() {
            NbrIterBeta::new(self.neighbors.as_ptr(), self.neighbors.as_ptr())
        } else {
            let start = self.offsets[u];
            let end = self.offsets[u] + self.degree[u] as usize;
            let start = unsafe { self.neighbors.as_ptr().add(start) };
            let end = unsafe { self.neighbors.as_ptr().add(end) };
            NbrIterBeta::new(start, end)
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

    fn parallel_delete_edges(&mut self, edges: &Vec<(I, I)>, reverse: bool, p: u32) {
        let mut delete_map: HashMap<I, HashSet<I>> = HashMap::new();
        let mut keys = vec![];
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    let mut set = HashSet::new();
                    set.insert(*src);
                    delete_map.insert(*dst, set);
                    keys.push(*dst);
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&src) {
                    set.insert(*dst);
                } else {
                    let mut set = HashSet::new();
                    set.insert(*dst);
                    delete_map.insert(*src, set);
                    keys.push(*src);
                }
            }
        }
        keys.sort();

        let safe_offsets_ptr = SafePtr::new(&self.offsets);
        let safe_degree_ptr = SafeMutPtr::new(&mut self.degree);
        let safe_neighbors_ptr = SafeMutPtr::new(&mut self.neighbors);
        let safe_keys_ptr = SafePtr::new(&keys);

        let keys_size = keys.len();
        let num_threads = p as usize;
        let chunk_size = (keys_size + num_threads - 1) / num_threads;

        let mut thread_deleted_edges = vec![0_usize; num_threads];

        let safe_delete_map_ptr = SafePtr::new(&delete_map);
        let safe_tde_ptr = SafeMutPtr::new(&mut thread_deleted_edges);

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = keys_size.min(start_idx + chunk_size);
                s.spawn(move |_| {
                    let keys_ref = safe_keys_ptr.get_ref();
                    let offsets_ref = safe_offsets_ptr.get_ref();
                    let degree_ref = safe_degree_ptr.get_mut();
                    let neighbors_ref = safe_neighbors_ptr.get_mut();
                    let tde_ref = safe_tde_ptr.get_mut();
                    let delete_map_ref = safe_delete_map_ptr.get_ref();
                    let mut deleted_edges = 0;
                    for v_index in start_idx..end_idx {
                        let v = keys_ref[v_index];
                        let mut offset = offsets_ref[v.index()];
                        let deg = degree_ref[v.index()];

                        let set = delete_map_ref.get(&v).unwrap();
                        let mut end = offset + deg as usize;
                        while offset < (end - 1) {
                            let nbr = neighbors_ref[offset];
                            if set.contains(&nbr) {
                                neighbors_ref[offset] = neighbors_ref[end - 1];
                                end -= 1;
                            } else {
                                offset += 1;
                            }
                        }
                        let nbr = neighbors_ref[end - 1];
                        if set.contains(&nbr) {
                            end -= 1;
                        }

                        let new_deg = (end - offsets_ref[v.index()]) as i32;
                        degree_ref[v.index()] = new_deg;

                        deleted_edges += (deg - new_deg) as usize;
                    }

                    tde_ref[i] = deleted_edges;
                });
            }
        });

        for v in thread_deleted_edges.iter() {
            self.edge_num -= *v;
        }
    }

    fn parallel_delete_edges_with_props(
        &mut self, edges: &Vec<(I, I)>, reverse: bool, table: &mut ColTable, p: u32,
    ) {
        let mut delete_map: HashMap<I, HashSet<I>> = HashMap::new();
        let mut keys = vec![];
        if reverse {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&dst) {
                    set.insert(*src);
                } else {
                    let mut set = HashSet::new();
                    set.insert(*src);
                    delete_map.insert(*dst, set);
                    keys.push(*dst);
                }
            }
        } else {
            for (src, dst) in edges.iter() {
                if let Some(set) = delete_map.get_mut(&src) {
                    set.insert(*dst);
                } else {
                    let mut set = HashSet::new();
                    set.insert(*dst);
                    delete_map.insert(*src, set);
                    keys.push(*src);
                }
            }
        }
        keys.sort();

        let safe_offsets_ptr = SafePtr::new(&self.offsets);
        let safe_degree_ptr = SafeMutPtr::new(&mut self.degree);
        let safe_neighbors_ptr = SafeMutPtr::new(&mut self.neighbors);
        let safe_keys_ptr = SafePtr::new(&keys);
        let safe_table_ptr = SafeMutPtr::new(table);

        let keys_size = keys.len();
        let num_threads = p as usize;
        let chunk_size = (keys_size + num_threads - 1) / num_threads;

        let mut thread_deleted_edges = vec![0_usize; num_threads];

        let safe_delete_map_ptr = SafePtr::new(&delete_map);
        let safe_tde_ptr = SafeMutPtr::new(&mut thread_deleted_edges);

        rayon::scope(|s| {
            for i in 0..num_threads {
                let start_idx = i * chunk_size;
                let end_idx = keys_size.min(start_idx + chunk_size);
                s.spawn(move |_| {
                    let keys_ref = safe_keys_ptr.get_ref();
                    let offsets_ref = safe_offsets_ptr.get_ref();
                    let degree_ref = safe_degree_ptr.get_mut();
                    let neighbors_ref = safe_neighbors_ptr.get_mut();
                    let table_ref = safe_table_ptr.get_mut();
                    let tde_ref = safe_tde_ptr.get_mut();
                    let delete_map_ref = safe_delete_map_ptr.get_ref();
                    let mut deleted_edges = 0;
                    for v_index in start_idx..end_idx {
                        let v = keys_ref[v_index];
                        let mut offset = offsets_ref[v.index()];
                        let deg = degree_ref[v.index()];

                        let set = delete_map_ref.get(&v).unwrap();
                        let mut end = offset + deg as usize;
                        while offset < (end - 1) {
                            let nbr = neighbors_ref[offset];
                            if set.contains(&nbr) {
                                neighbors_ref[offset] = neighbors_ref[end - 1];
                                table_ref.move_row(end - 1, offset);
                                end -= 1;
                            } else {
                                offset += 1;
                            }
                        }
                        let nbr = neighbors_ref[end - 1];
                        if set.contains(&nbr) {
                            end -= 1;
                        }

                        let new_deg = (end - offsets_ref[v.index()]) as i32;
                        degree_ref[v.index()] = new_deg;

                        deleted_edges += (deg - new_deg) as usize;
                    }

                    tde_ref[i] = deleted_edges;
                });
            }
        });

        for v in thread_deleted_edges.iter() {
            self.edge_num -= *v;
        }
    }

    fn insert_edges(&mut self, vertex_num: usize, edges: &Vec<(I, I)>, reverse: bool, p: u32) {
        let mut new_degree = vec![0; vertex_num];

        if reverse {
            for e in edges.iter() {
                new_degree[e.1.index()] += 1;
            }
        } else {
            for e in edges.iter() {
                new_degree[e.0.index()] += 1;
            }
        }

        let num_threads = p as usize;

        let old_vertex_num = self.offsets.len();
        // let chunk_size = ((vertex_num + num_threads - 1) / num_threads).min(32768); //
        // let chunk_size = ((vertex_num + num_threads - 1) / num_threads); // inf
        let chunk_size = (((vertex_num + num_threads - 1) / num_threads) + 3) / 4;
        let chunk_num = (vertex_num + chunk_size - 1) / chunk_size;

        let mut chunk_offset = vec![0_usize; chunk_num];
        let safe_chunk_offset_ptr = SafeMutPtr::new(&mut chunk_offset);
        let mut new_offsets = ArrayType::with_capacity(vertex_num);
        new_offsets.resize(vertex_num, 0);
        let safe_new_offsets_ptr = SafeMutPtr::new(&mut new_offsets);
        let safe_new_degree_ptr = SafeMutPtr::new(&mut new_degree);
        let safe_degree_ptr = SafePtr::new(&self.degree);

        let chunk_i = AtomicUsize::new(0);

        rayon::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|_| {
                    let chunk_i_ref = &chunk_i;
                    let new_offsets_ref = safe_new_offsets_ptr.get_mut();
                    let chunk_offset_ref = safe_chunk_offset_ptr.get_mut();
                    let degree_ref = safe_degree_ptr.get_ref();
                    let new_degree_ref = safe_new_degree_ptr.get_mut();
                    loop {
                        let cur_chunk = chunk_i_ref.fetch_add(1, Ordering::Relaxed);
                        if cur_chunk >= chunk_num {
                            break;
                        }
                        let mut local_offset = 0_usize;

                        let start_idx = cur_chunk * chunk_size;
                        let end_idx = vertex_num.min(start_idx + chunk_size);

                        if end_idx > old_vertex_num {
                            if start_idx >= old_vertex_num {
                                for v in start_idx..end_idx {
                                    new_offsets_ref[v] = local_offset;
                                    local_offset += (new_degree[v]) as usize;
                                }
                            } else {
                                for v in start_idx..old_vertex_num {
                                    new_offsets_ref[v] = local_offset;
                                    local_offset += (degree_ref[v] + new_degree_ref[v]) as usize;
                                }
                                for v in old_vertex_num..end_idx {
                                    new_offsets_ref[v] = local_offset;
                                    local_offset += (new_degree[v]) as usize;
                                }
                            }
                        } else {
                            for v in start_idx..end_idx {
                                new_offsets_ref[v] = local_offset;
                                local_offset += (degree_ref[v] + new_degree_ref[v]) as usize;
                            }
                        }
                        chunk_offset_ref[cur_chunk] = local_offset;
                    }
                });
            }
        });
        let mut cur_offset = 0_usize;
        for i in 0..chunk_num {
            let tmp = chunk_offset[i] + cur_offset;
            chunk_offset[i] = cur_offset;
            cur_offset = tmp;
        }

        let mut new_neighbors = ArrayType::with_capacity(cur_offset);
        new_neighbors.resize(cur_offset, I::new(0));

        let safe_new_neighbors_ptr = SafeMutPtr::new(&mut new_neighbors);
        let safe_neighbors_ptr = SafePtr::new(&self.neighbors);
        let safe_offsets_ptr = SafePtr::new(&self.offsets);

        let chunk_i = AtomicUsize::new(0);

        rayon::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|_| {
                    let chunk_i_ref = &chunk_i;
                    let neighbors_ref = safe_neighbors_ptr.get_ref();
                    let new_neighbors_ref = safe_new_neighbors_ptr.get_mut();
                    let new_offsets_ref = safe_new_offsets_ptr.get_mut();
                    let chunk_offset_ref = safe_chunk_offset_ptr.get_mut();
                    let offsets_ref = safe_offsets_ptr.get_ref();
                    let degree_ref = safe_degree_ptr.get_ref();

                    loop {
                        let cur_chunk = chunk_i_ref.fetch_add(1, Ordering::Relaxed);
                        if cur_chunk >= chunk_num {
                            break;
                        }

                        let local_offset = chunk_offset_ref[cur_chunk];
                        let start_idx = cur_chunk * chunk_size;
                        let end_idx = vertex_num.min(start_idx + chunk_size);

                        if end_idx > old_vertex_num {
                            if start_idx >= old_vertex_num {
                                for v in start_idx..end_idx {
                                    new_offsets_ref[v] += local_offset;
                                }
                            } else {
                                for v in start_idx..old_vertex_num {
                                    let offset = new_offsets_ref[v] + local_offset;
                                    new_offsets_ref[v] = offset;
                                    let old_offset = offsets_ref[v];
                                    let deg = degree_ref[v] as usize;
                                    new_neighbors_ref[offset..offset + deg]
                                        .copy_from_slice(&neighbors_ref[old_offset..old_offset + deg]);
                                }
                                for v in old_vertex_num..end_idx {
                                    new_offsets_ref[v] += local_offset;
                                }
                            }
                        } else {
                            for v in start_idx..end_idx {
                                let offset = new_offsets_ref[v] + local_offset;
                                new_offsets_ref[v] = offset;
                                let old_offset = offsets_ref[v];
                                let deg = degree_ref[v] as usize;
                                new_neighbors_ref[offset..offset + deg]
                                    .copy_from_slice(&neighbors_ref[old_offset..old_offset + deg]);
                            }
                        }
                    }
                });
            }
        });

        self.degree.resize(vertex_num, 0);
        let new_degree = &mut self.degree;
        if reverse {
            for (src, dst) in edges.iter() {
                let offset = new_offsets[dst.index()] + new_degree[dst.index()] as usize;
                new_degree[dst.index()] += 1;
                new_neighbors[offset] = *src;
            }
        } else {
            for (src, dst) in edges.iter() {
                let offset = new_offsets[src.index()] + new_degree[src.index()] as usize;
                new_degree[src.index()] += 1;
                new_neighbors[offset] = *dst;
            }
        }

        self.neighbors = new_neighbors;
        self.offsets = new_offsets;
        self.edge_num = cur_offset;
    }

    fn insert_edges_with_prop(
        &mut self, vertex_num: usize, edges: &Vec<(I, I)>, edges_prop: &ColTable, reverse: bool, p: u32,
        old_table: ColTable,
    ) -> ColTable {
        let mut new_degree = vec![0; vertex_num];
        if reverse {
            for e in edges.iter() {
                new_degree[e.1.index()] += 1;
            }
        } else {
            for e in edges.iter() {
                new_degree[e.0.index()] += 1;
            }
        }
        let mut new_table = old_table.new_empty();
        new_table.resize(self.edge_num + edges.len());

        let num_threads = p as usize;

        let old_vertex_num = self.offsets.len();
        let chunk_size = (((vertex_num + num_threads - 1) / num_threads) + 3) / 4;
        let chunk_num = (vertex_num + chunk_size - 1) / chunk_size;

        let mut chunk_offset = vec![0_usize; chunk_num];
        let safe_chunk_offset_ptr = SafeMutPtr::new(&mut chunk_offset);
        let mut new_offsets = ArrayType::with_capacity(vertex_num);
        new_offsets.resize(vertex_num, 0);
        let safe_new_offsets_ptr = SafeMutPtr::new(&mut new_offsets);
        let safe_new_degree_ptr = SafeMutPtr::new(&mut new_degree);
        let safe_degree_ptr = SafePtr::new(&self.degree);

        let chunk_i = AtomicUsize::new(0);

        rayon::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|_| {
                    let chunk_i_ref = &chunk_i;
                    let new_offsets_ref = safe_new_offsets_ptr.get_mut();
                    let chunk_offset_ref = safe_chunk_offset_ptr.get_mut();
                    let degree_ref = safe_degree_ptr.get_ref();
                    let new_degree_ref = safe_new_degree_ptr.get_mut();
                    loop {
                        let cur_chunk = chunk_i_ref.fetch_add(1, Ordering::Relaxed);
                        if cur_chunk >= chunk_num {
                            break;
                        }
                        let mut local_offset = 0_usize;

                        let start_idx = cur_chunk * chunk_size;
                        let end_idx = vertex_num.min(start_idx + chunk_size);

                        if end_idx > old_vertex_num {
                            if start_idx >= old_vertex_num {
                                for v in start_idx..end_idx {
                                    new_offsets_ref[v] = local_offset;
                                    local_offset += (new_degree[v]) as usize;
                                }
                            } else {
                                for v in start_idx..old_vertex_num {
                                    new_offsets_ref[v] = local_offset;
                                    local_offset += (degree_ref[v] + new_degree_ref[v]) as usize;
                                }
                                for v in old_vertex_num..end_idx {
                                    new_offsets_ref[v] = local_offset;
                                    local_offset += (new_degree[v]) as usize;
                                }
                            }
                        } else {
                            for v in start_idx..end_idx {
                                new_offsets_ref[v] = local_offset;
                                local_offset += (degree_ref[v] + new_degree_ref[v]) as usize;
                            }
                        }
                        chunk_offset_ref[cur_chunk] = local_offset;
                    }
                });
            }
        });
        let mut cur_offset = 0_usize;
        for i in 0..chunk_num {
            let tmp = chunk_offset[i] + cur_offset;
            chunk_offset[i] = cur_offset;
            cur_offset = tmp;
        }

        let mut new_neighbors = ArrayType::with_capacity(cur_offset);
        new_neighbors.resize(cur_offset, I::new(0));

        let safe_new_neighbors_ptr = SafeMutPtr::new(&mut new_neighbors);
        let safe_neighbors_ptr = SafePtr::new(&self.neighbors);
        let safe_offsets_ptr = SafePtr::new(&self.offsets);

        let safe_new_table_ptr = SafeMutPtr::new(&mut new_table);
        let safe_old_table_ptr = SafePtr::new(&old_table);

        let chunk_i = AtomicUsize::new(0);

        rayon::scope(|s| {
            for _ in 0..num_threads {
                s.spawn(|_| {
                    let chunk_i_ref = &chunk_i;
                    let neighbors_ref = safe_neighbors_ptr.get_ref();
                    let new_neighbors_ref = safe_new_neighbors_ptr.get_mut();
                    let new_offsets_ref = safe_new_offsets_ptr.get_mut();
                    let chunk_offset_ref = safe_chunk_offset_ptr.get_mut();
                    let offsets_ref = safe_offsets_ptr.get_ref();
                    let degree_ref = safe_degree_ptr.get_ref();
                    let new_table_ref = safe_new_table_ptr.get_mut();
                    let old_table_ref = safe_old_table_ptr.get_ref();

                    loop {
                        let cur_chunk = chunk_i_ref.fetch_add(1, Ordering::Relaxed);
                        if cur_chunk >= chunk_num {
                            break;
                        }

                        let local_offset = chunk_offset_ref[cur_chunk];
                        let start_idx = cur_chunk * chunk_size;
                        let end_idx = vertex_num.min(start_idx + chunk_size);

                        if end_idx > old_vertex_num {
                            if start_idx >= old_vertex_num {
                                for v in start_idx..end_idx {
                                    new_offsets_ref[v] += local_offset;
                                }
                            } else {
                                for v in start_idx..old_vertex_num {
                                    let offset = new_offsets_ref[v] + local_offset;
                                    new_offsets_ref[v] = offset;
                                    let old_offset = offsets_ref[v];
                                    let deg = degree_ref[v] as usize;
                                    new_neighbors_ref[offset..offset + deg]
                                        .copy_from_slice(&neighbors_ref[old_offset..old_offset + deg]);
                                    new_table_ref.copy_range(offset, old_table_ref, old_offset, deg);
                                }
                                for v in old_vertex_num..end_idx {
                                    new_offsets_ref[v] += local_offset;
                                }
                            }
                        } else {
                            for v in start_idx..end_idx {
                                let offset = new_offsets_ref[v] + local_offset;
                                new_offsets_ref[v] = offset;
                                let old_offset = offsets_ref[v];
                                let deg = degree_ref[v] as usize;
                                new_neighbors_ref[offset..offset + deg]
                                    .copy_from_slice(&neighbors_ref[old_offset..old_offset + deg]);
                                new_table_ref.copy_range(offset, old_table_ref, old_offset, deg);
                            }
                        }
                    }
                });
            }
        });

        self.degree.resize(vertex_num, 0);
        let new_degree = &mut self.degree;
        if reverse {
            for (row_i, (src, dst)) in edges.iter().enumerate() {
                let offset = new_offsets[dst.index()] + new_degree[dst.index()] as usize;
                new_degree[dst.index()] += 1;
                new_neighbors[offset] = *src;
                new_table.set_table_row(offset, edges_prop, row_i);
            }
        } else {
            for (row_i, (src, dst)) in edges.iter().enumerate() {
                let offset = new_offsets[src.index()] + new_degree[src.index()] as usize;
                new_degree[src.index()] += 1;
                new_neighbors[offset] = *dst;
                new_table.set_table_row(offset, edges_prop, row_i);
            }
        }

        self.neighbors = new_neighbors;
        self.offsets = new_offsets;
        self.edge_num = cur_offset;

        new_table
    }
}
