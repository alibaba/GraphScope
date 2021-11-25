#[macro_use]
extern crate lazy_static;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::time::Instant;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use memmap::Mmap;
use nohash_hasher::{BuildNoHashHasher, IntMap};

use crate::graph::IdGraph;

pub mod graph;

pub type Graph = IdGraph<NeighborsBackend>;

pub fn extract_partition<P: AsRef<Path>>(
    path: P, partition: u64, partitions: usize,
) -> std::io::Result<()> {
    let p = partitions as u64;
    encode(path, move |id| id % p == partition)
}

pub fn partition<P: AsRef<Path>>(_path: P, _partitions: usize) -> std::io::Result<Vec<Graph>> {
    todo!()
}

pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<IdGraph<NeighborsBackend>> {
    if path.as_ref().extension() == Some(OsStr::new("bin")) {
        println!("start load binary ... ");
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        println!("load binary file with len = {}", mmap.len());
        load_bin(mmap)
    } else {
        let p = path.as_ref().with_extension("bin");
        match File::open(p) {
            Ok(file) => {
                println!("start load binary ... ");
                let mmap = unsafe { Mmap::map(&file)? };
                load_bin(mmap)
            }
            Err(_) => {
                panic!("can't load graph as unsupported file format")
            }
        }
    }
}

fn load_bin(binary: Mmap) -> std::io::Result<Graph> {
    println!("load binary file with len = {}", binary.len());
    let start = Instant::now();
    let vertices = {
        let mut reader = binary.deref();
        let size = reader.read_u64::<LittleEndian>()? as usize;
        let (_, mut right) = reader.split_at(size);
        let vertices: Vec<(u64, u64, u64)> = read_dump(&mut right)?;
        let mut vertex_index =
            HashMap::with_capacity_and_hasher(vertices.len(), BuildNoHashHasher::default());
        for (id, offset, len) in vertices {
            vertex_index.insert(id, (offset, len));
        }
        vertex_index
    };

    let backend = NeighborsBackend::new(binary);
    let graph = IdGraph::new(vertices, backend);
    println!(
        "load graph with {} vertices, {} edges, cost {:?}",
        graph.total_vertices(),
        graph.total_edges(),
        start.elapsed()
    );
    Ok(graph)
}

pub fn encode<P: AsRef<Path>, F: Fn(&u64) -> bool>(path: P, filter: F) -> std::io::Result<()> {
    let as_bin = path.as_ref().with_extension("bin");
    let reader = BufReader::new(File::open(path)?);
    let spt = std::env::var("GRAPH_SPLIT")
        .unwrap_or(" ".to_owned())
        .parse::<char>()
        .unwrap();
    println!("use split '{}'", spt);

    let skip_head = std::env::var("GRAPH_SKIP_HEAD")
        .unwrap_or("0".to_owned())
        .parse::<u32>()
        .unwrap() as usize;

    if skip_head > 0 {
        println!("skip {} lines at head;", skip_head);
    }

    let src_index = std::env::var("GRAPH_SRC_INDEX")
        .unwrap_or("0".to_owned())
        .parse::<usize>()
        .unwrap();

    println!("will read source id at col[{}]", src_index);

    let dst = std::env::var("GRAPH_DST_INDEXES").unwrap_or("1".to_owned());

    let mut dst_indexes = vec![];
    for idx in dst.split(',') {
        let i = idx
            .parse::<usize>()
            .expect("unknown index format");
        dst_indexes.push(i);
    }
    dst_indexes.sort();
    println!("will read target id at col[{:?}]", dst_indexes);

    let mut edges: IntMap<u64, Vec<u64>> = IntMap::default();
    let mut vertices: IntMap<u64, (u64, u64)> = IntMap::default();
    let mut count = 0;
    let start = Instant::now();
    let mut tmp = Vec::with_capacity(dst_indexes.len() + 1);
    for (i, edge) in reader.lines().enumerate() {
        if i < skip_head {
            continue;
        }
        tmp.clear();
        match edge {
            Ok(e) => {
                for x in e.split(spt) {
                    tmp.push(x.to_owned())
                }
                if let Some(s) = tmp.get::<usize>(src_index) {
                    let src = s
                        .parse::<u64>()
                        .expect("error source id format");
                    if filter(&src) {
                        vertices.insert(src, (0, 0));
                        let targets = edges.entry(src).or_insert_with(Vec::new);
                        for i in dst_indexes.iter() {
                            if let Some(d) = tmp.get::<usize>(*i) {
                                let dst = d
                                    .parse::<u64>()
                                    .expect("error target id format");
                                targets.push(dst);
                                vertices.insert(dst, (0, 0));
                                count += 1;
                            } else {
                                panic!("error format line : {}", e);
                            }
                        }
                    }
                } else {
                    panic!("error format line: {}", e);
                }
            }
            Err(e) => {
                panic!("can't parse line to string {}", e);
            }
        }
        if count % 1_000_000 == 0 && count != 0 {
            println!("already load {} edges, use {:?}", count, start.elapsed());
        }
    }

    let mut offset = 0;
    let mut neighbors = vec![];
    for (id, edges) in edges {
        let (idx, len) = vertices.entry(id).or_insert((0, 0));
        *idx = offset as u64;
        *len = edges.len() as u64;
        assert_ne!(*len, 0);
        offset += edges.len();
        neighbors.extend(edges);
    }

    let mut vertex_vec = Vec::with_capacity(vertices.len());
    for (id, (mut idx, len)) in vertices {
        if len == 0 {
            idx = offset as u64;
        }
        vertex_vec.push((id, idx, len));
    }

    let mut length = (2 + neighbors.len()) * std::mem::size_of::<u64>();
    length += vertex_vec.len() * std::mem::size_of::<(u64, u64, u64)>();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(as_bin)?;
    file.set_len(length as u64)?;
    let mmap = unsafe { Mmap::map(&file) }?;
    let mut bytes = mmap.make_mut()?;
    let mut writer = bytes.deref_mut();
    dump(&mut writer, &neighbors)?;
    dump(&mut writer, &vertex_vec)?;
    writer.flush()?;
    bytes.flush()
}

#[inline]
fn dump<T: Copy, W: Write>(writer: &mut W, list: &Vec<T>) -> std::io::Result<()> {
    let len = list.len();
    let buf = unsafe {
        let ptr = list.as_ptr() as *const u8;
        let size = len * std::mem::size_of::<T>();
        std::slice::from_raw_parts(ptr, size)
    };
    writer.write_u64::<LittleEndian>(buf.len() as u64)?;
    writer.write_all(buf)?;
    Ok(())
}

#[inline]
fn read_dump<T: Copy, R: Read>(reader: &mut R) -> std::io::Result<Vec<T>> {
    let size = reader.read_u64::<LittleEndian>()? as usize;
    if size > 0 {
        let mut buf = vec![0u8; size];
        reader.read_exact(&mut buf)?;
        let list = unsafe {
            let ptr = buf.as_ptr() as *mut T;
            let length = size / std::mem::size_of::<T>();
            Vec::from_raw_parts(ptr, length, length)
        };
        std::mem::forget(buf);
        Ok(list)
    } else {
        Ok(vec![])
    }
}

pub struct NeighborsBackend {
    mmap: Mmap,
    len: usize,
}

impl NeighborsBackend {
    fn new(mmap: Mmap) -> Self {
        let size = mmap
            .deref()
            .read_u64::<LittleEndian>()
            .expect("unknown error;") as usize;
        let len = size / std::mem::size_of::<u64>();
        NeighborsBackend { mmap, len }
    }
}

impl Deref for NeighborsBackend {
    type Target = [u64];

    fn deref(&self) -> &Self::Target {
        let ptr = self.mmap.as_ptr() as *const u64;
        unsafe { std::slice::from_raw_parts(ptr, self.len) }
    }
}
