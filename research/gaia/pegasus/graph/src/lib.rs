use std::sync::Arc;
use std::path::Path;
use std::io::{BufReader, BufRead, Write, Read};
use std::fs::{File, OpenOptions};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::time::Instant;
use std::ffi::OsStr;
use nohash_hasher::{IntMap, BuildNoHashHasher};
use std::collections::HashMap;
use rand::Rng;
use memmap::Mmap;
use std::ops::{DerefMut, Deref};

pub struct Graph {
    topology : IntMap<u64, Arc<Vec<u64>>>,
    id_range: (u64, u64),
}

impl Graph {
    #[inline]
    pub fn get_neighbors(&self, id: u64) -> Neighbors {
        self.topology.get(&id).map(|list| Neighbors::new(list))
            .unwrap_or(Neighbors::empty())
    }

    #[inline]
    pub fn count_neighbors(&self, id: u64) -> usize {
        self.topology.get(&id).map(|list| list.len()).unwrap_or(0)
    }

    pub fn sample_vertices(&self, size: usize) -> Vec<u64> {
        let mut rn = rand::thread_rng();
        let mut sample = Vec::with_capacity(size);
        while sample.len() < size {
            let src = rn.gen_range(self.id_range.0 .. self.id_range.1);
            if self.count_neighbors(src) > 0 {
                sample.push(src);
            }
        }
        return sample;
    }
}

pub struct Neighbors {
    ptr: Arc<Vec<u64>>,
    cursor: usize,
}

impl Neighbors {
    pub fn new(ptr: &Arc<Vec<u64>>) -> Self {
        Neighbors {
            ptr : ptr.clone(),
            cursor: 0
        }
    }

    pub fn empty() -> Self {
        Neighbors {
            ptr : Arc::new(vec![]),
            cursor: 0
        }
    }
}

impl Iterator for Neighbors {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if  self.cursor < self.ptr.len() {
            self.cursor += 1;
            Some(self.ptr[self.cursor - 1])
        } else {
            None
        }
    }
}


pub fn load<P: AsRef<Path>>(path: P) -> std::io::Result<Graph> {
    let start = Instant::now();
    let graph = if path.as_ref().extension() == Some(OsStr::new("bin")) {
        println!("start load binary ... ");
        //let reader = BufReader::new(snap::read::FrameDecoder::new(File::open(path)?));
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? } ;
        let reader = mmap.deref();
        load_bin(reader)
    } else {
        let p = path.as_ref().with_extension("bin");
        match File::open(p) {
            Ok(file) => {
                println!("start load binary ... ");
                //let reader = snap::read::FrameDecoder::new(file);
                let mmap = unsafe { Mmap::map(&file)? } ;
                let reader = mmap.deref();
                load_bin(reader)
            },
            Err(_) => {
                println!("start load raw ...");
                load_raw(path)
            }
        }

    }?;
    println!("load graph with {} vertices, cost {:?}", graph.topology.len() , start.elapsed());
    Ok(graph)
}

fn load_bin<R: Read>(mut reader: R) -> std::io::Result<Graph> {
    let min_id = reader.read_u64::<LittleEndian>()?;
    let max_id = reader.read_u64::<LittleEndian>()?;
    let vertices = read_id_list(&mut reader)?;
    let mut map = HashMap::with_capacity_and_hasher(vertices.len(), BuildNoHashHasher::default());
    for src in vertices {
        let list = read_id_list(&mut reader)?;
        map.insert(src, Arc::new(list));
    }
Ok(Graph { topology: map, id_range: (min_id, max_id) })
}

fn load_raw<P: AsRef<Path>>(path: P) -> std::io::Result<Graph> {
    let as_bin = path.as_ref().with_extension("bin");
    let reader = BufReader::new(File::open(path)?);
    let spt = std::env::var("GRAPH_SPLIT").unwrap_or(" ".to_owned()).parse::<char>().unwrap();
    println!("use split '{}'", spt);
    let mut map: IntMap<u64, Vec<u64>> = IntMap::default();
    let mut count = 0;
    let mut max_id = 0;
    let mut min_id = !0u64;
    let start = Instant::now();
    for (i, edge) in reader.lines().enumerate() {
        match edge {
            Ok(e) => {
                let mut ids = e.split(spt).filter(|s| !s.is_empty());
                if let Some(src) = ids.next() {
                    match src.trim().parse::<u64>() {
                        Ok(src_id) => {
                            max_id = std::cmp::max(src_id, max_id);
                            min_id = std::cmp::min(src_id, min_id);
                            let nei = map.entry(src_id).or_insert_with(|| Vec::new());
                            for dst in ids {
                                match dst.trim().parse::<u64>() {
                                    Ok(dst_id) => {
                                        max_id = std::cmp::max(dst_id, max_id);
                                        min_id = std::cmp::min(dst_id, min_id);
                                        nei.push(dst_id);
                                        count += 1;
                                    },
                                    Err(e) => {
                                        eprintln!("can't parse line {}: {} ", i, e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("can't parse line {}:'{}'", i, e);
                        }
                    }
                } else {
                    eprintln!("invalid line: {}: {}", i, e);
                }
            },
            Err(e) => {
                eprintln!("can't parse line to string {}", e);
            }
        }
        if count % 1000_000 == 0 && count != 0 {
            println!("already load {} edges, use {:?}", count, start.elapsed());
        }
    }

    let size = map.len();
    let mut graph = HashMap::with_capacity_and_hasher(size, BuildNoHashHasher::default());
    let mut vertices = Vec::with_capacity(size);
    let mut length = size + 3;
    for (id, edges) in map.iter() {
        vertices.push(*id);
        length += edges.len();
    }

    length = (length * std::mem::size_of::<u64>()).next_power_of_two();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true).open(as_bin)?;
    file.set_len(length as u64)?;
    let mmap = unsafe { Mmap::map(&file) }?;
    let mut bytes = mmap.make_mut()?;
    let mut writer = bytes.deref_mut();
    writer.write_u64::<LittleEndian>(min_id)?;
    writer.write_u64::<LittleEndian>(max_id)?;
    write_id_list(&mut writer, &vertices)?;

    for (k, v) in map {
        write_id_list(&mut writer, &v)?;
        graph.insert(k, Arc::new(v));
    }

    bytes.flush()?;
    Ok(Graph { topology: graph, id_range: (min_id, max_id) })
}


#[inline]
fn write_id_list<W: Write>(writer: &mut W, list: &Vec<u64>) -> std::io::Result<()> {
    let len = list.len();
    writer.write_u64::<LittleEndian>(len as u64)?;
    let buf = unsafe {
        let ptr = list.as_ptr() as *const u8;
        let size = len * std::mem::size_of::<u64>();
        std::slice::from_raw_parts(ptr, size)
    };
    writer.write_all(buf)?;
    Ok(())
}

#[inline]
fn read_id_list<R: Read>(reader: &mut R) -> std::io::Result<Vec<u64>> {
    let size = reader.read_u64::<LittleEndian>()? as usize;
    if size > 0 {
        let length = size * std::mem::size_of::<u64>();
        let mut buf = vec![0u8; length];
        reader.read_exact(&mut buf)?;
        let list = unsafe {
            let ptr = buf.as_ptr() as *mut u64;
            Vec::from_raw_parts(ptr, size, size)
        };
        std::mem::forget(buf);
        Ok(list)
    } else {
        Ok(vec![])
    }

}