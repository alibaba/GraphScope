//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::path::Path;
use std::io::{BufReader, BufRead, ErrorKind, BufWriter, Write, Read};
use std::fs::File;
use std::collections::HashMap;
use std::cmp::min;
use std::time::Instant;
use std::sync::Arc;
use byteorder::{BigEndian, WriteBytesExt, ByteOrder};

pub struct SegmentList<T> {
    shift: usize,
    seg_size: usize,
    segments: Vec<Vec<T>>,
    current: Vec<T>,
    len: usize,
}

impl<T> SegmentList<T> {
    pub fn new(shift: usize) -> Self {
        let seg_size = 1 << shift;
        SegmentList {
            shift,
            seg_size,
            segments: Vec::new(),
            current: Vec::with_capacity(seg_size),
            len: 0,
        }
    }

    pub fn push(&mut self, e: T) {
        self.current.push(e);
        if self.current.len() == self.seg_size {
            self.segments.push(::std::mem::replace(&mut self.current,
                                                   Vec::with_capacity(self.seg_size)));
        }
        self.len += 1;
    }

    pub fn get(&self, offset: usize) -> Option<&T> {
        let seg = offset >> self.shift;
        let offset = offset - self.seg_size * seg;
        if seg > self.segments.len() {
            None
        } else if seg == self.segments.len() {
            Some(&self.current[offset])
        } else {
            Some(&self.segments[seg][offset])
        }
    }

    pub fn get_multi(&self, start: usize, len: usize) -> Result<Vec<&T>, String> {
        let mut tmp = Vec::with_capacity(len);
        let mut seg = start >> self.shift;
        let offset = start - self.seg_size * seg;
        let mut left = len;
        let mut start = offset;
        while left > 0 {
            let end = min(left, self.seg_size - start);
            let read = self.get_in_seg(seg, start, end)?;
            for e in read.iter() {
                tmp.push(e);
            }
            seg += 1;
            start = 0;
            left -= read.len();
        }

        Ok(tmp)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn get_in_seg(&self, seg: usize, start: usize, len: usize) -> Result<&[T], String> {
        let end = start + len;
        if seg > self.segments.len() {
            Err("Index out of bound".to_owned())
        } else if seg == self.segments.len() {
            if end > self.current.len() {
                Err("Index out of bound".to_owned())
            } else {
                Ok(&self.current[start..end])
            }
        } else {
            Ok(&self.segments[seg][start..end])
        }
    }
}

/// 1 -> (2,3,4),
/// 2 -> 3,
/// 4 -> 5,
/// 5 -> (1, 3),
/// 6 -> (7, 8),
/// 7 -> 8
const DEFAULT_GRAPH: [(u64, u64); 10] = [(1, 2), (1, 3), (1, 4), (2, 3), (5, 1), (5, 3), (4, 5), (6, 7), (6, 8), (7, 8)];

#[allow(dead_code)]
pub struct GraphTopology {
    partition: u32,
    peers: u32,
    count: usize,
    neighbors: HashMap<u64, Arc<Vec<u64>>>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Abomonation)]
pub struct Vertex {
    pub id: u64,
    #[cfg(feature = "padding")]
    padding_1: [u64; 8],
    #[cfg(feature = "padding")]
    padding_2: [u64; 7],
}

impl Vertex {
    pub fn new(id: u64) -> Self {
        Vertex {
            id,
            #[cfg(feature = "padding")]
            padding_1: [0; 8],
            #[cfg(feature = "padding")]
            padding_2: [0; 7]
        }
    }
}

pub struct NeighborIter {
    cursor: usize,
    len: usize,
    inner: Arc<Vec<u64>>
}

impl NeighborIter {
    pub fn new(neighbors: &Arc<Vec<u64>>) -> Self {
        NeighborIter {
            cursor: 0,
            len: neighbors.len(),
            inner: neighbors.clone(),
        }
    }

    pub fn empty() -> Self {
        NeighborIter {
            cursor: 0,
            len: 0,
            inner: Arc::new(vec![])
        }
    }
}

impl Iterator for NeighborIter {
    type Item = Vertex;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor == self.len {
            None
        } else {
            self.cursor += 1;
            Some(Vertex::new(self.inner[self.cursor - 1]))
        }
    }
}



impl GraphTopology {
    pub fn with_default(partition: u32, peers: u32, directed: bool) -> Self {
        let mut neighbors = HashMap::new();
        let mut count = 0;
        for (s, d) in DEFAULT_GRAPH.iter() {
            if peers == 1 || (s % peers as u64) as u32 == partition {
                let n = neighbors.entry(*s).or_insert(Vec::new());
                n.push(*d);
                count += 1;
            }

            if peers == 1 || (d % peers as u64) as u32 == partition {
                let n = neighbors.entry(*d).or_insert(Vec::new());
                if !directed {
                    n.push(*s);
                    count += 1;
                }
            }
        }
        let mut arc_neighbors = HashMap::new();
        for (k, v) in neighbors.drain() {
            arc_neighbors.insert(k, Arc::new(v));
        }

        GraphTopology {
            partition,
            count,
            peers,
            neighbors: arc_neighbors
        }
    }

    pub fn load<P: AsRef<Path>>(partition: u32, peers: u32, directed: bool, split: char, path: P) -> Self {
        let as_bin = path.as_ref().with_extension("bin");
        Self::convert_to_bin(path, as_bin.as_path(), split);
        info!("Convert raw file format to binary {:?}", as_bin.as_os_str());
        Self::load_bin(partition, peers, directed, as_bin.as_path())
    }

    /// Load graph from binary file.
    ///
    /// The binary file should follow this format: src1 dst1 src2 dst2 src3 dst3 ...
    /// Vertex IDs are 32-bit big endian integers.
    pub fn load_bin<P: AsRef<Path>>(partition: u32, peers: u32, directed: bool, path: P) -> Self {
        let mut reader = BufReader::new(File::open(path).unwrap());
        //let mut reader = File::open(path).unwrap();
        let mut neighbors = HashMap::new();
        let mut count = 0_usize;
        let mut start = ::std::time::Instant::now();
        let mut buffer = [0u8;1<< 12];
        let peers = peers as u64;
        loop {
            let read = match reader.read(&mut buffer[0..]) {
                Ok(n) => n,
                Err(e) => {
                    if let ErrorKind::UnexpectedEof = e.kind() {
                        break
                    } else {
                        panic!("{}",e);
                    }
                }
            };

            if read > 0 {
                assert!(read % 8 == 0, "unexpected: read {} bytes", read);
                let valid = &mut buffer[0..read];
                let mut extract = 0;
                while extract < read {
                    let src = BigEndian::read_u64(&valid[extract..]);
                    let dst = BigEndian::read_u64(&valid[extract + 8..]);
                    if peers == 1 || (src % peers) as u32 == partition {
                        let n = neighbors.entry(src).or_insert_with(|| Vec::new());
                        n.push(dst);
                    }
                    if !directed && (peers == 1 || (dst % peers) as u32 == partition) {
                        let n = neighbors.entry(dst).or_insert_with(|| Vec::new());
                        n.push(src);
                    }

                    count += 1;
                    if log::log_enabled!(log::Level::Debug) {
                        if count % 5000000 == 0 {
                            let duration_ms = (Instant::now() - start).as_millis() as f64;
                            let speed = 5000000.0 / duration_ms * 1000.0;
                            debug!("Scanned edges: {}, speed: {:.2}/s", count, speed);
                            start = ::std::time::Instant::now();
                        }
                    }

                    extract += 16;
                }
            } else {
               break
            }
        }

        let mut arc_neighbors = HashMap::new();
        for (k, v) in neighbors.drain() {
            arc_neighbors.insert(k, Arc::new(v));
        }
        GraphTopology {
            partition,
            count,
            peers: peers as u32,
            neighbors: arc_neighbors,
        }
    }

    /// Convert graph file from raw text format to binary format.
    /// The binary file should follow this format: src1 dst1 src2 dst2 src3 dst3 ...
    /// Vertex IDs are 32-bit big endian integers.
    pub fn  convert_to_bin<P1: AsRef<Path>, P2: AsRef<Path>>(input: P1, output: P2, split: char) {
        let reader = BufReader::new(File::open(input).unwrap());
        let mut writer = BufWriter::new(File::create(output).unwrap());
        let mut count = 0_usize;
        let mut start = ::std::time::Instant::now();
        for edge in reader.lines() {
            let edge = edge.unwrap();
            let edge = edge.split(split).collect::<Vec<_>>();
            let src: u64 = edge[0].parse().unwrap();
            let dst: u64 = edge[1].parse().unwrap();
            writer.write_u64::<BigEndian>(src).unwrap();
            writer.write_u64::<BigEndian>(dst).unwrap();

            count += 1;
            if count % 5000000 == 0 {
                let duration_ms = (Instant::now() - start).as_millis() as f64;
                let speed = 5000000.0 / duration_ms * 1000.0;
                debug!("Scanned edges: {}, speed: {:.2}/s", count, speed);
                start = ::std::time::Instant::now();
            }
        }

        writer.flush().unwrap();
    }

    pub fn get_neighbors(&self, src: &u64) -> Option<NeighborIter> {
        self.neighbors.get(src).map(|n| {
            NeighborIter::new(n)
        })
    }

    #[inline]
    pub fn count_nodes(&self) -> usize {
        self.neighbors.len()
    }

    #[inline]
    pub fn count_edges(&self) -> usize {
        self.count
    }
}



#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_segment_list() {
        let mut list = SegmentList::new(6);
        for i in 0..1024 {
            list.push(i);
        }


        for i in 0..1024 {
            let e = list.get(i as usize).unwrap();
            assert_eq!(i, *e);
        }

        for i in 0..1014 {
            let res = list.get_multi(i as usize, 10).unwrap();
            //println!("get res {:?}", res);
            for j in 0..10 {
                assert_eq!(i + j, *res[j]);
            }
        }
    }

    #[test]
    fn test_graph_load() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("data/twitter_rv.net");
        {
            println!("dir is : {}", d.display());
            let graph = GraphTopology::load(1, 1, true, ' ', d.as_path());
            println!("finish load");
            let n = graph.get_neighbors(&12).unwrap()
                .fold(0, |count, _| count + 1);
            assert_eq!(n, 4);
        }

        {
            let graph = GraphTopology::load_bin(1, 1, true, d.as_path().with_extension("bin"));
            let n = graph.get_neighbors(&12).unwrap()
                .map(|v| {
                    println!("get v : {}", v.id);
                    v
                })
                .fold(0, |count, _| count + 1);
            assert_eq!(n, 4);
        }
    }

    #[test]
    fn test_graph() {
        let graph = GraphTopology::with_default(3, 1, true);
        {
            let mut ns = vec![];
            for n in graph.get_neighbors(&1).unwrap() {
                ns.push(n);
            }
            let mut ns = ns.into_iter().map(|v| v.id).collect::<Vec<_>>();
            ns.sort();
            assert_eq!(ns, vec![2, 3, 4]);
        }

        {
            let mut ns = vec![];
            for n in graph.get_neighbors(&6).unwrap() {
                ns.push(n);
            }
            let mut ns = ns.into_iter().map(|v| v.id).collect::<Vec<_>>();
            ns.sort();
            assert_eq!(ns, vec![7, 8]);
        }
    }
}
