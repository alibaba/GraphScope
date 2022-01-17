use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::Deref;

use ahash::AHashMap;
use crossbeam_utils::sync::ShardedLock;
use nohash_hasher::IntMap;
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};

use crate::topo::IdTopo;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Direction {
    Out,
    In,
    Both,
}

struct SharedMap<K: Hash + Eq, V> {
    map: ShardedLock<AHashMap<K, V>>,
}

impl<K: Hash + Eq, V> SharedMap<K, V> {
    fn new() -> Self {
        SharedMap { map: ShardedLock::new(AHashMap::new()) }
    }

    fn insert(&self, key: K, value: V) -> Option<V> {
        let mut write = self.map.write().expect("write lock failure");
        write.insert(key, value)
    }
}

impl<K: Hash + Eq, V: Copy> SharedMap<K, V> {
    fn get<Q: ?Sized>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let map = self.map.read().expect("read lock failure");
        map.get(key).copied()
    }
}

struct SharedVec<T> {
    vec: ShardedLock<Vec<T>>,
}

impl<T> SharedVec<T> {
    fn new() -> Self {
        SharedVec { vec: ShardedLock::new(Vec::new()) }
    }

    fn push(&self, entry: T) -> usize {
        let mut write = self.vec.write().expect("write lock failure");
        write.push(entry);
        write.len() - 1
    }
}

impl<T: Clone> SharedVec<T> {
    fn get(&self, index: usize) -> Option<T> {
        let read = self.vec.read().expect("read lock failure");
        read.get(index).cloned()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct EdgeMeta {
    pub src_label_id: u32,
    pub edge_label_id: u32,
    pub dst_label_id: u32,
}

impl From<(u32, u32, u32)> for EdgeMeta {
    fn from(tuple: (u32, u32, u32)) -> Self {
        EdgeMeta { src_label_id: tuple.0, edge_label_id: tuple.1, dst_label_id: tuple.2 }
    }
}

impl EdgeMeta {
    pub fn get_edge_label(&self) -> Option<String> {
        ID_TO_LABEL.get(self.edge_label_id as usize)
    }

    pub fn get_src_label(&self) -> Option<String> {
        ID_TO_LABEL.get(self.src_label_id as usize)
    }

    pub fn get_dst_label(&self) -> Option<String> {
        ID_TO_LABEL.get(self.dst_label_id as usize)
    }
}

lazy_static! {
    static ref LABEL_TO_ID: SharedMap<String, u32> = SharedMap::new();
    static ref ID_TO_LABEL: SharedVec<String> = SharedVec::new();
    static ref EDGE_META: SharedMap<u32, EdgeMeta> = SharedMap::new();
}

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub struct VID(u32, u64);

impl VID {
    #[inline]
    pub fn label_id(&self) -> u32 {
        self.0
    }

    #[inline]
    pub fn vertex_id(&self) -> u64 {
        self.1
    }

    pub fn get_label(&self) -> Option<String> {
        ID_TO_LABEL.get(self.0 as usize)
    }
}

impl From<(u32, u64)> for VID {
    fn from(tuple: (u32, u64)) -> Self {
        VID(tuple.0, tuple.1)
    }
}

impl Encode for VID {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        (self.0, self.1).write_to(writer)
    }
}

impl Decode for VID {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let (label, id) = <(u32, u64)>::read_from(reader)?;
        Ok(VID(label, id))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct TopoMeta {
    meta_id: usize,
    src_type_id: u32,
    dst_type_id: u32,
    edge_type_id: u32,
    kind: Direction,
}

pub struct LabeledTopoGraph<B: Deref<Target = [u64]>> {
    v2e_metas: IntMap<u32, Vec<TopoMeta>>,
    topos: Vec<IdTopo<B>>,
}

impl<B: Deref<Target = [u64]>> LabeledTopoGraph<B> {
    fn get_or_create_label_id(label: &str) -> u32 {
        if let Some(id) = LABEL_TO_ID.get(label) {
            return id;
        } else {
            let next_id = ID_TO_LABEL.push(label.to_owned());
            LABEL_TO_ID.insert(label.to_owned(), next_id as u32);
            next_id as u32
        }
    }

    #[inline]
    pub fn get_label_id(label: &str) -> Option<u32> {
        LABEL_TO_ID.get(label)
    }

    pub fn get_edge_info(label: &str) -> Option<EdgeMeta> {
        let id = LABEL_TO_ID.get(label)?;
        EDGE_META.get(&id)
    }

    pub fn add_out_topo(&mut self, src_type: String, edge_type: String, dst_type: String, topo: IdTopo<B>) {
        let src_type_id = Self::get_or_create_label_id(&src_type);
        let edge_type_id = Self::get_or_create_label_id(&edge_type);
        let dst_type_id = Self::get_or_create_label_id(&dst_type);
        let meta_id = self.topos.len();
        let metas = self
            .v2e_metas
            .entry(src_type_id)
            .or_insert_with(Vec::new);
        let meta = TopoMeta { meta_id, src_type_id, dst_type_id, edge_type_id, kind: Direction::Out };
        metas.push(meta);
        self.topos.push(topo);
        EDGE_META.insert(edge_type_id, (src_type_id, edge_type_id, dst_type_id).into());
    }

    pub fn add_in_topo(&mut self, src_type: String, edge_type: String, dst_type: String, topo: IdTopo<B>) {
        let src_type_id = Self::get_or_create_label_id(&src_type);
        let edge_type_id = Self::get_or_create_label_id(&edge_type);
        let dst_type_id = Self::get_or_create_label_id(&dst_type);
        let meta_id = self.topos.len();
        let metas = self
            .v2e_metas
            .entry(dst_type_id)
            .or_insert_with(Vec::new);
        let meta = TopoMeta { meta_id, src_type_id, dst_type_id, edge_type_id, kind: Direction::In };
        metas.push(meta);
        self.topos.push(topo);
        EDGE_META.insert(edge_type_id, (src_type_id, edge_type_id, dst_type_id).into());
    }

    pub fn add_nodirct_topo(&mut self, vtype: String, edge_type: String, topo: IdTopo<B>) {
        let src_type_id = Self::get_or_create_label_id(&vtype);
        let edge_type_id = Self::get_or_create_label_id(&edge_type);
        let meta_id = self.topos.len();
        let metas = self
            .v2e_metas
            .entry(src_type_id)
            .or_insert_with(Vec::new);
        let meta = TopoMeta {
            meta_id,
            src_type_id,
            dst_type_id: src_type_id,
            edge_type_id,
            kind: Direction::Both,
        };
        metas.push(meta);
        self.topos.push(topo);
        EDGE_META.insert(edge_type_id, (src_type_id, edge_type_id, src_type_id).into());
    }

    pub fn count_all_edge(&self, vid: u64, vtype: &str, dir: Direction) -> usize {
        if let Some(id) = LABEL_TO_ID.get(vtype) {
            self.count_all_edge_(VID(id, vid), dir)
        } else {
            0
        }
    }

    pub fn count_all_edge_(&self, vid: VID, dir: Direction) -> usize {
        let mut count = 0;
        if let Some(metas) = self.v2e_metas.get(&vid.label_id()) {
            for m in metas {
                if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                    count += self.topos[m.meta_id].count_neighbors(vid.vertex_id())
                }
            }
        }
        count
    }

    pub fn get_all_neighbors(&self, vid: u64, vtype: &str, dir: Direction) -> impl Iterator<Item = VID> {
        if let Some(label_id) = Self::get_label_id(vtype) {
            self.get_all_neighbors_(VID(label_id, vid), dir)
        } else {
            panic!("label of {} not found", vtype)
        }
    }

    pub fn get_all_neighbors_(&self, vid: VID, dir: Direction) -> impl Iterator<Item = VID> {
        let mut result = vec![];
        if let Some(metas) = self.v2e_metas.get(&vid.label_id()) {
            for m in metas {
                if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                    let label_id = m.dst_type_id;
                    let neighbors = self.topos[m.meta_id]
                        .get_neighbors(vid.vertex_id())
                        .map(move |id| VID(label_id, id));
                    result.push(neighbors);
                }
            }
        }
        result.into_iter().flat_map(|n| n.into_iter())
    }

    pub fn get_neighbors_through(
        &self, vid: u64, vtype: &str, edge_label: &str, dir: Direction,
    ) -> impl Iterator<Item = u64> {
        if let Some(label_id) = LABEL_TO_ID.get(vtype) {
            VidIterator::Iter(self.get_neighbors_through_(VID(label_id, vid), edge_label, dir))
        } else {
            VidIterator::None
        }
    }

    pub fn get_neighbors_through_(
        &self, vid: VID, edge_label: &str, dir: Direction,
    ) -> impl Iterator<Item = u64> {
        if let Some(label_id) = LABEL_TO_ID.get(edge_label) {
            if let Some(meta) = self.v2e_metas.get(&vid.label_id()) {
                for m in meta {
                    if m.edge_type_id == label_id {
                        if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                            let n = self.topos[m.meta_id].get_neighbors(vid.vertex_id());
                            return VidIterator::Iter(n);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        VidIterator::None
    }
}

pub enum VidIterator<T: Iterator> {
    Iter(T),
    None,
}

impl<T: Iterator> Iterator for VidIterator<T> {
    type Item = T::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            VidIterator::Iter(iter) => iter.next(),
            VidIterator::None => None,
        }
    }
}
