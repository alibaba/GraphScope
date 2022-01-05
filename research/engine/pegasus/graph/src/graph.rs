use std::ops::Deref;

use ahash::AHashMap;
use nohash_hasher::IntMap;
use pegasus_common::codec::{Encode, WriteExt, Decode, ReadExt};

use crate::topo::IdTopo;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Direction {
    Out,
    In,
    Both,
}

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub struct Vid(u32, u64);

impl Vid {
    #[inline]
    pub fn label_id(&self) -> u32 {
        self.0
    }

    #[inline]
    pub fn vertex_id(&self) -> u64 {
        self.1
    }
}

impl Encode for Vid {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        (self.0, self.1).write_to(writer)
    }
}

impl Decode for Vid {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let (label, id) = <(u32, u64)>::read_from(reader)?;
        Ok(Vid(label, id))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TopoMeta {
    meta_id: usize,
    src_type_id: u32,
    dst_type_id: u32,
    edge_type_id: u32,
    kind: Direction,
}


pub struct LabeledTopoGraph<B: Deref<Target = [u64]>> {
    labels: Vec<String>,
    label_id_map: AHashMap<String, u32>,
    v2e_metas: IntMap<u32, Vec<TopoMeta>>,
    topos: Vec<IdTopo<B>>,
}

impl<B: Deref<Target = [u64]>> LabeledTopoGraph<B> {
    fn get_label_id(&mut self, label: &str) -> u32 {
        if let Some(id) = self.label_id_map.get(label) {
            return *id;
        } else {
            let next_id = self.labels.len();
            self.labels.push(label.to_owned());
            self.label_id_map.insert(label.to_owned(), next_id as u32);
            next_id as u32
        }
    }

    pub fn add_out_topo(&mut self, src_type: String, edge_type: String, dst_type: String, topo: IdTopo<B>) {
        let src_type_id = self.get_label_id(&src_type);
        let edge_type_id = self.get_label_id(&edge_type);
        let dst_type_id = self.get_label_id(&dst_type);
        let meta_id = self.topos.len();
        let metas = self
            .v2e_metas
            .entry(src_type_id)
            .or_insert_with(Vec::new);
        let meta = TopoMeta { meta_id, src_type_id, dst_type_id, edge_type_id, kind: Direction::Out };
        metas.push(meta);
        self.topos.push(topo);
    }

    pub fn add_in_topo(&mut self, src_type: String, edge_type: String, dst_type: String, topo: IdTopo<B>) {
        let src_type_id = self.get_label_id(&src_type);
        let edge_type_id = self.get_label_id(&edge_type);
        let dst_type_id = self.get_label_id(&dst_type);
        let meta_id = self.topos.len();
        let metas = self
            .v2e_metas
            .entry(dst_type_id)
            .or_insert_with(Vec::new);
        let meta = TopoMeta { meta_id, src_type_id, dst_type_id, edge_type_id, kind: Direction::In };
        metas.push(meta);
        self.topos.push(topo);
    }

    pub fn add_nodirct_topo(&mut self, vtype: String, edge_type: String, topo: IdTopo<B>) {
        let src_type_id = self.get_label_id(&vtype);
        let edge_type_id = self.get_label_id(&edge_type);
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
    }

    #[inline]
    pub fn check_label_id(&self, label: &str) -> Option<u32> {
        self.label_id_map.get(label).copied()
    }

    pub fn count_all_edge(&self, vid: u64, vtype: &str, dir: Direction) -> usize {
        if let Some(id) = self.label_id_map.get(vtype).copied() {
            self.count_all_edge_(Vid(id, vid), dir)
        } else {
            0
        }
    }

    pub fn count_all_edge_(&self, vid: Vid, dir: Direction) -> usize {
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

    pub fn get_all_neighbors(&self, vid: u64, vtype: &str, dir: Direction) -> impl Iterator<Item = Vid> {
        if let Some(label_id) = self.label_id_map.get(vtype).copied() {
            self.get_all_neighbors_(Vid(label_id, vid), dir)
        } else {
            panic!("label of {} not found", vtype)
        }
    }

    pub fn get_all_neighbors_(&self, vid: Vid, dir: Direction) -> impl Iterator<Item = Vid> {
        let mut result = vec![];
        if let Some(metas) = self.v2e_metas.get(&vid.label_id()) {
            for m in metas {
                if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                    let label_id = m.dst_type_id;
                    let neighbors = self.topos[m.meta_id].get_neighbors(vid.vertex_id()).map(move |id| Vid(label_id, id));
                    result.push(neighbors);
                }
            }
        }
        result.into_iter().flat_map(|n| n.into_iter())
    }

    pub fn get_neighbors_by(&self, vid: u64, vtype: &str, edge_label: &str, dir: Direction) -> impl Iterator<Item = Vid> {
        if let Some(label_id) = self.label_id_map.get(vtype) {
            VidIterator::Iter(self.get_neighbors_by_(Vid(*label_id, vid), edge_label, dir))
        } else {
            VidIterator::None
        }
    }

    pub fn get_neighbors_by_(&self, vid: Vid, edge_label: &str, dir: Direction) -> impl Iterator<Item = Vid> {
        if let Some(label_id) = self.label_id_map.get(edge_label) {
            if let Some(meta) = self.v2e_metas.get(&vid.label_id()) {
                for m in meta {
                    if m.edge_type_id == *label_id  {
                        if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                            let n = self.topos[m.meta_id].get_neighbors(vid.vertex_id());
                            let lid = m.dst_type_id;
                            return VidIterator::Iter(n.map(move |id| Vid(lid, id)));
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

pub enum VidIterator<T: Iterator<Item = Vid>> {
    Iter(T),
    None
}

impl<T: Iterator<Item = Vid>> Iterator for VidIterator<T> {
    type Item = Vid;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            VidIterator::Iter(iter) => iter.next(),
            VidIterator::None => None
        }
    }
}
