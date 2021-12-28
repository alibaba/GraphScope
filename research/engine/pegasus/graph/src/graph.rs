use std::ops::Deref;

use ahash::AHashMap;

use crate::topo::IdTopo;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Direction {
    Out,
    In,
    Both,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TopoMeta {
    meta_id: usize,
    src_type: String,
    dst_type: String,
    edge_type: String,
    kind: Direction,
}

pub struct LabeledTopoGraph<B: Deref<Target = [u64]>> {
    v2e_lables: AHashMap<String, Vec<TopoMeta>>,
    topos: Vec<IdTopo<B>>,
}

impl<B: Deref<Target = [u64]>> LabeledTopoGraph<B> {
    pub fn add_out_topo(&mut self, src_type: String, edge_type: String, dst_type: String, topo: IdTopo<B>) {
        let meta_id = self.topos.len();
        let metas = self
            .v2e_lables
            .entry(src_type.clone())
            .or_insert_with(Vec::new);
        let meta = TopoMeta { meta_id, src_type, dst_type, edge_type, kind: Direction::Out };
        metas.push(meta);
        self.topos.push(topo);
    }

    pub fn add_in_topo(&mut self, src_type: String, edge_type: String, dst_type: String, topo: IdTopo<B>) {
        let meta_id = self.topos.len();
        let metas = self
            .v2e_lables
            .entry(dst_type.clone())
            .or_insert_with(Vec::new);
        let meta = TopoMeta { meta_id, src_type, dst_type, edge_type, kind: Direction::In };
        metas.push(meta);
        self.topos.push(topo);
    }

    pub fn add_nodirct_topo(&mut self, vtype: String, edge_type: String, topo: IdTopo<B>) {
        let meta_id = self.topos.len();
        let metas = self
            .v2e_lables
            .entry(vtype.clone())
            .or_insert_with(Vec::new);
        let meta = TopoMeta {
            meta_id,
            src_type: vtype.clone(),
            dst_type: vtype,
            edge_type,
            kind: Direction::Both,
        };
        metas.push(meta);
        self.topos.push(topo);
    }

    pub fn count(&self, vid: u64, vtype: &str, dir: Direction) -> usize {
        let mut count = 0;
        if let Some(metas) = self.v2e_lables.get(vtype) {
            for m in metas {
                if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                    count += self.topos[m.meta_id].count_neighbors(vid)
                }
            }
        }
        count
    }

    pub fn count_label_of(&self, vid: u64, vtype: &str, edge_type: &str, dir: Direction) -> usize {
        let mut count = 0;
        if let Some(metas) = self.v2e_lables.get(vtype) {
            for m in metas {
                if (m.kind == dir || m.kind == Direction::Both || dir == Direction::Both)
                    && m.edge_type == edge_type
                {
                    count += self.topos[m.meta_id].count_neighbors(vid)
                }
            }
        }
        count
    }

    pub fn count_label_with_in<F: Fn(&str) -> bool>(
        &self, vid: u64, vtype: &str, dir: Direction, within: F,
    ) -> usize {
        let mut count = 0;
        if let Some(metas) = self.v2e_lables.get(vtype) {
            for m in metas {
                if (m.kind == dir || m.kind == Direction::Both || dir == Direction::Both)
                    && within(&m.edge_type)
                {
                    count += self.topos[m.meta_id].count_neighbors(vid)
                }
            }
        }
        count
    }

    pub fn batch_count(&self, vids: &[u64], vtype: &str, dir: Direction) -> usize {
        let mut count = 0;
        if let Some(metas) = self.v2e_lables.get(vtype) {
            for m in metas {
                if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                    for vid in vids {
                        count += self.topos[m.meta_id].count_neighbors(*vid)
                    }
                }
            }
        }
        count
    }

    pub fn get_neighbors(&self, vid: u64, vtype: &str, dir: Direction) -> impl Iterator<Item = u64> {
        let mut result = vec![];
        if let Some(metas) = self.v2e_lables.get(vtype) {
            for m in metas {
                if m.kind == dir || m.kind == Direction::Both || dir == Direction::Both {
                    result.push(self.topos[m.meta_id].get_neighbors(vid));
                }
            }
        }
        result.into_iter().flat_map(|n| n.into_iter())
    }
}
