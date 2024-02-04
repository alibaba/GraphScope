use std::sync::Arc;

use crate::db::api::{EdgeDirection, EdgeId, GraphResult, Records, SnapshotId, VertexId};
use crate::db::graph::bin::{
    edge_prefix, edge_table_prefix_key, parse_edge_key, parse_vertex_key, vertex_table_prefix_key,
};
use crate::db::graph::codec::get_codec_version;
use crate::db::graph::entity::{RocksEdgeImpl, RocksVertexImpl};
use crate::db::graph::types::{EdgeInfo, EdgeKindInfo, VertexTypeInfo};
use crate::db::storage::rocksdb::RocksDB;

pub struct VertexTypeScan {
    storage: Arc<RocksDB>,
    si: SnapshotId,
    vertex_type_info: Arc<VertexTypeInfo>,
    with_prop: bool,
}

fn check_v(id: VertexId, ts: SnapshotId, prev_id: Option<VertexId>, data_ts: SnapshotId) -> bool {
    data_ts >= ts
        && match prev_id {
            Some(prev_id) => id != prev_id,
            None => true,
        }
}
fn check_e(id: EdgeId, ts: SnapshotId, prev_id: Option<EdgeId>, data_ts: SnapshotId) -> bool {
    data_ts >= ts
        && match prev_id {
            Some(prev_id) => id != prev_id,
            None => true,
        }
}

impl VertexTypeScan {
    pub fn new(
        storage: Arc<RocksDB>, si: SnapshotId, vertex_type_info: Arc<VertexTypeInfo>,
        with_prop: bool,
    ) -> Self {
        VertexTypeScan { storage, si, vertex_type_info, with_prop }
    }
}

impl IntoIterator for VertexTypeScan {
    type Item = GraphResult<RocksVertexImpl>;
    type IntoIter = Records<RocksVertexImpl>;

    fn into_iter(self) -> Self::IntoIter {
        let si = self.si as i64;
        if let Some(table) = self.vertex_type_info.get_table(si) {
            let label = self.vertex_type_info.get_label();
            let prefix = vertex_table_prefix_key(table.id);
            let data_ts = si - table.start_si;
            let mut previous_vertex = None;
            let iter = self.storage.new_scan(&prefix).unwrap();
            let iter = iter.filter_map(move |(raw_key, raw_val)| {
                let key = raw_key.to_slice();
                let val = raw_val.to_slice();
                match parse_vertex_key(key) {
                    Ok((vertex_id, ts)) => {
                        if !check_v(vertex_id, ts, previous_vertex, data_ts) {
                            return None;
                        }
                        previous_vertex = Some(vertex_id);
                        if val.len() < 4 {
                            return None;
                        }
                        if self.with_prop {
                            match self
                                .vertex_type_info
                                .get_decoder(si, get_codec_version(val))
                            {
                                Ok(decoder) => {
                                    Some(Ok(RocksVertexImpl::new(vertex_id, label, Some(decoder), raw_val)))
                                }
                                Err(e) => Some(Err(e.into())),
                            }
                        } else {
                            Some(Ok(RocksVertexImpl::new(vertex_id, label, None, raw_val)))
                        }
                    }
                    Err(e) => Some(Err(e.into())),
                }
            });
            return Box::new(iter);
        }
        return Box::new(::std::iter::empty());
    }
}

pub struct EdgeTypeScan {
    storage: Arc<RocksDB>,
    si: SnapshotId,
    edge_info: Arc<EdgeInfo>,
    vertex_id: Option<VertexId>,
    direction: EdgeDirection,
    with_prop: bool,
}

impl EdgeTypeScan {
    pub fn new(
        storage: Arc<RocksDB>, si: SnapshotId, edge_info: Arc<EdgeInfo>,
        vertex_id: Option<VertexId>, direction: EdgeDirection, with_prop: bool,
    ) -> Self {
        EdgeTypeScan { storage, si, edge_info, vertex_id, direction, with_prop }
    }
}

impl IntoIterator for EdgeTypeScan {
    type Item = GraphResult<RocksEdgeImpl>;
    type IntoIter = Records<RocksEdgeImpl>;

    fn into_iter(self) -> Self::IntoIter {
        let edge_kinds = self.edge_info.lock();
        let mut edge_kind_iter = edge_kinds.iter_kinds().filter_map(|edge_kind| {
            if edge_kind.is_alive_at(self.si) {
                Some(edge_kind.clone())
            } else {
                None
            }
        });
        let mut res: Records<RocksEdgeImpl> = Box::new(::std::iter::empty());
        while let Some(edge_kind) = edge_kind_iter.next() {
            let iter = EdgeKindScan::new(
                self.storage.clone(),
                self.si,
                edge_kind.clone(),
                self.vertex_id,
                self.direction,
                self.with_prop,
            )
            .into_iter();
            res = Box::new(res.chain(iter));
        }
        res
    }
}

pub struct EdgeKindScan {
    storage: Arc<RocksDB>,
    si: SnapshotId,
    edge_kind_info: Arc<EdgeKindInfo>,
    vertex_id: Option<VertexId>,
    direction: EdgeDirection,
    with_prop: bool,
}

impl EdgeKindScan {
    pub fn new(
        storage: Arc<RocksDB>, si: SnapshotId, edge_kind_info: Arc<EdgeKindInfo>,
        vertex_id: Option<VertexId>, direction: EdgeDirection, with_prop: bool,
    ) -> Self {
        EdgeKindScan { storage, si, edge_kind_info, vertex_id, direction, with_prop }
    }
}

impl IntoIterator for EdgeKindScan {
    type Item = GraphResult<RocksEdgeImpl>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        let si = self.si as i64;
        if let Some(table) = self.edge_kind_info.get_table(si) {
            let data_ts = si - table.start_si;
            let scan_iter = match self.direction {
                EdgeDirection::In | EdgeDirection::Out => {
                    let prefix = edge_prefix(table.id, self.vertex_id.unwrap() as i64, self.direction);
                    self.storage.new_scan(&prefix)
                }
                EdgeDirection::Both => {
                    let prefix = edge_table_prefix_key(table.id, EdgeDirection::Out);
                    self.storage.new_scan(&prefix)
                }
            };
            let mut prev_id = None;
            let e_iter = scan_iter
                .unwrap()
                .filter_map(move |(raw_key, raw_val)| {
                    let key = raw_key.to_slice();
                    let val = raw_val.to_slice();
                    let (edge_id, ts) = parse_edge_key(key);
                    if !check_e(edge_id, ts, prev_id, data_ts) {
                        return None;
                    }
                    prev_id = Some(edge_id);
                    if val.len() < 4 {
                        return None;
                    }
                    if self.with_prop {
                        let codec_version = get_codec_version(val);
                        match self
                            .edge_kind_info
                            .get_decoder(si, codec_version)
                        {
                            Ok(decoder) => {
                                let edge_kind = self.edge_kind_info.get_type();
                                Some(Ok(RocksEdgeImpl::new(
                                    edge_id,
                                    edge_kind.into(),
                                    Some(decoder),
                                    raw_val,
                                )))
                            }
                            Err(e) => Some(Err(e.into())),
                        }
                    } else {
                        let edge_kind = self.edge_kind_info.get_type();
                        Some(Ok(RocksEdgeImpl::new(edge_id, edge_kind.into(), None, raw_val)))
                    }
                });
            return Box::new(e_iter);
        }
        return Box::new(::std::iter::empty());
    }
}
