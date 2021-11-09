use crate::db::storage::ExternalStorage;
use std::sync::Arc;
use crate::db::graph::types::{VertexTypeInfo, EdgeInfo, EdgeKindInfo};
use crate::v2::GraphResult;
use crate::v2::graph::entity::{VertexImpl, EdgeImpl};
use crate::v2::api::{SnapshotId, VertexId, LabelId, Records, EdgeRelation};
use crate::db::graph::bin::{vertex_table_prefix_key, parse_vertex_key, edge_table_prefix_key, parse_edge_key, edge_prefix};
use crate::db::graph::codec::{get_codec_version};
use crate::db::api::EdgeDirection;

pub struct VertexTypeScan {
    storage: Arc<dyn ExternalStorage>,
    snapshot_id: SnapshotId,
    vertex_type_info: Arc<VertexTypeInfo>,
}

impl VertexTypeScan {
    pub fn new(storage: Arc<dyn ExternalStorage>,
               snapshot_id: SnapshotId,
               vertex_type_info: Arc<VertexTypeInfo>,
    ) -> Self {
        VertexTypeScan {
            storage,
            snapshot_id,
            vertex_type_info,
        }
    }
}

impl IntoIterator for VertexTypeScan {
    type Item = GraphResult<VertexImpl>;
    type IntoIter = Records<VertexImpl>;

    fn into_iter(self) -> Self::IntoIter {
        let snapshot_id = self.snapshot_id as i64;
        if let Some(table) = self.vertex_type_info.get_table(snapshot_id) {
            let prefix = vertex_table_prefix_key(table.id);
            let data_ts = snapshot_id - table.start_si;
            let mut previous_vertex = None;
            let v_iter = self.storage.new_scan(&prefix).unwrap().filter_map(move |(raw_key, raw_val)| {
                let key = unsafe { raw_key.to_slice() };
                let val = unsafe { raw_val.to_slice() };
                match parse_vertex_key(key) {
                    Ok((vertex_id, ts)) => {
                        if data_ts < ts || match previous_vertex {
                            None => false,
                            Some(prev_v) => prev_v == vertex_id,
                        } {
                            return None;
                        }
                        previous_vertex = Some(vertex_id);
                        if val.len() < 4 {
                            return None;
                        }
                        let codec_version = get_codec_version(val);
                        match self.vertex_type_info.get_decoder(snapshot_id, codec_version) {
                            Ok(decoder) => {
                                let label = self.vertex_type_info.get_label();
                                Some(Ok(VertexImpl::new(vertex_id as VertexId, label as LabelId, decoder, raw_val)))
                            }
                            Err(e) => {
                                Some(Err(e.into()))
                            }
                        }
                    }
                    Err(e) => {
                        Some(Err(e.into()))
                    }
                }
            });
            return Box::new(v_iter);
        }
        return Box::new(::std::iter::empty());
    }
}

pub struct EdgeTypeScan {
    storage: Arc<dyn ExternalStorage>,
    snapshot_id: SnapshotId,
    edge_info: Arc<EdgeInfo>,
    vertex_id: Option<VertexId>,
    direction: EdgeDirection,
}

impl EdgeTypeScan {
    pub fn new(storage: Arc<dyn ExternalStorage>,
               snapshot_id: SnapshotId,
               edge_info: Arc<EdgeInfo>,
               vertex_id: Option<VertexId>,
               direction: EdgeDirection
    ) -> Self {
            EdgeTypeScan { storage, snapshot_id, edge_info, vertex_id, direction }
    }
}

impl IntoIterator for EdgeTypeScan {
    type Item = GraphResult<EdgeImpl>;
    type IntoIter = Records<EdgeImpl>;

    fn into_iter(self) -> Self::IntoIter {
        let mut edge_kind_iter = self.edge_info.get_kinds(self.snapshot_id as i64);
        let mut res: Records<EdgeImpl> = Box::new(::std::iter::empty());
        while let Some(edge_kind) = edge_kind_iter.next() {
            let iter = EdgeKindScan::new(self.storage.clone(), self.snapshot_id, edge_kind.clone(), self.vertex_id, self.direction).into_iter();
            res = Box::new(res.chain(iter));
        }
        res
    }
}

pub struct EdgeKindScan {
    storage: Arc<dyn ExternalStorage>,
    snapshot_id: SnapshotId,
    edge_kind_info: Arc<EdgeKindInfo>,
    vertex_id: Option<VertexId>,
    direction: EdgeDirection,
}

impl EdgeKindScan {
    pub fn new(storage: Arc<dyn ExternalStorage>,
               snapshot_id: SnapshotId,
               edge_kind_info: Arc<EdgeKindInfo>,
               vertex_id: Option<VertexId>,
               direction: EdgeDirection
    ) -> Self {
        EdgeKindScan { storage, snapshot_id, edge_kind_info, vertex_id, direction }
    }
}

impl IntoIterator for EdgeKindScan {
    type Item = GraphResult<EdgeImpl>;
    type IntoIter = Box<dyn Iterator<Item=Self::Item> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        let snapshot_id = self.snapshot_id as i64;
        if let Some(table) = self.edge_kind_info.get_table(snapshot_id) {
            let data_ts = snapshot_id - table.start_si;
            let scan_iter = match self.direction {
                EdgeDirection::In | EdgeDirection::Out => {
                    let prefix = edge_prefix(table.id, self.vertex_id.unwrap() as i64, self.direction);
                    self.storage.new_scan(&prefix)
                },
                EdgeDirection::Both => {
                    let prefix = edge_table_prefix_key(table.id, EdgeDirection::Out);
                    self.storage.new_scan(&prefix)
                },
            };
            let mut previous_edge = None;
            let e_iter = scan_iter.unwrap().filter_map(move |(raw_key, raw_val)| {
                let key = unsafe { raw_key.to_slice() };
                let val = unsafe { raw_val.to_slice() };
                let (edge_id, ts) = parse_edge_key(key);
                if data_ts < ts || match previous_edge {
                    None => false,
                    Some(prev_e) => prev_e == edge_id,
                } {
                    return None;
                }
                previous_edge = Some(edge_id);
                if val.len() < 4 {
                    return None;
                }
                let codec_version = get_codec_version(val);
                match self.edge_kind_info.get_decoder(snapshot_id, codec_version) {
                    Ok(decoder) => {
                        let edge_kind = self.edge_kind_info.get_type();
                        let edge_relation = EdgeRelation::new(edge_kind.edge_label_id as LabelId,
                                                              edge_kind.src_vertex_label_id as LabelId,
                                                              edge_kind.dst_vertex_label_id as LabelId);
                        Some(Ok(EdgeImpl::new(edge_id.into(), edge_relation, decoder, raw_val)))
                    }
                    Err(e) => {
                        Some(Err(e.into()))
                    }
                }
            });
            return Box::new(e_iter);
        }
        return Box::new(::std::iter::empty());
    }
}
