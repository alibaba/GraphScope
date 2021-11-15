use std::sync::Arc;
use crate::db::storage::{ExternalStorage, RawBytes};
use crate::v2::multi_version_graph::MultiVersionGraph;
use crate::v2::api::{SnapshotId, VertexId, LabelId, PropertyId, EdgeId, EdgeRelation, Condition, Records, SerialId};
use crate::db::graph::types::{VertexTypeManager, EdgeTypeManager};
use crate::v2::GraphResult;
use crate::db::graph::bin::{vertex_key, edge_key};
use crate::db::graph::codec::get_codec_version;
use crate::v2::graph::entity::{RocksVertex, RocksEdge};
use crate::v2::graph::iter::{VertexTypeScan, EdgeTypeScan};
use crate::db::api::EdgeDirection;

pub struct RocksGraph {
    vertex_manager: VertexTypeManager,
    edge_manager: EdgeTypeManager,
    storage: Arc<dyn ExternalStorage>,
}

impl RocksGraph {
    #[allow(dead_code)]
    pub fn new(vertex_manager: VertexTypeManager, edge_manager: EdgeTypeManager, storage: Arc<dyn ExternalStorage>) -> Self {
        RocksGraph { vertex_manager, edge_manager, storage }
    }

    fn get_vertex_from_label(&self,
                             snapshot_id: SnapshotId,
                             vertex_id: VertexId,
                             label_id: LabelId,
                             _property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<RocksVertex>> {
        let snapshot_id = snapshot_id as i64;
        let vertex_type_info = self.vertex_manager.get_type_info(snapshot_id, label_id as i32)?;
        if let Some(table) = vertex_type_info.get_table(snapshot_id) {
            let key = vertex_key(table.id, vertex_id as i64, snapshot_id - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..16] == key[0..16] && v.len() > 4 {
                    let codec_version = get_codec_version(v);
                    let decoder = vertex_type_info.get_decoder(snapshot_id, codec_version)?;
                    let vertex = RocksVertex::new(vertex_id, vertex_type_info.get_label() as LabelId, decoder, RawBytes::new(v));
                    return Ok(Some(vertex));
                }
            }
        }
        Ok(None)
    }

    fn get_edge_from_relation(&self,
                              snapshot_id: SnapshotId,
                              edge_id: EdgeId,
                              edge_relation: &EdgeRelation,
                              _property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<RocksEdge>> {
        let snapshot_id = snapshot_id as i64;
        let info = self.edge_manager.get_edge_kind(snapshot_id, &edge_relation.into())?;
        if let Some(table) = info.get_table(snapshot_id) {
            let key = edge_key(table.id, edge_id.into(), EdgeDirection::Out, snapshot_id - table.start_si);
            let mut iter = self.storage.scan_from(&key)?;
            if let Some((k, v)) = iter.next() {
                if k[0..32] == key[0..32] && v.len() >= 4 {
                    let codec_version = get_codec_version(v);
                    let decoder = info.get_decoder(snapshot_id, codec_version)?;
                    let edge = RocksEdge::new(edge_id, info.get_type().into(), decoder, RawBytes::new(v));
                    return Ok(Some(edge));
                }
            }
        }
        Ok(None)
    }

    fn query_edges(&self,
                   snapshot_id: SnapshotId,
                   vertex_id: Option<VertexId>,
                   direction: EdgeDirection,
                   label_id: Option<LabelId>,
                   _condition: Option<&Condition>,
                   _property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<RocksEdge>> {
        if let Some(label_id) = label_id {
            let edge_info = self.edge_manager.get_edge_info(snapshot_id as i64, label_id as i32)?;
            let scan = EdgeTypeScan::new(self.storage.clone(), snapshot_id, edge_info, vertex_id, direction);
            Ok(scan.into_iter())
        } else {
            let mut edge_info_iter = self.edge_manager.get_all_edges(snapshot_id as i64);
            let mut res: Records<RocksEdge> = Box::new(::std::iter::empty());
            while let Some(info) = edge_info_iter.next_info() {
                let label_iter = EdgeTypeScan::new(self.storage.clone(), snapshot_id, info, vertex_id, direction).into_iter();
                res = Box::new(res.chain(label_iter));
            }
            Ok(res)
        }
    }
}

impl MultiVersionGraph for RocksGraph {
    type V = RocksVertex;
    type E = RocksEdge;

    fn get_vertex(&self,
                  snapshot_id: SnapshotId,
                  vertex_id: VertexId,
                  label_id: Option<LabelId>,
                  property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::V>> {
        if let Some(label_id) = label_id {
            self.get_vertex_from_label(snapshot_id, vertex_id, label_id, property_ids)
        } else {
            let mut iter = self.vertex_manager.get_all(snapshot_id as i64);
            while let Some(info) = iter.next() {
                if let Some(vertex) = self.get_vertex_from_label(snapshot_id, vertex_id, info.get_label() as LabelId, property_ids)? {
                    return Ok(Some(vertex));
                }
            }
            Ok(None)
        }
    }

    fn get_edge(&self,
                snapshot_id: SnapshotId,
                edge_id: EdgeId,
                edge_relation: Option<&EdgeRelation>,
                property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::E>> {
        if let Some(relation) = edge_relation {
            self.get_edge_from_relation(snapshot_id, edge_id, relation, property_ids)
        } else {
            let mut iter = self.edge_manager.get_all_edges(snapshot_id as i64);
            while let Some(info) = iter.next() {
                let mut edge_kind_iter = info.into_iter();
                while let Some(edge_kind_info) = edge_kind_iter.next() {
                    if let Some(edge) = self.get_edge_from_relation(snapshot_id, edge_id, &edge_kind_info.get_type().into(), property_ids)? {
                        return Ok(Some(edge));
                    }
                }
            }
            Ok(None)
        }
    }

    fn scan_vertex(&self,
                   snapshot_id: SnapshotId,
                   label_id: Option<LabelId>,
                   _condition: Option<&Condition>,
                   _property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::V>> {
        if let Some(label_id) = label_id {
            let vertex_type_info = self.vertex_manager.get_type_info(snapshot_id as i64, label_id as i32)?;
            let scan = VertexTypeScan::new(self.storage.clone(), snapshot_id, vertex_type_info);
            Ok(scan.into_iter())
        } else {
            let mut vertex_type_info_iter = self.vertex_manager.get_all(snapshot_id as i64);
            let mut res: Records<Self::V> = Box::new(::std::iter::empty());
            while let Some(info) = vertex_type_info_iter.next_info() {
                let label_iter = VertexTypeScan::new(self.storage.clone(), snapshot_id, info).into_iter();
                res = Box::new(res.chain(label_iter));
            }
            Ok(res)
        }
    }

    fn scan_edge(&self,
                 snapshot_id: SnapshotId,
                 label_id: Option<LabelId>,
                 condition: Option<&Condition>,
                 property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::E>> {
        self.query_edges(snapshot_id, None, EdgeDirection::Both, label_id, condition, property_ids)
    }

    fn get_out_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Records<Self::E>> {
        self.query_edges(snapshot_id, Some(vertex_id), EdgeDirection::Out, label_id, condition, property_ids)
    }

    fn get_in_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Records<Self::E>> {
        self.query_edges(snapshot_id, Some(vertex_id), EdgeDirection::In, label_id, condition, property_ids)
    }

    fn get_out_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation) -> GraphResult<usize> {
        let edges_iter = self.get_out_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                            Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_in_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation) -> GraphResult<usize> {
        let edges_iter = self.get_in_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                           Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_kth_out_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        let mut edges_iter = self.get_out_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                                property_ids)?;
        edges_iter.nth(k as usize).transpose()
    }

    fn get_kth_in_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        let mut edges_iter = self.get_in_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                               property_ids)?;
        edges_iter.nth(k as usize).transpose()
    }
}
