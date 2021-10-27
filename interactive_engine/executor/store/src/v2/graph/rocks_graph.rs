use std::sync::Arc;
use crate::db::storage::ExternalStorage;
use crate::v2::multi_version_graph::MultiVersionGraph;
use crate::v2::api::{SnapshotId, VertexId, LabelId, PropertyId, EdgeId, EdgeRelation, Condition, Records, SerialId};
use crate::db::graph::types::{VertexTypeManager, EdgeTypeManager};
use crate::v2::GraphResult;
use crate::db::graph::bin::{vertex_table_prefix_key, parse_vertex_key};
use crate::db::graph::codec::get_codec_version;
use std::collections::HashMap;
use crate::v2::errors::GraphError;
use crate::v2::graph::entity::{VertexImpl, EdgeImpl};
use crate::v2::graph::iter::{VertexTypeScan, EdgeTypeScan};

pub struct RocksGraph {
    vertex_manager: VertexTypeManager,
    edge_manager: EdgeTypeManager,
    storage: Arc<dyn ExternalStorage>,
}

impl RocksGraph {

}

impl MultiVersionGraph for RocksGraph {
    type V = VertexImpl;
    type E = EdgeImpl;

    fn get_vertex(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::V>> {
        todo!()
    }

    fn get_edge(&self, snapshot_id: SnapshotId, edge_id: EdgeId, edge_relation: Option<&EdgeRelation>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        todo!()
    }

    fn scan_vertex(&self,
                   snapshot_id: SnapshotId,
                   label_id: Option<LabelId>,
                   condition: Option<&Condition>,
                   property_ids: Option<&Vec<PropertyId>>
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
        if let Some(label_id) = label_id {
            let edge_info = self.edge_manager.get_edge_info(snapshot_id as i64, label_id as i32)?;
            let scan = EdgeTypeScan::new(self.storage.clone(), snapshot_id, edge_info);
            Ok(scan.into_iter())
        } else {
            let mut edge_info_iter = self.edge_manager.get_all_edges(snapshot_id as i64);
            let mut res: Records<Self::E> = Box::new(::std::iter::empty());
            while let Some(info) = edge_info_iter.next_info() {
                let label_iter = EdgeTypeScan::new(self.storage.clone(), snapshot_id, info).into_iter();
                res = Box::new(res.chain(label_iter));
            }
            Ok(res)
        }
    }

    fn get_out_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Records<Self::E>> {
        todo!()
    }

    fn get_in_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Records<Self::E>> {
        todo!()
    }

    fn get_out_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation) -> GraphResult<usize> {
        todo!()
    }

    fn get_in_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation) -> GraphResult<usize> {
        todo!()
    }

    fn get_kth_out_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        todo!()
    }

    fn get_kth_in_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> GraphResult<Option<Self::E>> {
        todo!()
    }
}
