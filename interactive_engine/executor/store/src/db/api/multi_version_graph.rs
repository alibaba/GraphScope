//
//! Copyright 2020 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use crate::db::api::condition::Condition;
use crate::db::api::{SnapshotId, VertexId, LabelId, GraphResult, EdgeId, EdgeKind, Records, SerialId, TypeDef, PropertyMap, DataLoadTarget, PropertyId, BackupId};
use crate::db::api::types::{RocksVertex, RocksEdge};

pub trait MultiVersionGraph {
    type V: RocksVertex;
    type E: RocksEdge;

    fn get_vertex(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::V>>;

    fn get_edge(
        &self,
        snapshot_id: SnapshotId,
        edge_id: EdgeId,
        edge_relation: Option<&EdgeKind>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>>;

    fn scan_vertex(
        &self,
        snapshot_id: SnapshotId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::V>>;

    fn scan_edge(
        &self,
        snapshot_id: SnapshotId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>>;

    fn get_out_edges(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>>;

    fn get_in_edges(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>>;

    fn get_out_degree(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
    ) -> GraphResult<usize>;

    fn get_in_degree(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
    ) -> GraphResult<usize>;

    fn get_kth_out_edge(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        edge_relation: &EdgeKind,
        k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>>;

    fn get_kth_in_edge(
        &self,
        snapshot_id: SnapshotId,
        vertex_id: VertexId,
        edge_relation: &EdgeKind,
        k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>>;

    /// Create a new vertex type with `label` and `type_def` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If vertex type already exists, `si` is smaller than last operation, get lock error, storage error
    /// or other errors, `GraphError` will be returned.
    /// Returns true if schema_version changed, false otherwise.
    fn create_vertex_type(&self, si: SnapshotId, schema_version: i64, label: LabelId, type_def: &TypeDef, table_id: i64) -> GraphResult<bool>;

    /// Create a new edge type with `label` and `type_def` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If edge type already exists, `si` is smaller than last operation, get lock error, storage error
    /// or other errors, `GraphError` will be returned.
    /// Returns true if schema_version changed, false otherwise.
    fn create_edge_type(&self, si: SnapshotId, schema_version: i64, label: LabelId, type_def: &TypeDef) -> GraphResult<bool>;

    /// Add a new edge kind of `kind` to edge type with `kind.label` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If edge type with `kind.label` not exists, edge kind `kind` already exists, `si` is smaller
    /// than last operation, get lock error, storage error or other errors, `GraphError` will be returned.
    /// Returns true if schema_version changed, false otherwise.
    fn add_edge_kind(&self, si: SnapshotId, schema_version: i64, kind: &EdgeKind, table_id: i64) -> GraphResult<bool>;

    /// Drop a vertex type of `label` at `si` and `schema_version`.  This interface is thread safe.
    ///
    /// If storage error, `si` is smaller than last operation, get lock error or other errors,
    /// `GraphError` will be returned.
    ///
    /// Returns true if schema_version changed, false otherwise.
    fn drop_vertex_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId) -> GraphResult<bool>;

    /// Drop an edge type of `label` as well as all edge kinds of `label` at `si` and `schema_version`.  This interface is thread safe.
    ///
    /// If storage error, `si` is smaller than last operation, get lock error or other errors,
    /// `GraphError` will be returned.
    ///
    /// Returns true if schema_version changed, false otherwise.
    fn drop_edge_type(&self, si: SnapshotId, schema_version: i64, label_id: LabelId) -> GraphResult<bool>;

    /// Remove an edge type of `edge_kind` at `si` and `schema_version`. This interface is thread safe.
    ///
    /// If storage error, `si` is smaller than last operation, get lock error or other errors,
    /// `GraphError` will be returned.
    ///
    /// Returns true if schema_version changed, false otherwise.
    fn remove_edge_kind(&self, si: SnapshotId, schema_version: i64, edge_kind: &EdgeKind) -> GraphResult<bool>;

    /// realtime write interfaces
    /// These realtime write interfaces should be thread safe and user should ensure all data are in
    /// ascending order by si, or error will be return. The distribute protocol ensure si of all data
    /// sending to store will be monotonically increasing, so it's easy to meet the constrain and if not,
    /// there must be some problems in protocol or local realtime write logic.

    /// Insert a vertex with `id`, `label` and `properties` at `si`. It'll overwrite the vertex with
    /// `id` no matter whether it exits. This interface is thread safe.
    ///
    /// If vertex type of `label` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_overwrite_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Insert or update a vertex with `id` and `label` at `si`. If the vertex already exists, merge its
    /// properties with the provided `properties` and create a new version of this vertex. Otherwise
    /// insert a new vertex with `id`, `label` and `properties`. This interface is thread safe.
    ///
    /// If vertex type of `label` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_update_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Delete a vertex with `id` and `label` at `si`. The existence will not be checked. This interface is thread safe.
    ///
    /// If vertex type of `label` not found, storage error or other errors, `GraphError` will be returned.
    fn delete_vertex(&self, si: SnapshotId, id: VertexId, label: LabelId) -> GraphResult<()>;

    /// Insert an edge with `id`, `edge_kind`, `forward` and `properties` at `si`. It'll overwrite the edge with
    /// `id` no matter whether it exits. This interface is thread safe.
    ///
    /// If edge kind of `edge_kind` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_overwrite_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Insert or update an edge with `id` and `edge_kind` at `si`. If the edge already exists, merge its
    /// properties with the provided `properties` and create a new version of this edge. Otherwise
    /// insert a new edge with `id`, `edge_kind` and `properties`. This interface is thread safe.
    ///
    /// If edge kind of `edge_kind` not found, storage error, encoding error, meta error or other errors,
    /// `GraphError` will be returned.
    fn insert_update_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, forward: bool, properties: &dyn PropertyMap) -> GraphResult<()>;

    /// Delete an edge with `id` and `edge_kind` at `si`. The existence will not be checked. This interface is thread safe.
    ///
    /// If edge kind of `edge_kind` not found, storage error or other errors, `GraphError` will be returned.
    fn delete_edge(&self, si: SnapshotId, id: EdgeId, edge_kind: &EdgeKind, forward: bool) -> GraphResult<()>;

    /// garbage collection at `si`
    fn gc(&self, si: SnapshotId);

    /// Returns current GraphDefPb bytes
    fn get_graph_def_blob(&self) -> GraphResult<Vec<u8>>;

    /// prepare data load
    fn prepare_data_load(&self, si: SnapshotId, schema_version: i64, target: &DataLoadTarget, table_id: i64) -> GraphResult<bool>;

    fn commit_data_load(&self, si: SnapshotId, schema_version: i64, target: &DataLoadTarget, table_id: i64) -> GraphResult<bool>;

    /// Open a backup engine of graph storage that implements GraphBackup trait.
    fn open_backup_engine(&self, backup_path: &str) -> GraphResult<Box<dyn GraphBackup>>;
}

pub trait GraphBackup {
    /// Create a new backup of graph store. This interface is thread safe.
    ///
    /// Returns the new created backup id if successful, `GraphError` otherwise.
    fn create_new_backup(&mut self) -> GraphResult<BackupId>;

    /// Delete a backup of `backup_id`. This interface is thread safe.
    ///
    /// If `backup_id` is not available, something error when deleting or other errors,
    /// `GraphError` will be returned.
    fn delete_backup(&mut self, backup_id: BackupId) -> GraphResult<()>;

    /// Restore the graph store from `backup_id` at `restore_path`. This interface is thread safe.
    ///
    /// If `restore_path` is not available，`backup_id` is not available, something error when
    /// restoring or other errors, `GraphError` will be returned.
    fn restore_from_backup(&mut self, restore_path: &str, backup_id: BackupId) -> GraphResult<()>;

    /// Verify the backup of `backup_id`. This interface is thread safe.
    ///
    /// If `backup_id` is not available, backup files are broken, backup checksum mismatch or
    /// other errors, `GraphError` will be returned.
    fn verify_backup(&self, backup_id: BackupId) -> GraphResult<()>;

    /// Get the current available backup id list. This interface is thread safe.
    ///
    /// Returns the available backup id vector(may be empty)。
    fn get_backup_list(&self) -> Vec<BackupId>;
}
