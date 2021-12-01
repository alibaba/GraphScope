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

use crate::db::api::types::{RocksVertex, RocksEdge};
use crate::db::api::condition::Condition;
use crate::db::api::{VertexId, LabelId, PropertyId, GraphResult, EdgeId, Records, SerialId, SnapshotId, EdgeKind};

/// Snapshot of a graph partition. All the interfaces should be thread-safe
pub trait PartitionSnapshot {
    type V: RocksVertex;
    type E: RocksEdge;

    /// Returns the vertex entity of given `vertex_id`, properties are filtered
    /// by the `property_ids` optionally.
    ///
    /// If `label_id` is [`None`], all vertex labels will be searched and the
    /// first match vertex will be returned.
    fn get_vertex(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::V>>;

    /// Returns the edge entity of given `edge_id`, properties are filtered
    /// by the `property_ids` optionally.
    ///
    /// If `edge_relation` is [`None`], all edge relations will be searched and
    /// the first match edge will be returned.
    fn get_edge(
        &self,
        edge_id: EdgeId,
        edge_relation: Option<&EdgeKind>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>>;


    /// Returns all vertices, filtered by `label_id` and `condition`
    /// optionally.
    ///
    /// Properties of the vertices are filtered by the `property_ids`
    /// optionally.
    fn scan_vertex(
        &self,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::V>>;

    /// Returns all edges, filtered by `label_id` and `condition`
    /// optionally.
    ///
    /// Properties of the edges are filtered by the `property_ids` optionally.
    fn scan_edge(
        &self,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>>;

    /// Returns out edges of vertex `vertex_id`, filtered by `label_id` and
    /// `condition` optionally.
    ///
    /// Properties of the edges are filtered by the `property_ids` optionally.
    fn get_out_edges(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>>;

    /// Returns in edges of vertex `vertex_id`, filtered by `label_id` and
    /// `condition` optionally.
    ///
    /// Properties of the edges are filtered by the `property_ids` optionally.
    fn get_in_edges(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Records<Self::E>>;

    /// Returns the out-degree of vertex `vertex_id` in `label_id`
    fn get_out_degree(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
    ) -> GraphResult<usize>;

    /// Returns the in-degree of vertex `vertex_id` in `label_id`
    fn get_in_degree(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
    ) -> GraphResult<usize>;

    /// Returns the `k`th out edge of vertex `vertex_id` in `edge_relation`.
    ///
    /// Properties of the edge are filtered by the `property_ids` optionally.
    fn get_kth_out_edge(
        &self,
        vertex_id: VertexId,
        edge_relation: &EdgeKind,
        k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>>;

    /// Returns the `k`th in edge of vertex `vertex_id` in `edge_relation`.
    ///
    /// Properties of the edge are filtered by the `property_ids` optionally.
    fn get_kth_in_edge(
        &self,
        vertex_id: VertexId,
        edge_relation: &EdgeKind,
        k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> GraphResult<Option<Self::E>>;

    /// Returns the id of the snapshot
    fn get_snapshot_id(&self) -> SnapshotId;
}