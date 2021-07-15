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

use crate::v2::api::{VertexId, LabelId, PropertyId, EdgeId, Records, SerialId, SnapshotId};
use crate::v2::api::types::{Vertex, Edge, EdgeRelation};
use crate::v2::api::condition::Condition;
use crate::v2::Result;

pub trait PartitionSnapshot: Send + Sync {
    type V: Vertex;
    type E: Edge;

    fn get_vertex(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Option<Self::V>>;

    fn get_edge(
        &self,
        edge_id: EdgeId,
        edge_relation: Option<&EdgeRelation>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Option<Self::E>>;

    fn scan_vertex(
        &self,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Records<Self::V>>;

    fn scan_edge(
        &self,
        edge_relation: Option<&EdgeRelation>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Records<Self::E>>;

    fn get_out_edges(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Records<Self::E>>;

    fn get_in_edges(
        &self,
        vertex_id: VertexId,
        label_id: Option<LabelId>,
        condition: Option<&Condition>,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Records<Self::E>>;

    fn get_out_degree(
        &self,
        vertex_id: VertexId,
        edge_relation: &EdgeRelation,
    ) -> Result<usize>;

    fn get_in_degree(
        &self,
        vertex_id: VertexId,
        edge_relation: &EdgeRelation,
    ) -> Result<usize>;

    fn get_kth_out_edge(
        &self,
        vertex_id: VertexId,
        edge_relation: &EdgeRelation,
        k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Option<Self::E>>;

    fn get_kth_in_edge(
        &self,
        vertex_id: VertexId,
        edge_relation: &EdgeRelation,
        k: SerialId,
        property_ids: Option<&Vec<PropertyId>>,
    ) -> Result<Option<Self::E>>;

    fn get_snapshot_id(&self) -> SnapshotId;
}