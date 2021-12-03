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

use super::{Vertex, Edge, SnapshotId, VertexId, LabelId, PropId, PartitionId, PartitionVertexIds, PartitionLabeledVertexIds};
use super::prelude::Condition;
use std::sync::Arc;
use super::graph_schema::Schema;

pub trait GlobalGraphQuery: Send + Sync {
    type V: Vertex;
    type E: Edge;
    type VI: Iterator<Item=Self::V>;
    type EI: Iterator<Item=Self::E>;

    fn get_out_vertex_ids(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>>;
    fn get_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>>;
    fn get_in_vertex_ids(&self, si: SnapshotId, dst_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::VI)>>;
    fn get_in_edges(&self, si: SnapshotId, dst_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize) -> Box<dyn Iterator<Item=(VertexId, Self::EI)>>;
    fn count_out_edges(&self, si: SnapshotId, src_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(VertexId, usize)>>;
    fn count_in_edges(&self, si: SnapshotId, dst_ids: Vec<PartitionVertexIds>, edge_labels: &Vec<LabelId>, condition: Option<&Condition>) -> Box<dyn Iterator<Item=(VertexId, usize)>>;

    fn get_vertex_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>, output_prop_ids: Option<&Vec<PropId>>) -> Self::VI;
    fn get_edge_properties(&self, si: SnapshotId, ids: Vec<PartitionLabeledVertexIds>,  output_prop_ids: Option<&Vec<PropId>>) -> Self::EI;

    fn get_all_vertices(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::VI;
    fn get_all_edges(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>,  dedup_prop_ids: Option<&Vec<PropId>>, output_prop_ids: Option<&Vec<PropId>>, limit: usize, partition_ids: &Vec<PartitionId>) -> Self::EI;
    fn count_all_vertices(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, partition_ids: &Vec<PartitionId>) -> u64;
    fn count_all_edges(&self, si: SnapshotId, labels: &Vec<LabelId>, condition: Option<&Condition>, partition_ids: &Vec<PartitionId>) -> u64;

    fn translate_vertex_id(&self, vertex_id: VertexId) -> VertexId;

    fn get_schema(&self, si: SnapshotId) -> Option<Arc<dyn Schema>>;
}
