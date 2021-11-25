// Copyright 2020 Alibaba Group Holding Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::db::api::partition_graph::PartitionGraph;
use std::sync::Arc;
use crate::db::api::multi_version_graph::MultiVersionGraph;
use crate::db::api::{SnapshotId, VertexId, LabelId, PropertyId, GraphResult, EdgeId, EdgeKind, Records, SerialId};
use crate::db::api::partition_snapshot::PartitionSnapshot;
use crate::db::api::condition::Condition;

pub struct WrapperPartitionGraph<G: MultiVersionGraph> {
    multi_version_graph: Arc<G>,
}

impl<G: MultiVersionGraph> WrapperPartitionGraph<G> {
    pub fn new(multi_version_graph: Arc<G>) -> Self {
        WrapperPartitionGraph {
            multi_version_graph,
        }
    }
}

impl<G: MultiVersionGraph> PartitionGraph for WrapperPartitionGraph<G> {
    type S = WrapperPartitionSnapshot<G>;

    fn get_snapshot(&self, snapshot_id: SnapshotId) -> Self::S {
        let multi_version_graph = self.multi_version_graph.clone();
        WrapperPartitionSnapshot {
            multi_version_graph,
            snapshot_id,
        }
    }
}

pub struct WrapperPartitionSnapshot<G: MultiVersionGraph> {
    multi_version_graph: Arc<G>,
    snapshot_id: SnapshotId,
}

impl<G: MultiVersionGraph> PartitionSnapshot for WrapperPartitionSnapshot<G> {
    type V = G::V;
    type E = G::E;

    fn get_vertex(&self,
                  vertex_id: VertexId,
                  label_id: Option<LabelId>,
                  property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::V>> {
        self.multi_version_graph.get_vertex(
            self.snapshot_id,
            vertex_id,
            label_id,
            property_ids
        )
    }

    fn get_edge(&self,
                edge_id: EdgeId,
                edge_relation: Option<&EdgeKind>,
                property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::E>> {
        self.multi_version_graph.get_edge(
            self.snapshot_id,
            edge_id,
            edge_relation,
            property_ids
        )
    }

    fn scan_vertex(&self,
                   label_id: Option<LabelId>,
                   condition: Option<&Condition>,
                   property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::V>> {
        self.multi_version_graph.scan_vertex(
            self.snapshot_id,
            label_id,
            condition,
            property_ids
        )
    }

    fn scan_edge(&self,
                 label_id: Option<LabelId>,
                 condition: Option<&Condition>,
                 property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::E>> {
        self.multi_version_graph.scan_edge(
            self.snapshot_id,
            label_id,
            condition,
            property_ids
        )
    }

    fn get_out_edges(&self,
                     vertex_id: VertexId,
                     label_id: Option<LabelId>,
                     condition: Option<&Condition>,
                     property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::E>> {
        self.multi_version_graph.get_out_edges(
            self.snapshot_id,
            vertex_id,
            label_id,
            condition,
            property_ids
        )
    }

    fn get_in_edges(&self,
                    vertex_id: VertexId,
                    label_id: Option<LabelId>,
                    condition: Option<&Condition>,
                    property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Records<Self::E>> {
        self.multi_version_graph.get_in_edges(
            self.snapshot_id,
            vertex_id,
            label_id,
            condition,
            property_ids
        )
    }

    fn get_out_degree(&self,
                      vertex_id: VertexId,
                      label_id: Option<LabelId>,
    ) -> GraphResult<usize> {
        self.multi_version_graph.get_out_degree(
            self.snapshot_id,
            vertex_id,
            label_id,
        )
    }

    fn get_in_degree(&self,
                     vertex_id: VertexId,
                     label_id: Option<LabelId>,
    ) -> GraphResult<usize> {
        self.multi_version_graph.get_in_degree(
            self.snapshot_id,
            vertex_id,
            label_id,
        )
    }

    fn get_kth_out_edge(&self,
                        vertex_id: VertexId,
                        edge_relation: &EdgeKind,
                        k: SerialId,
                        property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::E>> {
        self.multi_version_graph.get_kth_out_edge(
            self.snapshot_id,
            vertex_id,
            edge_relation,
            k,
            property_ids
        )
    }

    fn get_kth_in_edge(&self,
                       vertex_id: VertexId,
                       edge_relation: &EdgeKind,
                       k: SerialId,
                       property_ids: Option<&Vec<PropertyId>>
    ) -> GraphResult<Option<Self::E>> {
        self.multi_version_graph.get_kth_in_edge(
            self.snapshot_id,
            vertex_id,
            edge_relation,
            k,
            property_ids
        )
    }

    fn get_snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }
}
