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

use crate::v2::multi_version_graph::MultiVersionGraph;
use crate::v2::api::{SnapshotId, VertexId, LabelId, PropertyId, EdgeId, Records, SerialId};
use crate::v2::api::types::{EdgeRelation, Vertex, PropertyReader, PropertyValue, Property, Edge};
use crate::v2::api::condition::Condition;
use crate::v2::Result;
use std::sync::Arc;
use crate::db::storage::ExternalStorage;
use crate::db::graph::types::{VertexTypeManager, EdgeTypeManager};
use crate::db::api::GraphStorage;

pub struct GraphStorageWrapper<G: GraphStorage> {
    storage: Arc<G>,
}

impl<G: GraphStorage> GraphStorageWrapper<G> {
    pub fn new() -> Self {
        unimplemented!()
    }
}

impl<G: GraphStorage> MultiVersionGraph for GraphStorageWrapper<G> {
    type V = WrapperVertex;
    type E = WrapperEdge;

    fn get_vertex(&self,
                  snapshot_id: SnapshotId,
                  vertex_id: VertexId,
                  label_id: Option<LabelId>,
                  property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Option<Self::V>> {
        let snapshot_id = snapshot_id as i64;
        let vertex_id = vertex_id as i64;
        let label_id = label_id.map(|l| l as i32);
        let v = self.storage.get_vertex(snapshot_id, vertex_id, label_id)?;
        Ok(None)
    }

    fn get_edge(&self, snapshot_id: SnapshotId, edge_id: EdgeId, edge_relation: Option<&EdgeRelation>, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::E>> {
        unimplemented!()
    }

    fn scan_vertex(&self, snapshot_id: SnapshotId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::V>> {
        unimplemented!()
    }

    fn scan_edge(&self, snapshot_id: SnapshotId, edge_relation: Option<&EdgeRelation>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::E>> {
        unimplemented!()
    }

    fn get_out_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::E>> {
        unimplemented!()
    }

    fn get_in_edges(&self, snapshot_id: SnapshotId, vertex_id: VertexId, label_id: Option<LabelId>, condition: Option<&Condition>, property_ids: Option<&Vec<PropertyId>>) -> Result<Records<Self::E>> {
        unimplemented!()
    }

    fn get_out_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation) -> Result<usize> {
        unimplemented!()
    }

    fn get_in_degree(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation) -> Result<usize> {
        unimplemented!()
    }

    fn get_kth_out_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::E>> {
        unimplemented!()
    }

    fn get_kth_in_edge(&self, snapshot_id: SnapshotId, vertex_id: VertexId, edge_relation: &EdgeRelation, k: SerialId, property_ids: Option<&Vec<PropertyId>>) -> Result<Option<Self::E>> {
        unimplemented!()
    }
}

pub struct WrapperProperty {

}

impl Property for WrapperProperty {
    fn get_property_id(&self) -> PropertyId {
        unimplemented!()
    }

    fn get_property_value(&self) -> &PropertyValue {
        unimplemented!()
    }
}

pub struct WrapperVertex {

}

impl Vertex for WrapperVertex {
    fn get_vertex_id(&self) -> VertexId {
        unimplemented!()
    }

    fn get_label_id(&self) -> LabelId {
        unimplemented!()
    }
}

impl PropertyReader for WrapperVertex {
    type P = WrapperProperty;
    type PropertyIterator = Box<Iterator<Item=Self::P>>;

    fn get_property_value(&self, property_id: PropertyId) -> Option<&PropertyValue> {
        unimplemented!()
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        unimplemented!()
    }
}

pub struct WrapperEdge {

}

impl Edge for WrapperEdge {
    fn get_edge_id(&self) -> &EdgeId {
        unimplemented!()
    }

    fn get_edge_relation(&self) -> &EdgeRelation {
        unimplemented!()
    }
}

impl PropertyReader for WrapperEdge {
    type P = WrapperProperty;
    type PropertyIterator = Box<Iterator<Item=Self::P>>;

    fn get_property_value(&self, property_id: PropertyId) -> Option<&PropertyValue> {
        unimplemented!()
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        unimplemented!()
    }
}
