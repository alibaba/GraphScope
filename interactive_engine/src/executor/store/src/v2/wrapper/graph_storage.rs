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
use crate::db::api::{GraphStorage, EdgeKind};
use std::collections::HashMap;

pub struct GraphStorageWrapper<G: GraphStorage> {
    storage: Arc<G>,
}

impl<G: GraphStorage> GraphStorageWrapper<G> {
    pub fn new(storage: Arc<G>) -> Self {
        GraphStorageWrapper {
            storage,
        }
    }

    fn parse_snapshot_id(snapshot_id: SnapshotId) -> crate::db::api::SnapshotId {
        snapshot_id as i64
    }

    fn parse_label_id(label_id: Option<LabelId>) -> Option<crate::db::api::LabelId> {
        label_id.map(|l| l as i32)
    }

    fn parse_vertex_id(vertex_id: VertexId) -> crate::db::api::VertexId {
        vertex_id as i64
    }

    fn parse_vertex<V: crate::db::api::Vertex>(from_vertex: V, property_ids: Option<&Vec<PropertyId>>)
        -> WrapperVertex {
        unimplemented!()
    }

    fn parse_edge_id(from_edge_id: EdgeId) -> crate::db::api::EdgeId {
        let src_vertex_id = from_edge_id.get_src_vertex_id();
        let dst_vertex_id = from_edge_id.get_dst_vertex_id();
        let edge_inner_id = from_edge_id.get_edge_inner_id();
        crate::db::api::EdgeId::new(src_vertex_id as i64, dst_vertex_id as i64, edge_inner_id as i64)
    }

    fn parse_edge_kind(relation: &EdgeRelation) -> EdgeKind {
        let edge_label_id = relation.get_edge_label_id();
        let src_v_label_id = relation.get_src_vertex_label_id();
        let dst_v_label_id = relation.get_dst_vertex_label_id();
        EdgeKind::new(edge_label_id as i32, src_v_label_id as i32, dst_v_label_id as i32)
    }

    fn parse_edge<E: crate::db::api::Edge>(from_edge: E, property_ids: Option<&Vec<PropertyId>>) -> WrapperEdge {
        unimplemented!()
    }

    fn parse_condition(condition: Option<&Condition>) -> Option<Arc<crate::db::api::Condition>> {
        match condition {
            None => {
                None
            }
            Some(_) => {
                unimplemented!()
            }
        }
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
        let snapshot_id = Self::parse_snapshot_id(snapshot_id);
        let vertex_id = Self::parse_vertex_id(vertex_id);
        let label_id = Self::parse_label_id(label_id);
        let raw_v = self.storage.get_vertex(snapshot_id, vertex_id, label_id)?;
        Ok(raw_v.map(|inner| Self::parse_vertex(inner, property_ids)))
    }

    fn get_edge(&self,
                snapshot_id: SnapshotId,
                edge_id: EdgeId,
                edge_relation: Option<&EdgeRelation>,
                property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Option<Self::E>> {
        let snapshot_id = Self::parse_snapshot_id(snapshot_id);
        let edge_id = Self::parse_edge_id(edge_id);
        let edge_kind = edge_relation.map(|relation| Self::parse_edge_kind(relation));
        let raw_e = self.storage.get_edge(snapshot_id, edge_id, edge_kind.as_ref())?;
        Ok(raw_e.map(|inner| Self::parse_edge(inner, property_ids)))
    }

    fn scan_vertex(&self,
                   snapshot_id: SnapshotId,
                   label_id: Option<LabelId>,
                   condition: Option<&Condition>,
                   property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Records<Self::V>> {
        let snapshot_id = Self::parse_snapshot_id(snapshot_id);
        let label_id = Self::parse_label_id(label_id);
        let condition = Self::parse_condition(condition);
        let mut raw_iter = self.storage.query_vertices(snapshot_id, label_id, condition)?;
        let mut res = vec![];
        while let Some(v) = raw_iter.next() {
            res.push(Ok(Self::parse_vertex(v, property_ids)));
        }
        Ok(Box::new(res.into_iter()))
    }

    fn scan_edge(&self,
                 snapshot_id: SnapshotId,
                 edge_relation: Option<&EdgeRelation>,
                 condition: Option<&Condition>,
                 property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Records<Self::E>> {
        let snapshot_id = Self::parse_snapshot_id(snapshot_id);
        let edge_kind = edge_relation.map(|relation| Self::parse_edge_kind(relation));
        let condition = Self::parse_condition(condition);
        let mut raw_iter = self.storage.query_edges(snapshot_id, edge_kind.map(|k| k.edge_label_id), condition)?;
        let mut res = vec![];
        while let Some(e) = raw_iter.next() {
            res.push(Ok(Self::parse_edge(e, property_ids)));
        }
        Ok(Box::new(res.into_iter()))
    }

    fn get_out_edges(&self,
                     snapshot_id: SnapshotId,
                     vertex_id: VertexId,
                     label_id: Option<LabelId>,
                     condition: Option<&Condition>,
                     property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Records<Self::E>> {
        let snapshot_id = Self::parse_snapshot_id(snapshot_id);
        let vertex_id = Self::parse_vertex_id(vertex_id);
        let label_id = Self::parse_label_id(label_id);
        let condition = Self::parse_condition(condition);
        let mut raw_iter = self.storage.get_out_edges(snapshot_id, vertex_id, label_id, condition)?;
        let mut res = vec![];
        while let Some(e) = raw_iter.next() {
            res.push(Ok(Self::parse_edge(e, property_ids)));
        }
        Ok(Box::new(res.into_iter()))
    }

    fn get_in_edges(&self,
                    snapshot_id: SnapshotId,
                    vertex_id: VertexId,
                    label_id: Option<LabelId>,
                    condition: Option<&Condition>,
                    property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Records<Self::E>> {
        let snapshot_id = Self::parse_snapshot_id(snapshot_id);
        let vertex_id = Self::parse_vertex_id(vertex_id);
        let label_id = Self::parse_label_id(label_id);
        let condition = Self::parse_condition(condition);
        let mut raw_iter = self.storage.get_in_edges(snapshot_id, vertex_id, label_id, condition)?;
        let mut res = vec![];
        while let Some(e) = raw_iter.next() {
            res.push(Ok(Self::parse_edge(e, property_ids)));
        }
        Ok(Box::new(res.into_iter()))
    }

    fn get_out_degree(&self,
                      snapshot_id: SnapshotId,
                      vertex_id: VertexId,
                      edge_relation: &EdgeRelation
    ) -> Result<usize> {
        let edges_iter = self.get_out_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                            Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_in_degree(&self,
                     snapshot_id: SnapshotId,
                     vertex_id: VertexId,
                     edge_relation: &EdgeRelation
    ) -> Result<usize> {
        let edges_iter = self.get_in_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                            Some(vec![]).as_ref())?;
        Ok(edges_iter.count())
    }

    fn get_kth_out_edge(&self,
                        snapshot_id: SnapshotId,
                        vertex_id: VertexId,
                        edge_relation: &EdgeRelation,
                        k: SerialId,
                        property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Option<Self::E>> {
        let mut edges_iter = self.get_out_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                                property_ids)?;
        edges_iter.nth(k).transpose()
    }

    fn get_kth_in_edge(&self,
                       snapshot_id: SnapshotId,
                       vertex_id: VertexId,
                       edge_relation: &EdgeRelation,
                       k: SerialId,
                       property_ids: Option<&Vec<PropertyId>>
    ) -> Result<Option<Self::E>> {
        let mut edges_iter = self.get_in_edges(snapshot_id, vertex_id, Some(edge_relation.get_edge_label_id()), None,
                                                property_ids)?;
        edges_iter.nth(k).transpose()
    }
}

#[derive(Clone)]
pub struct WrapperProperty {
    property_id: PropertyId,
    property_value: PropertyValue,
}

impl Property for WrapperProperty {
    fn get_property_id(&self) -> PropertyId {
        self.property_id
    }

    fn get_property_value(&self) -> &PropertyValue {
        &self.property_value
    }
}

#[derive(Clone)]
pub struct WrapperVertex {
    vertex_id: VertexId,
    label_id: LabelId,
    properties: HashMap<PropertyId, WrapperProperty>,
}

impl WrapperVertex {
    pub fn new(vertex_id: VertexId, label_id: LabelId, properties: HashMap<PropertyId, WrapperProperty>) -> Self {
        WrapperVertex {
            vertex_id,
            label_id,
            properties,
        }
    }
}

impl Vertex for WrapperVertex {
    fn get_vertex_id(&self) -> VertexId {
        self.vertex_id
    }

    fn get_label_id(&self) -> LabelId {
        self.label_id
    }
}

impl PropertyReader for WrapperVertex {
    type P = WrapperProperty;
    type PropertyIterator = Box<Iterator<Item=Self::P>>;

    fn get_property_value(&self, property_id: PropertyId) -> Option<&PropertyValue> {
        self.properties.get(&property_id).map(|p| p.get_property_value())
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        Box::new(self.properties.clone().into_iter().map(|(id, p)| p).collect::<Vec<WrapperProperty>>().into_iter())
    }
}

pub struct WrapperEdge {
    edge_id: EdgeId,
    edge_relation: EdgeRelation,
    properties: HashMap<PropertyId, WrapperProperty>,
}

impl Edge for WrapperEdge {
    fn get_edge_id(&self) -> &EdgeId {
        &self.edge_id
    }

    fn get_edge_relation(&self) -> &EdgeRelation {
        &self.edge_relation
    }
}

impl PropertyReader for WrapperEdge {
    type P = WrapperProperty;
    type PropertyIterator = Box<Iterator<Item=Self::P>>;

    fn get_property_value(&self, property_id: PropertyId) -> Option<&PropertyValue> {
        self.properties.get(&property_id).map(|p| p.get_property_value())
    }

    fn get_property_iterator(&self) -> Self::PropertyIterator {
        Box::new(self.properties.clone().into_iter().map(|(id, p)| p).collect::<Vec<WrapperProperty>>().into_iter())
    }
}
