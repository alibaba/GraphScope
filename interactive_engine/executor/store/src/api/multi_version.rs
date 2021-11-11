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

use super::{Vertex, Edge, SnapshotId, VertexId, LabelId, EdgeId};
use std::collections::HashMap;
use crate::schema::prelude::*;
use std::sync::Arc;
use super::prelude::*;

pub trait MVGraphQuery: Send + Sync {
    type V: Vertex;
    type E: Edge;
    type VI: Iterator<Item=Self::V>;
    type EI: Iterator<Item=Self::E>;
    // Snapshot-ed methods
    fn get_vertex(&self, si: SnapshotId, id: VertexId, label: Option<LabelId>) -> Option<Self::V>;
    fn get_out_edges(&self, si: SnapshotId, src_id: VertexId, label: Option<LabelId>) -> Self::EI;
    fn get_in_edges(&self, si: SnapshotId, dst_id: VertexId, label: Option<LabelId>) -> Self::EI;
    fn scan(&self, si: SnapshotId, label: Option<LabelId>) -> Self::VI;
    fn scan_edges(&self, si: SnapshotId, label: Option<LabelId>) -> Self::EI;

    fn count_out_edges(&self, si: SnapshotId, src_id: VertexId, label: Option<LabelId>) -> usize;
    fn count_in_edges(&self, si: SnapshotId, dst_id: VertexId, label: Option<LabelId>) -> usize;

    // non-snapshot methods
    fn get_vertex_default(&self, id: VertexId, label: Option<LabelId>) -> Option<Self::V> {
        self.get_vertex(SnapshotId::max_value() - 1, id, label)
    }
    fn get_out_edges_default(&self, src_id: VertexId, label: Option<LabelId>) -> Self::EI {
        self.get_out_edges(SnapshotId::max_value() - 1, src_id, label)
    }
    fn get_in_edges_default(&self, dst_id: VertexId, label: Option<LabelId>) -> Self::EI {
        self.get_in_edges(SnapshotId::max_value() - 1, dst_id, label)
    }
    fn scan_default(&self, label: Option<LabelId>) -> Self::VI {
        self.scan(SnapshotId::max_value() - 1, label)
    }

    fn edge_count(&self) -> u64;
    fn vertex_count(&self) -> u64;

    fn estimate_vertex_count(&self, label: Option<LabelId>) -> u64;
    fn estimate_edge_count(&self, label: Option<LabelId>) -> u64;
}

pub trait GraphUpdate: Send + Sync {
    fn insert_vertex(&self,
                     si: SnapshotId,
                     label: LabelId,
                     schema_version: i32,
                     id: VertexId,
                     data: &HashMap<i32, Vec<u8>>);

    fn replace_vertex(&self, si: SnapshotId, label: LabelId, schema_version: i32, id: VertexId, data: &HashMap<i32, Vec<u8>>);

    fn insert_edge(&self,
                   si: SnapshotId,
                   relation: Relation,
                   schema_version: i32,
                   edge_id: EdgeId,
                   src_id: VertexId,
                   dst_id: VertexId,
                   data: &HashMap<i32, Vec<u8>>);

    fn replace_edge(&self, si: SnapshotId, relation: Relation, schema_version: i32, edge_id: EdgeId,
                    src_id: VertexId, dst_id: VertexId, data: &HashMap<i32, Vec<u8>>);

    fn update_vertex(&self,
                     si: SnapshotId,
                     label: LabelId,
                     schema_version: i32,
                     id: VertexId,
                     data: &HashMap<i32, Vec<u8>>);

    fn update_edge(&self, si: SnapshotId,
                   relation: Relation,
                   schema_version: i32,
                   edge_id: EdgeId,
                   src_id: VertexId,
                   dst_id: VertexId,
                   data: &HashMap<i32, Vec<u8>>);

    fn delete_vertex(&self, si: SnapshotId, label: LabelId, id: VertexId);

    fn delete_edge(&self, si: SnapshotId, relation: Relation, edge_id: EdgeId, src_id: VertexId, dst_id: VertexId);

    fn offline(&self, si: SnapshotId);

    fn online(&self, si: SnapshotId);
}

pub trait GraphLoader: Send + Sync {
    fn load_table(&self, schema_version: SchemaVersion, table: TableInfo, source_data_dir: String) -> Result<(), String>;
    fn online_table(&self, si: SnapshotId, table: TableInfo) -> bool;
}


pub trait DDL {
    fn update_schema(&self, si: SnapshotId, schema_version: SchemaVersion, schema: Arc<dyn Schema>);
    fn drop_vertex_type(&self, si: SnapshotId, label: LabelId);
    fn drop_edge_type(&self, si: SnapshotId, label: LabelId);
    fn drop_relation_ship(&self, si: SnapshotId, relation: Relation);
}

pub trait MVGraph: MVGraphQuery + GraphUpdate + GraphLoader + DDL {

    fn get_schema(&self, si: SnapshotId) -> Option<Arc<dyn Schema>>;
    fn get_schema_by_version(&self, version: SchemaVersion) -> Option<Arc<dyn Schema>>;
    fn get_partition_id(&self, src_id: VertexId) -> PartitionId;

    /// get active partition ids
    fn get_partitions(&self) -> Vec<PartitionId>;
    fn active_partitions(&self, partitions: Vec<PartitionId>);
    fn get_serving_types(&self, si: SnapshotId) -> Vec<TypePartition>;
    fn get_partition(&self, partition_id: PartitionId) -> Option<Arc<dyn MVGraphQuery<V=Self::V, E=Self::E, VI=Self::VI, EI=Self::EI>>>;
    fn get_dimension_partition(&self) -> Arc<dyn MVGraphQuery<V=Self::V, E=Self::E, VI=Self::VI, EI=Self::EI>>;
    fn clear(&self);
    fn get_cur_serving_snapshot(&self) -> SnapshotId;
}
