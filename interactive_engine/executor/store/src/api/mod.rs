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

mod condition;
mod property;
mod multi_version;
mod elem;
mod global_query;
pub mod graph_partition;
pub mod prelude;
pub mod graph_schema;

pub use self::elem::*;
pub use self::multi_version::*;
pub use self::global_query::*;
pub use self::condition::*;

use crate::schema::prelude::*;
use std::collections::HashMap;

pub type VertexId = i64;
pub type LabelId = u32;
pub type PartitionId = u32;
pub type EdgeId = i64;
pub type SnapshotId = i64;
pub type SchemaVersion = i32;
pub type PropId = u32;
pub type EdgeIdTuple = (VertexId, i64, VertexId);
pub type EdgeType = (LabelId,LabelId,LabelId);
pub type PartitionVertexIds = (PartitionId,Vec<VertexId>);
pub type PartitionLabeledVertexIds = (PartitionId, Vec<(Option<LabelId>, Vec<VertexId>)>);

pub static MAX_PARTITION_NUM: u32 = 4096;
pub const MAX_SNAPSHOT_ID: SnapshotId = SnapshotId::max_value() - 1;

#[derive(Clone, Debug)]
pub struct PartitionKey {
    pub partition_id: u32,
    pub graph_name: String,
    pub label: String,
}

#[allow(dead_code)]
impl PartitionKey {
    pub fn new(partition_id: u32, graph_name: String, label: String) -> Self {
        PartitionKey {
            partition_id,
            graph_name,
            label,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Default, Clone)]
pub struct TableInfo {
    pub label: LabelId,
    pub src_label: LabelId,
    pub dst_label: LabelId,
    pub data_type: Type,
    pub partition_id: PartitionId,
    pub schema_version: SchemaVersion,
    pub id: i32,
}

impl TableInfo {
    pub fn to_relation(&self) -> Relation {
        Relation::new(self.label, self.src_label, self.dst_label)
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct TypePartition {
    pub label: LabelId,
    pub src_label: LabelId,
    pub dst_label: LabelId,
    pub partition_id: PartitionId,
}

impl TypePartition {
    pub fn vertex(label: LabelId, partition_id: PartitionId) -> Self {
        TypePartition {
            label,
            src_label: 0,
            dst_label: 0,
            partition_id,
        }
    }

    pub fn edge(relation: Relation, partition_id: PartitionId) -> Self {
        TypePartition {
            label: relation.label,
            src_label: relation.src_label,
            dst_label: relation.dst_label,
            partition_id,
        }
    }

    pub fn is_vertex(&self) -> bool {
        self.src_label == 0
    }
}

pub struct VertexData<'a> {
    pub id: VertexId,
    pub props: &'a HashMap<PropId, Vec<u8>>,
}

impl<'a> VertexData<'a> {
    pub fn new(id: VertexId, props: &'a HashMap<PropId, Vec<u8>>) -> Self {
        VertexData {
            id,
            props,
        }
    }
}

pub struct EdgeData<'a> {
    pub src_id: VertexId,
    pub dst_id: VertexId,
    pub edge_id: EdgeId,
    pub props: &'a HashMap<PropId, Vec<u8>>,
}

impl<'a> EdgeData<'a> {
    pub fn new(src_id: VertexId, dst_id: VertexId, edge_id: EdgeId, props: &'a HashMap<PropId, Vec<u8>>) -> Self {
        EdgeData {
            src_id,
            dst_id,
            edge_id,
            props,
        }
    }
}
