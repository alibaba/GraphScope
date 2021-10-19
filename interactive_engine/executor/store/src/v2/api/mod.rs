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

use crate::v2::Result;

pub mod partition_snapshot;
pub mod types;
pub mod condition;
pub mod partition_graph;

pub use self::partition_snapshot::*;
pub use self::types::*;
pub use self::condition::*;

pub type SnapshotId = u64;
pub type LabelId = u32;
pub type PropertyId = u32;
pub type VertexId = u64;
pub type EdgeInnerId = u64;
pub type SerialId = u32;

pub type Records<T> = Box<dyn Iterator<Item=Result<T>> + Send>;

#[repr(C)]
#[derive(Clone)]
pub struct EdgeId {
    edge_inner_id: EdgeInnerId,
    src_vertex_id: VertexId,
    dst_vertex_id: VertexId,
}

impl EdgeId {
    pub fn new(edge_inner_id: EdgeInnerId, src_vertex_id: VertexId, dst_vertex_id: VertexId) -> Self {
        EdgeId {
            edge_inner_id,
            src_vertex_id,
            dst_vertex_id,
        }
    }

    pub fn get_src_vertex_id(&self) -> VertexId {
        self.src_vertex_id
    }

    pub fn get_dst_vertex_id(&self) -> VertexId {
        self.dst_vertex_id
    }

    pub fn get_edge_inner_id(&self) -> EdgeInnerId {
        self.edge_inner_id
    }
}



