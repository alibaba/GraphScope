//
//! Copyright 2021 Alibaba Group Holding Limited.
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

use ir_common::NameOrId;

use crate::apis::graph::PK;
use crate::apis::DynDetails;
use crate::GraphProxyResult;

/// The interfaces of writing data (vertices, edges and their properties) into a graph.
pub trait WriteGraphProxy: Send + Sync {
    /// Add a vertex
    fn add_vertex(
        &mut self, vertex_pk: PK, label: &NameOrId, properties: Option<&DynDetails>,
    ) -> GraphProxyResult<()>;

    /// Add an edge
    fn add_edge(
        &mut self, edge_pk: Option<PK>, label: &NameOrId, src_vertex_pk: PK, src_vertex_label: &NameOrId,
        dst_vertex_pk: PK, dst_vertex_label: &NameOrId, properties: Option<&DynDetails>,
    ) -> GraphProxyResult<()>;

    /// A hint of all vertices/edges are added.
    fn finish(&mut self) -> GraphProxyResult<()>;
}
