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

use crate::apis::{Edge, Vertex};
use crate::GraphProxyResult;

/// The interfaces of writing data (vertices, edges and their properties) into a graph.
pub trait WriteGraphProxy: Send + Sync {
    /// Add a vertex
    fn add_vertex(&mut self, vertex: Vertex) -> GraphProxyResult<()>;

    /// Add a batch of vertices
    fn add_vertices(&mut self, vertices: Vec<Vertex>) -> GraphProxyResult<()>;

    /// Add an edge
    fn add_edge(&mut self, edge: Edge) -> GraphProxyResult<()>;

    /// Add a batch of edges
    fn add_edges(&mut self, edges: Vec<Edge>) -> GraphProxyResult<()>;

    /// A hint of all vertices/edges are added.
    fn finish(&mut self) -> GraphProxyResult<()>;
}
