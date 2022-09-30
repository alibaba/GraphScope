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

use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use ir_common::LabelId;

use crate::apis::graph::PKV;
use crate::apis::{Direction, Edge, QueryParams, Vertex, ID};
use crate::GraphProxyResult;

/// The function for graph query
pub trait Statement<I, O>: Send + 'static {
    fn exec(&self, next: I) -> GraphProxyResult<Box<dyn Iterator<Item = O> + Send>>;
}

impl<I, O, F: 'static> Statement<I, O> for F
where
    F: Fn(I) -> GraphProxyResult<Box<dyn Iterator<Item = O> + Send>> + Send + Sync,
{
    fn exec(&self, param: I) -> GraphProxyResult<Box<dyn Iterator<Item = O> + Send>> {
        (self)(param)
    }
}

pub fn from_fn<I, O, F>(func: F) -> Box<dyn Statement<I, O>>
where
    F: Fn(I) -> GraphProxyResult<Box<dyn Iterator<Item = O> + Send>> + Send + Sync + 'static,
{
    Box::new(func) as Box<dyn Statement<I, O>>
}

/// The interfaces of reading data (vertices, edges and their properties) from a graph.
pub trait ReadGraph: Send + Sync {
    /// Scan all vertices with query parameters, and return an iterator over them.
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    /// Scan a vertex with a specified label and its primary key value(s), and additional query parameters,
    /// and return the vertex if exists.
    fn index_scan_vertex(
        &self, label: LabelId, primary_key: &PKV, params: &QueryParams,
    ) -> GraphProxyResult<Option<Vertex>>;

    /// Scan all edges with query parameters, and return an iterator over them.
    fn scan_edge(&self, params: &QueryParams) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>>;

    /// Get vertices with the given global_ids (defined in runtime) and parameters, and return an iterator over them.
    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>>;

    /// Get edges with the given global_ids (defined in runtime) and parameters, and return an iterator over them.
    fn get_edge(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>>;

    /// Get adjacent vertices of the given direction with parameters, and return the closure of Statement.
    /// We could further call the returned closure with input vertex and get its adjacent vertices.
    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Vertex>>>;

    /// Get adjacent edges of the given direction with parameters, and return the closure of Statement.
    /// We could further call the returned closure with input vertex and get its adjacent edges.
    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Edge>>>;

    /// Get primary key value(s) with the given global_id,
    /// and return the primary key value(s) if exists
    fn get_primary_key(&self, id: &ID) -> GraphProxyResult<Option<PKV>>;
}

lazy_static! {
    /// GRAPH_PROXY is a raw pointer which can be safely shared between threads.
    pub static ref GRAPH_PROXY: AtomicPtr<Arc<dyn ReadGraph >> = AtomicPtr::default();
}

pub fn register_graph(graph: Arc<dyn ReadGraph>) {
    let ptr = Box::into_raw(Box::new(graph));
    GRAPH_PROXY.store(ptr, Ordering::SeqCst);
}

pub fn get_graph() -> Option<Arc<dyn ReadGraph>> {
    let ptr = GRAPH_PROXY.load(Ordering::SeqCst);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { (*ptr).clone() })
    }
}
