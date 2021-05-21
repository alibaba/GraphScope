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

use super::graph_db::*;
use crate::common::*;
use crate::config::{
    DIR_BINARY_DATA, FILE_EDGE_PPT_DATA, FILE_GRAPH_STRUCT, FILE_INDEX_DATA, FILE_NODE_PPT_DATA,
};
use crate::error::{GDBError, GDBResult};
use crate::io::export;
use crate::schema::{LDBCGraphSchema, Schema};
use crate::table::*;
use crate::utils::{Iter, IterList};
use petgraph::graph::{edge_index, EdgeReference, IndexType};
use petgraph::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::sync::Arc;

/// To record the indexing data of this partition of graph. Each vertex has both a globally
/// unique identifier, as well as a local id (index) generated while adding this vertex to the
/// graph database. This structure maintains the mapping of:
///     global id <-> internal index
///     label id -> all vertices' global ids that have the given label
#[derive(Serialize, Deserialize)]
pub struct IndexData<G: Send + Sync + IndexType, I: Send + Sync + IndexType> {
    /// A mapping from global vertex id to internal vertex index.
    global_id_to_index: HashMap<G, NodeIndex<I>>,
    /// Group the internal indices of the vertices by their labels
    label_indices: Vec<Vec<NodeIndex<I>>>,
    /// A mapping from global vertex id to corner internal vertex index. The corner vertexs
    /// are the vertexs that do not belong to current partition, but included by edges.
    corner_global_id_to_index: HashMap<G, NodeIndex<I>>,
    /// A mapping from internal vertex index to global vertex id (including corner vertex)
    index_to_global_id: Vec<G>,
}

impl<G, I> IndexData<G, I>
where
    G: Send + Sync + IndexType,
    I: Send + Sync + IndexType,
{
    pub fn new(num_labels: usize) -> Self {
        let mut label_indices = Vec::with_capacity(num_labels);
        for _ in 0..num_labels {
            label_indices.push(Vec::new())
        }
        Self {
            global_id_to_index: HashMap::new(),
            label_indices,
            corner_global_id_to_index: HashMap::new(),
            index_to_global_id: Vec::new(),
        }
    }

    /// Add a vertex of given global_id, label_id, and internal_id.
    ///
    /// If the vertex already presents, update the value and return `false`,
    /// otherwise insert the value and return `true`
    fn add_vertex(
        &mut self, global_id: G, label: Label, internal_id: NodeIndex<I>, is_corner: bool,
    ) -> bool {
        let existed = if !is_corner {
            let max_label_id = if label[1] != INVALID_LABEL_ID {
                std::cmp::max(label[0], label[1])
            } else {
                label[0]
            } as usize;
            // only a non-corner vertex will be inserted in the labelled vectors.
            while max_label_id >= self.label_indices.len() {
                self.label_indices.push(vec![]);
            }

            self.label_indices[label[0] as usize].push(internal_id);
            if label[1] != INVALID_LABEL_ID {
                self.label_indices[label[1] as usize].push(internal_id);
            }

            self.global_id_to_index.insert(global_id, internal_id).is_some()
        } else {
            self.corner_global_id_to_index.insert(global_id, internal_id).is_some()
        };

        while internal_id.index() >= self.index_to_global_id.len() {
            self.index_to_global_id.push(G::default());
        }
        self.index_to_global_id[internal_id.index()] = global_id;

        !existed
    }

    /// Get internal id from a given global id for both a local vertex and a corner vertex.
    /// Return `None` if the vertex does not present.
    fn get_internal_id(&self, global_id: G) -> Option<NodeIndex<I>> {
        let local_id = self.global_id_to_index.get(&global_id);
        if local_id.is_some() {
            local_id.cloned()
        } else {
            self.corner_global_id_to_index.get(&global_id).cloned()
        }
    }

    /// Get all internal ids from a given label
    fn get_indices_of_label(&self, label_id: LabelId) -> Iter<NodeIndex<I>> {
        if let Some(label_indices) = self.label_indices.get(label_id as usize) {
            Iter::from_iter(label_indices.iter().cloned())
        } else {
            Iter::from_iter(vec![].into_iter())
        }
    }

    /// Get global id from a given internal id
    fn get_global_id(&self, internal_id: NodeIndex<I>) -> Option<G> {
        self.index_to_global_id.get(internal_id.index()).cloned()
    }

    fn shrink_to_fit(&mut self) {
        self.global_id_to_index.shrink_to_fit();
        for indices in &mut self.label_indices {
            indices.shrink_to_fit();
        }
        self.corner_global_id_to_index.shrink_to_fit();
        self.index_to_global_id.shrink_to_fit();
    }
}

/// This is a large-scale, distributed property graph storage.
/// Each vertex will be assigned a global unique id as GID, and each edge, which is directed,
/// will be identified as (startGID, endGID). In the distributed context, a vertex will be
/// hash-partitioned across the data workers based on its GID (by default GID % # workers).
/// An edge (startGID, endGID) will be sent to both the partition of startGID and endGID,
/// and the edge's data will be placed on *BOTH* partitions (if different) accordingly.
/// Note that when putting an into the partition of startGID, endGID may not belong to this
/// partition. In this case, we will call endGID a corner vertex of this partition.
///
/// `LargeGraphDB` maintains:
/// * The local graph data structure, which local vertex and edge id on each machine.
/// * The properties of vertex and edge that is indexed via the local ids.
/// * The bidirectional mapping from vertex's local id to global unique identity.
///
/// The local graph data structure is maintained using the `petgraph::Graph` library, while
/// the properties are maintain through the `PropertyTableTrait`, which is an abstraction
/// of many forms of storage, including an in-memory hashmap-based storage `PropertyTable`,
/// a `SingleValueTable` designed specifically for LDBC's edge data (which has at most one
/// property of `u64` type), and a `RocksDB`-based storage `RocksTable`. See `graph_partition.rs`
/// for how to partition the raw graph data (preprocessed as csv format) over a cluster of
/// workers and maintain a partition in each worker.
///
pub struct LargeGraphDB<
    G: Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
    N: PropertyTableTrait = PropertyTable,
    E: PropertyTableTrait = SingleValueTable,
> {
    /// Which partition of this part of data
    pub(crate) partition: usize,
    /// The graph structure, the label will be encoded as `LabelId`
    pub(crate) graph: DiGraph<Label, LabelId, I>,
    /// The schema of the vertex/edge property table
    pub(crate) graph_schema: Arc<LDBCGraphSchema>,
    /// Table from internal vertexs' indices to their properties
    pub(crate) vertex_prop_table: N,
    /// Table from internal edges' indices to their properties
    pub(crate) edge_prop_table: E,
    /// The index data that maintains the mapping between vertices' global ids and their internal ids
    pub(crate) index_data: IndexData<G, I>,
}

impl<G, I, N, E> LargeGraphDB<G, I, N, E>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
    N: PropertyTableTrait + Sync,
    E: PropertyTableTrait + Sync,
{
    // Below are some private helper functions
    fn index_to_local_vertex(
        &self, index: NodeIndex<I>, with_property: bool,
    ) -> Option<LocalVertex<G>> {
        if let Some(global_id) = self.index_data.get_global_id(index) {
            let label = self.graph.node_weight(index).cloned().unwrap();
            if with_property {
                Some(LocalVertex::with_property(
                    global_id,
                    label,
                    RowWithSchema::new(
                        self.get_all_vertex_property(&index),
                        self.graph_schema.get_vertex_schema(label[0]),
                    ),
                ))
            } else {
                Some(LocalVertex::new(global_id, label))
            }
        } else {
            None
        }
    }

    fn edge_ref_to_local_edge(
        &self, edge: EdgeReference<LabelId, I>, from_start: bool,
    ) -> Option<LocalEdge<G, I>> {
        let src_global_id = self.index_data.get_global_id(edge.source());
        let dst_global_id = self.index_data.get_global_id(edge.target());

        if src_global_id.is_some() && dst_global_id.is_some() {
            let edge_id = edge.id();
            let label = *edge.weight();
            let mut local_edge =
                LocalEdge::new(src_global_id.unwrap(), dst_global_id.unwrap(), label, edge.id());
            local_edge = local_edge.with_from_start(from_start);
            if let Some(properties) = self.get_all_edge_property(&edge_id) {
                local_edge = local_edge.with_properties(RowWithSchema::new(
                    Some(properties),
                    self.graph_schema.get_edge_schema(label),
                ));
            }
            Some(local_edge)
        } else {
            None
        }
    }

    /// Verify if a vertex of given `index` is local to this partition
    fn _is_vertex_local(&self, index: NodeIndex<I>) -> bool {
        if let Some(gid) = self.index_data.get_global_id(index) {
            self.index_data.global_id_to_index.contains_key(&gid)
        } else {
            false
        }
    }

    /// Get all the vertices regarding to the edges (with direction `dir`) of the given vertex `src_id`.
    /// Return an iterator. If the given vertex does not present or it contains no outgoing
    /// edges, an empty iterator is returned.
    fn _get_adj_vertices(
        &self, src_id: G, edge_label: Option<LabelId>, dir: Direction,
    ) -> Iter<LocalVertex<G>> {
        let index = self.index_data.get_internal_id(src_id);
        if index.is_some() {
            Iter::from_iter(
                self.graph
                    .edges_directed(index.unwrap(), dir)
                    .filter(move |edge| {
                        if edge_label.is_some() {
                            self.graph.edge_weight(edge.id()) == edge_label.as_ref()
                        } else {
                            true
                        }
                    })
                    .map(move |edge| {
                        if dir == Direction::Outgoing {
                            self.index_to_local_vertex(edge.target(), false).unwrap()
                        } else {
                            self.index_to_local_vertex(edge.source(), false).unwrap()
                        }
                    }),
            )
        } else {
            Iter::from_iter(vec![].into_iter())
        }
    }

    /// Analogous to `Self::_get_adj_vertices`, but accept multiple edge labels.
    /// Note that an empty `edge_labels` with return empty results. To obtain all adjacent vertices,
    /// call `Self::_get_adj_vertices()` with `None` label instead.
    fn _get_adj_vertices_of_labels(
        &self, src_id: G, edge_labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<LocalVertex<G>> {
        let index = self.index_data.get_internal_id(src_id);
        if index.is_some() {
            Iter::from_iter(
                self.graph
                    .edges_directed(index.unwrap(), dir)
                    .filter(move |edge| {
                        edge_labels.contains(self.graph.edge_weight(edge.id()).unwrap())
                    })
                    .map(move |edge| {
                        if dir == Direction::Outgoing {
                            self.index_to_local_vertex(edge.target(), false).unwrap()
                        } else {
                            self.index_to_local_vertex(edge.source(), false).unwrap()
                        }
                    }),
            )
        } else {
            Iter::from_iter(vec![].into_iter())
        }
    }

    fn _get_adj_edges(
        &self, src_id: G, edge_label: Option<LabelId>, dir: Direction,
    ) -> Iter<LocalEdge<G, I>> {
        let index = self.index_data.get_internal_id(src_id);
        if index.is_some() {
            Iter::from_iter(
                self.graph
                    .edges_directed(index.unwrap(), dir)
                    .filter(move |edge| {
                        if edge_label.is_some() {
                            self.graph.edge_weight(edge.id()) == edge_label.as_ref()
                        } else {
                            true
                        }
                    })
                    .map(move |edge| {
                        if dir == Direction::Outgoing {
                            self.edge_ref_to_local_edge(edge, true).unwrap()
                        } else {
                            self.edge_ref_to_local_edge(edge, false).unwrap()
                        }
                    }),
            )
        } else {
            Iter::from_iter(vec![].into_iter())
        }
    }

    fn _get_adj_edges_of_labels(
        &self, src_id: G, edge_labels: Vec<LabelId>, dir: Direction,
    ) -> Iter<LocalEdge<G, I>> {
        let index = self.index_data.get_internal_id(src_id);
        if index.is_some() {
            Iter::from_iter(
                self.graph
                    .edges_directed(index.unwrap(), dir)
                    .filter(move |edge| {
                        edge_labels.contains(self.graph.edge_weight(edge.id()).unwrap())
                    })
                    .map(move |edge| {
                        if dir == Direction::Outgoing {
                            self.edge_ref_to_local_edge(edge, true).unwrap()
                        } else {
                            self.edge_ref_to_local_edge(edge, false).unwrap()
                        }
                    }),
            )
        } else {
            Iter::from_iter(vec![].into_iter())
        }
    }

    fn _get_all_vertices(&self, _label: Option<LabelId>) -> Iter<LocalVertex<G>> {
        if let Some(label) = _label {
            let iter = self
                .index_data
                .get_indices_of_label(label as LabelId)
                .map(move |internal_id| self.index_to_local_vertex(internal_id, true).unwrap());
            Iter::from_iter(iter)
        } else {
            // return all vertices
            let iter = self
                .graph
                .node_indices()
                .filter(move |internal_id| self._is_vertex_local(*internal_id))
                .map(move |internal_id| self.index_to_local_vertex(internal_id, true).unwrap());

            Iter::from_iter(iter)
        }
    }

    fn _get_all_vertices_of_labels(&self, labels: Vec<LabelId>) -> Iter<LocalVertex<G>> {
        let mut result_iter = vec![];
        for label in labels.into_iter() {
            let result_iter_of_label = self
                .index_data
                .get_indices_of_label(label as LabelId)
                .map(move |internal_id| self.index_to_local_vertex(internal_id, true).unwrap());
            result_iter.push(result_iter_of_label);
        }

        Iter::from_iter(IterList::new(result_iter))
    }

    fn _get_all_edges(&self, label_id: Option<LabelId>) -> Iter<LocalEdge<G, I>> {
        let result_iter = self
            .graph
            .edge_references()
            .filter(move |edge| {
                if self._is_vertex_local(edge.source()) {
                    if label_id.is_some() {
                        self.graph.edge_weight(edge.id()) == label_id.as_ref()
                    } else {
                        true
                    }
                } else {
                    false
                }
            })
            // Can safely unwrap, as the edge must present
            .map(move |edge| self.edge_ref_to_local_edge(edge, true).unwrap());

        Iter::from_iter(result_iter)
    }

    fn _get_all_edges_of_labels(&self, labels: Vec<LabelId>) -> Iter<LocalEdge<G, I>> {
        let result_iter = self
            .graph
            .edge_references()
            .filter(move |edge| {
                if self._is_vertex_local(edge.source()) {
                    labels.contains(self.graph.edge_weight(edge.id()).unwrap())
                } else {
                    false
                }
            })
            .map(move |edge| self.edge_ref_to_local_edge(edge, true).unwrap());

        Iter::from_iter(result_iter)
    }

    /// Get incoming degree of a vertex
    pub fn in_degree(&self, global_id: G) -> usize {
        if let Some(id) = self.index_data.get_internal_id(global_id) {
            self.graph.neighbors_directed(id, Direction::Incoming).count()
        } else {
            0
        }
    }

    /// Get outgoing degree of a vertex
    pub fn out_degree(&self, global_id: G) -> usize {
        if let Some(id) = self.index_data.get_internal_id(global_id) {
            self.graph.neighbors_directed(id, Direction::Outgoing).count()
        } else {
            0
        }
    }

    /// Get both incoming and outgoing degree of a vertex
    pub fn degree(&self, global_id: G) -> usize {
        if let Some(id) = self.index_data.get_internal_id(global_id) {
            self.graph.neighbors_undirected(id).count()
        } else {
            0
        }
    }

    /// Verify if a vertex of given `global_id` is local to this partition
    pub fn is_vertex_local(&self, global_id: G) -> bool {
        self.index_data.global_id_to_index.contains_key(&global_id)
    }

    /// Print the statistics for debugging
    pub fn print_statistics(&self) {
        println!("Statics of the graph in partition: {}", self.partition);
        for (label, ids) in self.index_data.label_indices.iter().enumerate() {
            println!("Label {:?}, number of vertices {:?}", label, ids.len());
        }
        println!(
            "Vertex property size: {:?},\n \
            Edge property size: {:?},\n \
            Size of global_id_to_index (local vertices): {:?},\n \
            Size of corner_global_id_to_index (corner vertices): {:?},\n \
            Size of index_to_global_id: {:?},\n \
            Number of all vertices (local + corner): {:?},\n \
            Number of edges: {:?}
            ",
            self.vertex_prop_table.len(),
            self.edge_prop_table.len(),
            self.index_data.global_id_to_index.len(),
            self.index_data.corner_global_id_to_index.len(),
            self.index_data.index_to_global_id.len(),
            self.graph.node_count(),
            self.graph.edge_count(),
        );
    }
}

/// Do not expose these apis, only for internal use
trait PrivatePropertyTrait<I> {
    /// Get all properties of a given vertex specified by an internal vertex id
    fn get_all_vertex_property(&self, internal_id: &NodeIndex<I>) -> Option<RowRef>;

    /// Get all properties of a given edge specified by an internal edge id
    fn get_all_edge_property(&self, internal_id: &EdgeIndex<I>) -> Option<RowRef>;
}

impl<G, I, N, E> PrivatePropertyTrait<I> for LargeGraphDB<G, I, N, E>
where
    G: IndexType + Send + Sync,
    I: IndexType + Send + Sync,
    N: PropertyTableTrait,
    E: PropertyTableTrait,
{
    fn get_all_vertex_property(&self, internal_id: &NodeIndex<I>) -> Option<RowRef> {
        self.vertex_prop_table.get_row(internal_id.index()).ok()
    }
    fn get_all_edge_property(&self, internal_id: &EdgeIndex<I>) -> Option<RowRef> {
        self.edge_prop_table.get_row(internal_id.index()).ok()
    }
}

impl<G, I, N, E> GlobalStoreTrait<G, I> for LargeGraphDB<G, I, N, E>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
    N: PropertyTableTrait + Sync,
    E: PropertyTableTrait + Sync,
{
    fn get_adj_vertices(
        &self, src_id: G, _edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalVertex<G>> {
        if let Some(edge_labels) = _edge_labels {
            if edge_labels.len() == 1 {
                self._get_adj_vertices(src_id, Some(edge_labels[0]), dir)
            } else {
                self._get_adj_vertices_of_labels(src_id, edge_labels.clone(), dir)
            }
        } else {
            self._get_adj_vertices(src_id, None, dir)
        }
    }

    fn get_adj_edges(
        &self, src_id: G, _edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalEdge<G, I>> {
        if let Some(edge_labels) = _edge_labels {
            if edge_labels.len() == 1 {
                self._get_adj_edges(src_id, Some(edge_labels[0]), dir)
            } else {
                self._get_adj_edges_of_labels(src_id, edge_labels.clone(), dir)
            }
        } else {
            self._get_adj_edges(src_id, None, dir)
        }
    }

    fn get_vertex(&self, id: G) -> Option<LocalVertex<G>> {
        if let Some(index) = self.index_data.get_internal_id(id) {
            self.index_to_local_vertex(index, true)
        } else {
            None
        }
    }

    fn get_edge(&self, edge_id: EdgeId<G>) -> Option<LocalEdge<G, I>> {
        if self.is_vertex_local(edge_id.0) {
            let ei = edge_index::<I>(edge_id.1);
            if let Some((src, dst)) = self.graph.edge_endpoints(ei.clone()) {
                let _src_v = self.index_data.get_global_id(src);
                let _dst_v = self.index_data.get_global_id(dst);
                let label = *self.graph.edge_weight(ei).unwrap();
                if _src_v.is_some() && _dst_v.is_some() {
                    let mut local_edge = LocalEdge::new(_src_v.unwrap(), _dst_v.unwrap(), label, ei);
                    if let Some(properties) = self.get_all_edge_property(&ei) {
                        local_edge = local_edge.with_properties(RowWithSchema::new(
                            Some(properties),
                            self.graph_schema.get_edge_schema(label),
                        ));
                    }
                    Some(local_edge)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_all_vertices(&self, _labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G>> {
        if let Some(labels) = _labels {
            if labels.len() == 1 {
                self._get_all_vertices(Some(labels[0]))
            } else {
                self._get_all_vertices_of_labels(labels.clone())
            }
        } else {
            self._get_all_vertices(None)
        }
    }

    fn get_all_edges(&self, _labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>> {
        if let Some(labels) = _labels {
            if labels.len() == 1 {
                self._get_all_edges(Some(labels[0]))
            } else {
                self._get_all_edges_of_labels(labels.clone())
            }
        } else {
            self._get_all_edges(None)
        }
    }

    fn count_all_vertices(&self, _labels: Option<&Vec<LabelId>>) -> usize {
        let mut count = 0;
        if let Some(labels) = _labels {
            for &label in labels {
                if let Some(ids) = self.index_data.label_indices.get(label as usize) {
                    count += ids.len();
                }
            }
        } else {
            count = self.index_data.global_id_to_index.len()
        }

        count
    }

    fn count_all_edges(&self, _labels: Option<&Vec<LabelId>>) -> usize {
        let edge_iter =
            self.graph.edge_references().filter(|edge| self._is_vertex_local(edge.source()));

        if let Some(labels) = _labels {
            edge_iter
                .filter(move |edge| {
                    if let Some(l) = self.graph.edge_weight(edge.id()) {
                        labels.contains(l)
                    } else {
                        false
                    }
                })
                .count()
        } else {
            edge_iter.count()
        }
    }

    fn get_schema(&self) -> Arc<dyn Schema> {
        self.graph_schema.clone()
    }

    fn get_current_partition(&self) -> usize {
        self.partition
    }
}

/// A mutable version of `LargeGraphDB`
pub struct MutableGraphDB<
    G: Send + Sync + IndexType = DefaultId,
    I: Send + Sync + IndexType = InternalId,
    N: PropertyTableTrait = PropertyTable,
    E: PropertyTableTrait = SingleValueTable,
> {
    /// The root directory to maintain all the data
    pub(crate) root_dir: PathBuf,
    /// Which partition of this part of data
    pub(crate) partition: usize,
    /// The graph structure, the label will be encoded as `LabelId`
    pub(crate) graph: DiGraph<Label, LabelId, I>,
    /// Table from internal vertexs' indices to their properties
    pub(crate) vertex_prop_table: N,
    /// Table from internal edges' indices to their properties
    pub(crate) edge_prop_table: E,
    /// The index data that maintains the mapping between vertices' global ids and their internal ids
    pub(crate) index_data: IndexData<G, I>,
}

/// for graph construction
impl<G, I, N, E> MutableGraphDB<G, I, N, E>
where
    G: Eq + IndexType + Send + Sync,
    I: IndexType + Send + Sync,
    N: PropertyTableTrait + Sync,
    E: PropertyTableTrait + Sync,
{
    /// A private function that adds a vertex into the graph database.
    ///
    /// If the vertex specified by given id already presents, update the vertex's label, and return
    /// with a `false` indicator. Otherwise, return the newly assigned internal id of the vertex,
    /// with a `true` indicator
    fn add_vertex_internal(&mut self, global_id: G, label: Label) -> (bool, NodeIndex<I>) {
        if let Some(existed_vertex) = self.index_data.global_id_to_index.get(&global_id) {
            // update a more fine-grained label
            if label[1] != INVALID_LABEL_ID {
                if let Some(w) = self.graph.node_weight_mut(*existed_vertex) {
                    *w = label;
                }
            }

            (false, existed_vertex.clone())
        } else {
            let index = self.graph.add_node(label);
            self.index_data.add_vertex(global_id, label, index, false);

            (true, index)
        }
    }

    /// Similar to `Self::add_vertex_internal`, but adding a corner vertex.
    /// Unlike `Self::add_vertex_internal`:
    ///     ** Will not update a corner vertex's label if it already presents **.
    fn add_corner_vertex_internal(
        &mut self, global_id: G, label_id: LabelId,
    ) -> (bool, NodeIndex<I>) {
        if let Some(existed_vertex) = self.index_data.corner_global_id_to_index.get(&global_id) {
            return (false, existed_vertex.clone());
        }
        let label = [label_id, INVALID_LABEL_ID];
        let index = self.graph.add_node(label);
        self.index_data.add_vertex(global_id, label, index, true);

        (true, index)
    }

    /// A private function that adds an edge into the graph database.
    /// Before the adding of an edge, the vertex of both ends must already present.
    /// If this is the case, return the internal id of the edge, otherwise, return `None` value
    fn add_edge_internal(
        &mut self, global_src_id: G, global_dst_id: G, label_id: LabelId,
    ) -> Option<EdgeIndex<I>> {
        let _src_index = self.index_data.get_internal_id(global_src_id);
        let _dst_index = self.index_data.get_internal_id(global_dst_id);

        if _src_index.is_some() && _dst_index.is_some() {
            let src_index = _src_index.unwrap();
            let dst_index = _dst_index.unwrap();
            Some(self.graph.add_edge(src_index, dst_index, label_id))
        } else {
            None
        }
    }

    /// Verify if a vertex of given `global_id` is local to this partition
    pub fn is_vertex_local(&self, global_id: G) -> bool {
        self.index_data.global_id_to_index.contains_key(&global_id)
    }

    pub fn shrink_to_fit(&mut self) {
        self.index_data.shrink_to_fit();
        self.graph.shrink_to_fit();
    }

    pub fn node_count(&self) -> usize {
        self.index_data.global_id_to_index.len()
    }

    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    pub fn into_graph(self, mut schema: LDBCGraphSchema) -> LargeGraphDB<G, I, N, E> {
        schema.trim();
        LargeGraphDB {
            partition: self.partition,
            graph: self.graph,
            vertex_prop_table: self.vertex_prop_table,
            edge_prop_table: self.edge_prop_table,
            index_data: self.index_data,
            graph_schema: Arc::new(schema),
        }
    }
}

impl<G, I, N, E> MutableGraphDB<G, I, N, E>
where
    G: IndexType + Serialize + DeserializeOwned + Send + Sync,
    I: IndexType + Serialize + DeserializeOwned + Send + Sync,
    N: PropertyTableTrait + Send + Sync,
    E: PropertyTableTrait + Send + Sync,
{
    /// Export this object to bin files
    pub fn export(&self) -> GDBResult<()> {
        info!("Partition {:?} writing binary file...", self.partition);
        let partition_dir =
            self.root_dir.join(DIR_BINARY_DATA).join(format!("partition_{}", self.partition));

        create_dir_all(&partition_dir)?;

        export(&self.graph, &partition_dir.join(FILE_GRAPH_STRUCT))?;
        self.vertex_prop_table.export(&partition_dir.join(FILE_NODE_PPT_DATA))?;
        self.edge_prop_table.export(&partition_dir.join(FILE_EDGE_PPT_DATA))?;
        export(&self.index_data, &partition_dir.join(FILE_INDEX_DATA))?;

        Ok(())
    }
}

impl<G, I, N, E> GlobalStoreUpdate<G, I> for MutableGraphDB<G, I, N, E>
where
    G: IndexType + Send + Sync,
    I: IndexType + Send + Sync,
    N: PropertyTableTrait + Sync,
    E: PropertyTableTrait + Sync,
{
    fn add_vertex(&mut self, global_id: G, label: Label) -> bool {
        self.add_vertex_internal(global_id, label).0
    }

    fn add_corner_vertex(&mut self, global_id: G, label_id: LabelId) -> bool {
        self.add_corner_vertex_internal(global_id, label_id).0
    }

    fn add_or_update_vertex_properties(
        &mut self, global_id: G, properties: Row,
    ) -> GDBResult<Option<Row>> {
        if let Some(internal_id) = self.index_data.get_internal_id(global_id) {
            self.vertex_prop_table.insert(internal_id.index(), properties)
        } else {
            Err(GDBError::VertexNotFoundError)
        }
    }

    fn add_edge(&mut self, global_src_id: G, global_dst_id: G, label_id: LabelId) -> bool {
        self.add_edge_internal(global_src_id, global_dst_id, label_id).is_some()
    }

    fn add_edge_with_properties(
        &mut self, global_src_id: G, global_dst_id: G, label_id: LabelId, properties: Row,
    ) -> GDBResult<Option<Row>> {
        if let Some(edge_id) = self.add_edge_internal(global_src_id, global_dst_id, label_id) {
            self.edge_prop_table.insert(edge_id.index(), properties)
        } else {
            GDBResult::Err(GDBError::EdgeNotFoundError)
        }
    }

    fn add_vertex_batches<Iter: Iterator<Item = (G, Label, Row)>>(
        &mut self, iter: Iter,
    ) -> GDBResult<usize> {
        let mut properties: Vec<(usize, Row)> = Vec::new();
        let mut count = 0;
        for (nid, label, ppt) in iter {
            let (is_new, inner_id) = self.add_vertex_internal(nid, label);
            if is_new {
                count += 1;
            }
            // only non-empty properties will be added
            if !ppt.is_empty() {
                properties.push((inner_id.index(), ppt));
            }
        }

        self.vertex_prop_table.insert_batches(properties.into_iter())?;

        Ok(count)
    }

    fn add_edge_batches<Iter: Iterator<Item = (G, G, LabelId, Row)>>(
        &mut self, iter: Iter,
    ) -> GDBResult<usize> {
        let mut properties: Vec<(usize, Row)> = Vec::new();
        let mut count = 0;
        for (src_id, dst_id, label_id, ppt) in iter {
            if let Some(inner_id) = self.add_edge_internal(src_id, dst_id, label_id) {
                count += 1;
                // only non-empty properties will be added
                if !ppt.is_empty() {
                    properties.push((inner_id.index(), ppt));
                }
            }
        }

        let _ = self.edge_prop_table.insert_batches(properties.into_iter())?;

        Ok(count)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::config::{GraphDBConfig, JsonConf};
    use crate::ldbc::*;
    use crate::parser::DataType;
    use crate::schema::ID_FIELD;
    use crate::table::ItemType;
    use std::path::Path;

    // person ids
    static PIDS: [DefaultId; 9] = [
        1 << LABEL_SHIFT_BITS | 111,
        1 << LABEL_SHIFT_BITS | 222,
        1 << LABEL_SHIFT_BITS | 333,
        1 << LABEL_SHIFT_BITS | 444,
        1 << LABEL_SHIFT_BITS | 555,
        1 << LABEL_SHIFT_BITS | 666,
        1 << LABEL_SHIFT_BITS | 777,
        1 << LABEL_SHIFT_BITS | 888,
        1 << LABEL_SHIFT_BITS | 999,
    ];
    // comment ids
    static CIDS: [DefaultId; 9] = [
        2 << LABEL_SHIFT_BITS | 111,
        2 << LABEL_SHIFT_BITS | 222,
        2 << LABEL_SHIFT_BITS | 333,
        2 << LABEL_SHIFT_BITS | 444,
        2 << LABEL_SHIFT_BITS | 555,
        2 << LABEL_SHIFT_BITS | 666,
        2 << LABEL_SHIFT_BITS | 777,
        2 << LABEL_SHIFT_BITS | 888,
        2 << LABEL_SHIFT_BITS | 999,
    ];

    #[test]
    fn test_graph_store_update() {
        let root_dir = "data/small_data";
        let mut graphdb: MutableGraphDB<DefaultId, InternalId> =
            GraphDBConfig::default().root_dir(root_dir).number_vertex_labels(20).new();
        assert!(graphdb.add_vertex(PIDS[0], [1, INVALID_LABEL_ID]));
        // Cannot re-add a vertex
        assert!(!graphdb.add_vertex(PIDS[0], [1, INVALID_LABEL_ID]));
        assert!(graphdb.add_vertex(PIDS[2], [1, INVALID_LABEL_ID]));
        assert!(graphdb.add_corner_vertex(PIDS[5], 1));

        let prop = Row::from(vec![object!(15), object!("John")]);
        let new_prop = Row::from(vec![object!(16), object!("Steve")]);

        // Update the vertex's properties
        assert!(graphdb.add_or_update_vertex_properties(PIDS[0], prop.clone()).unwrap().is_none());

        // The vertex PIDS[1] does no exist, can not update
        assert!(graphdb.add_or_update_vertex_properties(PIDS[1], prop.clone()).is_err());

        // Add vertex PIDS[1]
        assert!(graphdb
            .add_vertex_with_properties(PIDS[1], [1, INVALID_LABEL_ID], prop.clone())
            .unwrap()
            .is_none());

        // The vertex already exists, return the old properties
        assert_eq!(
            Some(prop.clone()),
            graphdb
                .add_vertex_with_properties(PIDS[1], [1, INVALID_LABEL_ID], new_prop.clone())
                .unwrap()
        );

        assert!(graphdb.add_edge(PIDS[0], PIDS[1], 13));
        // add an edge from a corner vertex
        assert!(graphdb.add_edge(PIDS[5], PIDS[0], 12));

        // PIDS[3] does not exist, therefore `false` is returned
        assert!(!graphdb.add_edge(PIDS[0], PIDS[3], 12));
        assert!(!graphdb.add_edge(PIDS[3], PIDS[0], 12));

        let edge_prop = Row::from(20200202_i64);
        // add duplicate edge to the db
        assert!(graphdb
            .add_edge_with_properties(PIDS[0], PIDS[1], 12, edge_prop.clone())
            .unwrap()
            .is_none());

        // PIDS[3] does not exist, thus return error.
        assert!(graphdb.add_edge_with_properties(PIDS[0], PIDS[3], 12, edge_prop.clone()).is_err());

        let schema =
            LDBCGraphSchema::from_json_file("data/schema.json").expect("Get Schema error!");

        let graph = graphdb.into_graph(schema);

        assert_eq!(
            vec![prop.clone().get(0), prop.clone().get(1)],
            vec![
                graph.get_vertex(PIDS[0]).unwrap().get_property(ID_FIELD),
                graph.get_vertex(PIDS[0]).unwrap().get_property("firstName"),
            ]
        );

        assert_eq!(
            vec![new_prop.clone().get(0), new_prop.clone().get(1)],
            vec![
                graph.get_vertex(PIDS[1]).unwrap().get_property(ID_FIELD),
                graph.get_vertex(PIDS[1]).unwrap().get_property("firstName"),
            ]
        );

        // we have added vertex PIDS[0], PIDS[1], PIDS[2], and edge PIDS[0]-PIDS[1] twice (multi-edges)
        assert_eq!(3, graph.count_all_vertices(None));
        assert_eq!(2, graph.count_all_edges(None));
        // one edge of label 12
        assert_eq!(1, graph.count_all_edges(Some(&vec![12])));
        // one edge of label 13
        assert_eq!(1, graph.count_all_edges(Some(&vec![13])));
    }

    #[test]
    fn test_get_vertex_edge_by_id() {
        let data_dir = "data/small_data";
        let root_dir = "data/small_data";
        let schema_file = "data/schema.json";
        let mut loader =
            GraphLoader::<DefaultId, u32>::new(data_dir, root_dir, schema_file, 20, 0, 1);
        // load whole graph
        loader.load().expect("Load graph error!");
        let graphdb = loader.into_graph();
        let v1_id = LDBCVertexParser::to_global_id(6455, 5);
        let v2_id = LDBCVertexParser::to_global_id(1, 0);
        let v1 = graphdb.get_vertex(v1_id).unwrap();
        let v2 = graphdb.get_vertex(v2_id).unwrap();

        assert_eq!(v1.get_id(), v1_id);
        assert_eq!(v1.get_label(), [5, 12]);
        check_properties(
            &graphdb,
            &v1,
            "6455|Tsing_Hua_University|http://dbpedia.org/resource/Tsing_Hua_University",
        );

        assert_eq!(v2.get_id(), v2_id);
        assert_eq!(v2.get_label(), [0, 8]);
        check_properties(&graphdb, &v2, "1|China|http://dbpedia.org/resource/China");

        // e1 = (v1, v2), which can be both referenced from v1 and v2.
        // As this is the same machine, they must have the same internal id
        let e1_id = (v1_id, 2);
        let e1 = graphdb.get_edge(e1_id).unwrap();

        assert_eq!(e1.get_edge_id(), e1_id);
        assert_eq!(e1.get_src_id(), v1_id);
        assert_eq!(e1.get_dst_id(), v2_id);
        assert_eq!(e1.get_label(), 11);
        assert!(e1.clone_all_properties().unwrap().is_empty());

        let mut edges = graphdb.get_out_edges(v1_id, None);
        let e1 = loop {
            if let Some(e) = edges.next() {
                if e.get_edge_id().1 == 2 {
                    break e;
                }
            }
        };
        let mut edges = graphdb.get_in_edges(v2_id, None);
        let e2 = loop {
            if let Some(e) = edges.next() {
                if e.get_edge_id().1 == 2 {
                    break e;
                }
            }
        };
        assert_eq!(e1.get_src_id(), e2.get_src_id());
        assert_eq!(e1.get_dst_id(), e2.get_dst_id());
        assert_eq!(e1.get_other_id(), v2_id);
        assert_eq!(e2.get_other_id(), v1_id);
    }

    fn check_graph(graphdb: &LargeGraphDB) {
        // test get_in_vertices..
        let mut in_vertices: Vec<(DefaultId, LabelId)> = graphdb
            .get_adj_vertices(PIDS[1], None, Direction::Incoming)
            .map(|item| (item.get_id(), item.get_label()[0]))
            .collect();
        in_vertices.sort();
        assert_eq!(vec![(PIDS[0], 1), (CIDS[0], 2)], in_vertices);

        // get in_vertices of given edge labels
        let in_vertices_has_creator: Vec<(DefaultId, LabelId)> = graphdb
            .get_adj_vertices(PIDS[1], Some(&vec![0]), Direction::Incoming)
            .map(|item| (item.get_id(), item.get_label()[0]))
            .collect();
        assert_eq!(vec![(CIDS[0], 2)], in_vertices_has_creator);

        // test get_out_vertices..
        let mut out_vertices: Vec<(DefaultId, LabelId)> = graphdb
            .get_adj_vertices(PIDS[1], Some(&vec![0, 12]), Direction::Outgoing)
            .map(|item| (item.get_id(), item.get_label()[0]))
            .collect();
        out_vertices.sort();
        assert_eq!(vec![(PIDS[3], 1), (PIDS[6], 1), (PIDS[7], 1), (PIDS[8], 1)], out_vertices);

        let mut out_vertices_knows: Vec<(DefaultId, LabelId)> = graphdb
            .get_out_vertices(PIDS[1], Some(&vec![12]))
            .map(|item| (item.get_id(), item.get_label()[0]))
            .collect();
        out_vertices_knows.sort();
        assert_eq!(
            vec![(PIDS[3], 1), (PIDS[6], 1), (PIDS[7], 1), (PIDS[8], 1)],
            out_vertices_knows
        );

        // test get_in_edges..
        let in_edge_neighbor: Vec<(DefaultId, Option<ItemType>)> = graphdb
            .get_adj_edges(PIDS[1], Some(&vec![0, 12]), Direction::Incoming)
            .map(move |item| {
                (
                    item.get_src_id(),
                    item.get_property("creationDate").map(|obj| obj.try_to_owned().unwrap()),
                )
            })
            .collect();
        assert_eq!(
            vec![(PIDS[0], Some(object!(20100313073721718_u64))), (CIDS[0], None)],
            in_edge_neighbor
        );

        let in_edge_neighbor_has_creator: Vec<(DefaultId, Option<ItemType>)> = graphdb
            .get_adj_edges(PIDS[1], Some(&vec![0]), Direction::Incoming)
            .map(move |item| {
                (
                    item.get_src_id(),
                    item.get_property("creationDate").map(|obj| obj.try_to_owned().unwrap()),
                )
            })
            .collect();
        assert_eq!(vec![(CIDS[0], None)], in_edge_neighbor_has_creator);

        // test get_out_edges..
        let mut out_edge_neighbor: Vec<(DefaultId, Option<ItemType>)> = graphdb
            .get_adj_edges(PIDS[1], Some(&vec![0, 12]), Direction::Outgoing)
            .map(move |item| {
                (
                    item.get_dst_id(),
                    item.get_property("creationDate").map(|obj| obj.try_to_owned().unwrap()),
                )
            })
            .collect();
        out_edge_neighbor.sort_by_key(|k| k.0);
        assert_eq!(
            vec![
                (PIDS[3], Some(object![20100804033836982_u64])),
                (PIDS[6], Some(object![20100202163844119_u64])),
                (PIDS[7], Some(object![20100331220757321_u64])),
                (PIDS[8], Some(object![20100724111548162_u64]))
            ],
            out_edge_neighbor
        );

        // test get_vertex_properties..
        let vertex = graphdb.get_vertex(PIDS[0]).unwrap();
        let prop = vertex.get_property("locationIP").unwrap();
        assert_eq!(prop.as_str().unwrap(), "119.235.7.103");

        let vertex_none = graphdb.get_vertex(1000);
        assert_eq!(true, vertex_none.is_none());

        // test get_all_vertices..
        let all_vertices = graphdb.get_all_vertices(Some(&vec![1, 2]));
        assert_eq!(18, all_vertices.count());

        let mut all_person_vertices = graphdb
            .get_all_vertices(Some(&vec![1]))
            .map(|local_vertex| local_vertex.get_id())
            .collect::<Vec<DefaultId>>();
        all_person_vertices.sort();
        assert_eq!(
            all_person_vertices,
            vec![PIDS[0], PIDS[1], PIDS[2], PIDS[3], PIDS[4], PIDS[5], PIDS[6], PIDS[7], PIDS[8]]
        );

        // test get_all_edges..
        let all_edges = graphdb.get_all_edges(Some(&vec![0, 12]));
        assert_eq!(18, all_edges.count());

        let all_knows_edges = graphdb
            .get_all_edges(Some(&vec![12]))
            .map(|local_edge| (local_edge.get_src_id(), local_edge.get_dst_id()))
            .collect::<Vec<(DefaultId, DefaultId)>>();
        assert_eq!(
            vec![
                (PIDS[0], PIDS[1]),
                (PIDS[0], PIDS[2]),
                (PIDS[0], PIDS[3]),
                (PIDS[0], PIDS[4]),
                (PIDS[0], PIDS[5]),
                (PIDS[1], PIDS[3]),
                (PIDS[1], PIDS[6]),
                (PIDS[1], PIDS[7]),
                (PIDS[1], PIDS[8])
            ],
            all_knows_edges
        );

        // test count_all_vertices..
        let all_vertex_count = graphdb.count_all_vertices(None);
        assert_eq!(18, all_vertex_count);

        // test count_all_edges..
        let all_edge_count = graphdb.count_all_edges(None);
        assert_eq!(18, all_edge_count);

        // test count vertices of certain type
        assert_eq!(9, graphdb.count_all_vertices(Some(&vec![1])));

        // test count edges of certain type
        assert_eq!(9, graphdb.count_all_edges(Some(&vec![12])));
    }

    fn check_properties<G: IndexType + Send + Sync, I: IndexType + Send + Sync>(
        graph: &LargeGraphDB<G, I>, vertex: &LocalVertex<G>, record: &str,
    ) {
        let expected_results: Vec<&str> = record.split('|').collect();

        let mut index = 0;
        for (name, dt) in graph.get_schema().get_vertex_header(vertex.get_label()[0]).unwrap() {
            if name != "~LABEL" {
                // does not store LABEL as properties
                match dt {
                    DataType::String => {
                        assert_eq!(
                            vertex.get_property(name).unwrap().as_str().unwrap(),
                            expected_results[index]
                        )
                    }
                    _ => assert_eq!(
                        vertex.get_property(name).unwrap().as_u64().unwrap(),
                        expected_results[index].parse::<u64>().unwrap()
                    ),
                }
            }
            index += 1;
        }
    }

    #[test]
    fn test_graph_query() {
        let data_dir = "data/large_data";
        let root_dir = "data/large_data";
        let schema_file = "data/schema.json";
        let mut loader =
            GraphLoader::<DefaultId, u32>::new(data_dir, root_dir, schema_file, 20, 0, 1);
        // load whole graph
        loader.load().expect("Load graph error!");
        let graphdb = loader.into_graph();

        check_graph(&graphdb);
    }

    #[test]
    fn test_serde() {
        let temp = tempdir::TempDir::new("test_serde").expect("Open temp folder error");
        let data_dir = Path::new("data/large_data");
        let root_dir = temp.path();
        let schema_file = Path::new("data/schema.json");
        let mut loader =
            GraphLoader::<DefaultId, InternalId>::new(&data_dir, &root_dir, &schema_file, 20, 0, 1);
        // load whole graph
        loader.load().expect("Load graph error");
        let graph = loader.into_mutable_graph();
        graph.export().expect("Export error!");

        let imported_graph = GraphDBConfig::default()
            .root_dir(root_dir)
            .schema_file(&schema_file)
            .open::<DefaultId, InternalId, _, _>()
            .expect("Import graph error");

        check_graph(&imported_graph);
    }
}
