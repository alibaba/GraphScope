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

use crate::common::*;
use crate::error::GDBResult;
use crate::parser::DataType;
use crate::schema::Schema;
use crate::table::*;
use crate::utils::Iter;
use petgraph::graph::{EdgeIndex, IndexType};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use itertools::Itertools;
pub use petgraph::Direction;

/// Edge id is associated with its start/end-vertex's id given by `G`, and an internal index
/// associated with the start/end vertex.
pub type EdgeId<G> = (G, usize);

/// Construct a row with its schema
#[derive(Clone)]
pub struct RowWithSchema<'a> {
    row: RowRef<'a>,
    header: Option<&'a HashMap<String, (DataType, usize)>>,
}

impl<'a> Debug for RowWithSchema<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowWithSchema").field("row", &self.row).finish()
    }
}

impl<'a> RowWithSchema<'a> {
    pub fn new(
        row: Option<RowRef<'a>>, header: Option<&'a HashMap<String, (DataType, usize)>>,
    ) -> Option<Self> {
        if row.is_none() {
            None
        } else {
            Some(Self { row: row.unwrap(), header })
        }
    }

    /// Get certain property specified by `key`, where
    /// * `key` can be a property name, which can be used to search the index of the field from `Self::header`.
    /// * `key` can also be a number-value, giving the index directly in case that `Self::header` is `None`
    ///
    /// Return a `BorrowObject` indicating the data type and value, and `None` if:
    /// * `Self::header` is `None` and `key` is not a number value
    /// * The property given by `key` does not exist
    ///
    pub fn get(&self, key: &str) -> Option<ItemTypeRef> {
        if let Some(header) = &self.header {
            if let Some((_, index)) = header.get(key) {
                self.row.get(*index)
            } else {
                None
            }
        } else {
            if let Ok(index) = key.parse::<usize>() {
                self.row.get(index)
            } else {
                None
            }
        }
    }

    /// Turn into a map of all properties
    pub fn into_properties(self) -> Option<HashMap<String, ItemType>> {
        self.header.and_then(|header| {
            let mut map = HashMap::new();
            for (key, (_, index)) in header.iter().sorted_by(|x, y| x.1 .1.cmp(&y.1 .1)) {
                if let Some(val) = self.row.get(*index) {
                    if let Some(obj) = val.try_to_owned() {
                        map.insert(key.clone(), obj);
                    }
                }
            }
            Some(map)
        })
    }
}

/// A data structure to maintain a local view of the vertex.
#[derive(Debug, Clone)]
pub struct LocalVertex<'a, G: IndexType> {
    /// The vertex's global id
    id: G,
    /// The vertex's label
    label: Label,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    properties: Option<RowWithSchema<'a>>,
}

impl<'a, G: IndexType> LocalVertex<'a, G> {
    pub fn new(id: G, label: Label) -> Self {
        LocalVertex { id, label, properties: None }
    }

    pub fn with_property(id: G, label: Label, properties: Option<RowWithSchema<'a>>) -> Self {
        LocalVertex { id, label, properties }
    }

    pub fn get_id(&self) -> G {
        self.id
    }

    pub fn get_label(&self) -> Label {
        self.label
    }

    pub fn get_property(&self, key: &str) -> Option<ItemTypeRef> {
        self.properties.as_ref().and_then(|prop| prop.get(key))
    }

    pub fn clone_all_properties(&self) -> Option<HashMap<String, ItemType>> {
        self.properties.as_ref().and_then(|prop| prop.clone().into_properties())
    }
}

/// A data structure to maintain a local view of the edge.
#[derive(Debug, Clone)]
pub struct LocalEdge<'a, G: IndexType, I: IndexType> {
    /// The start vertex's global id
    start: G,
    /// The end vertex's global id
    end: G,
    /// The edge label id
    label: LabelId,
    /// Whether this edge has been obtained from `Self::start`
    from_start: bool,
    /// The internal edge id associated with either `Self::start` or `Self::end`
    edge_id: EdgeIndex<I>,
    /// The properties of the edge if any
    properties: Option<RowWithSchema<'a>>,
}

impl<'a, G: IndexType, I: IndexType> LocalEdge<'a, G, I> {
    pub fn new(start: G, end: G, label: LabelId, edge_id: EdgeIndex<I>) -> Self {
        LocalEdge { start, end, label, from_start: true, edge_id, properties: None }
    }

    /// Set the properties of this edge
    pub fn with_properties(mut self, properties: Option<RowWithSchema<'a>>) -> Self {
        self.properties = properties;
        self
    }

    /// Set the `from_start` indicator
    pub fn with_from_start(mut self, from_start: bool) -> Self {
        self.from_start = from_start;
        self
    }

    /// An edge is uniquely indiced by its start/end vertex's global id, as well
    /// as its internal id indiced from this start/end-vertex.
    /// Whether this is a start/end vertex, is determined by `Self::from_start`
    pub fn get_edge_id(&self) -> EdgeId<G> {
        if self.from_start {
            (self.start, self.edge_id.index())
        } else {
            (self.end, self.edge_id.index())
        }
    }

    pub fn get_src_id(&self) -> G {
        self.start
    }

    pub fn get_dst_id(&self) -> G {
        self.end
    }

    /// Get the other vertex of this edge.
    /// If `Self::from_start`, return `Self::end`, otherwise, return `Self::start`
    pub fn get_other_id(&self) -> G {
        if self.from_start {
            self.end
        } else {
            self.start
        }
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_property(&self, key: &str) -> Option<ItemTypeRef> {
        self.properties.as_ref().and_then(|prop| prop.get(key))
    }

    pub fn clone_all_properties(&self) -> Option<HashMap<String, ItemType>> {
        self.properties.as_ref().and_then(|prop| prop.clone().into_properties())
    }
}

pub trait GlobalStoreTrait<G: IndexType, I: IndexType> {
    /// Get all the vertices linked from the given vertex `src_id`. The linked edge must also
    /// satisfy the edge labels `edge_labels` and the direction `dir`.
    ///
    /// # Return
    ///  * An iterator of vertices, if query successfully
    ///  * An empty iterator, if the given vertex does not present or it contains no edge.
    fn get_adj_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalVertex<G>>;

    /// Get all the edges regarding with the labels `edge_labels` and direction `dir`
    /// from the given vertex `src_id`.
    ///
    /// # Return
    /// * An iterator of edges, if query successfully
    /// * An empty iterator, if the given vertex does not present or it contains no edge.
    fn get_adj_edges(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalEdge<G, I>>;

    /// A wrapper of `Self::get_adj_vertices()` for outgoing direction.
    fn get_out_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>,
    ) -> Iter<LocalVertex<G>> {
        self.get_adj_vertices(src_id, edge_labels, Direction::Outgoing)
    }

    /// A wrapper of `Self::get_adj_vertices()` for incoming direction.
    fn get_in_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>,
    ) -> Iter<LocalVertex<G>> {
        self.get_adj_vertices(src_id, edge_labels, Direction::Incoming)
    }

    /// Concatenate `get_out_vertices()` and `get_in_vertices()`
    fn get_both_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>,
    ) -> Iter<LocalVertex<G>> {
        Iter::from_iter(
            self.get_out_vertices(src_id, edge_labels)
                .chain(self.get_in_vertices(src_id, edge_labels)),
        )
    }

    /// A wrapper of `Self::get_adj_edges()` for outgoing direction.
    fn get_out_edges(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>,
    ) -> Iter<LocalEdge<G, I>> {
        self.get_adj_edges(src_id, edge_labels, Direction::Outgoing)
    }

    /// A wrapper of `Self::get_adj_edges()` for incoming direction.
    fn get_in_edges(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>> {
        self.get_adj_edges(src_id, edge_labels, Direction::Incoming)
    }

    /// A wrapper of `Self::get_adj_edges()` for both directions.
    fn get_both_edges(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>,
    ) -> Iter<LocalEdge<G, I>> {
        Iter::from_iter(
            self.get_out_edges(src_id, edge_labels).chain(self.get_in_edges(src_id, edge_labels)),
        )
    }

    /// Get the vertex of given global identity
    fn get_vertex(&self, id: G) -> Option<LocalVertex<G>>;

    /// Get the edge of given source vertex id and its internal index
    fn get_edge(&self, edge_id: EdgeId<G>) -> Option<LocalEdge<G, I>>;

    /// Get all vertices of a given labels. If `None` label is given, return all vertices.
    fn get_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G>>;

    /// Get all edges of given labels. If `None` label is given, return all vertices.
    fn get_all_edges(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>>;

    /// Count all vertices of given labels in current partition.
    /// If `None` labels is given, count all vertices.
    fn count_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> usize;

    /// Count all edges of given labels where the source vertex is in current partition
    /// If `None` labels is given, count all edges.
    fn count_all_edges(&self, labels: Option<&Vec<LabelId>>) -> usize;

    /// Get the schema for either vertex/edge properties
    fn get_schema(&self) -> Arc<dyn Schema>;

    /// Get the partition of current storage.
    fn get_current_partition(&self) -> usize;
}

pub trait GlobalStoreUpdate<G: Copy, I> {
    /// Add a vertex to the storage, return `true` if added,
    /// `false` if the vertex already presents.
    fn add_vertex(&mut self, global_id: G, label: Label) -> bool;

    /// Add a corner vertex to the storage, return `true` if added,
    /// `false` if the corner vertex already presents.
    ///
    /// Note that a corner vertex, as introduced by an edge, only need to know its primary
    /// vertex type, therefore, we add here `label_id` instead of a full-version label.
    fn add_corner_vertex(&mut self, global_id: G, label_id: LabelId) -> bool;

    /// Add or update a vertex (cannot be corner vertex)'s properties. Return
    /// * `Err` if the vertex does not exist or unexpected errors occur.
    /// * `Ok(None)` if the vertex's properties do not present, and the data is inserted
    /// * `Ok(Some(old_data))` if the vertex's properties do present, and the data is updated.
    fn add_or_update_vertex_properties(
        &mut self, global_id: G, properties: Row,
    ) -> GDBResult<Option<Row>>;

    /// And a vertex (cannot be corner vertex) with its properties. Return
    /// * `Err` if the vertex does not exist or unexpected errors occur
    /// * `Ok(None)` if the vertex's properties do not present, and the data is inserted
    /// * `Ok(Some(old_data))` if the vertex's properties do present, and the data is updated.
    fn add_vertex_with_properties(
        &mut self, global_id: G, label: Label, properties: Row,
    ) -> GDBResult<Option<Row>> {
        self.add_vertex(global_id, label);
        self.add_or_update_vertex_properties(global_id, properties)
    }

    /// Add an edge to the storage, return `true` if added, `false` if errors occur (either src or
    /// dst node does not present in this graph). Note that multiple edges can be added for the
    /// same pair of (src, dst).
    fn add_edge(&mut self, global_src_id: G, global_dst_id: G, label_id: LabelId) -> bool;

    /// And an edge with its properties, Returns
    /// * `Err` if either the src vertex or dst vertex do not present or unexpected errors occur.
    /// * `Ok(None)` if the edge's properties do not present, and the data is inserted
    /// * `Ok(Some(old_data))` if the edge's properties do present, and the data is updated.
    fn add_edge_with_properties(
        &mut self, global_src_id: G, global_dst_id: G, label_id: LabelId, properties: Row,
    ) -> GDBResult<Option<Row>>;

    /// Add (none-corner) vertexs in batches, where each item contains the following elements:
    /// * vertex's global id with type `G`
    /// * vertex's label id
    /// * vertex's property if any (corner vertex does not have any property)
    ///
    /// Return the number of vertexs that were successfully added. If there is any error while
    /// attempting to add certain vertex, `PropertyError` will be thrown.
    fn add_vertex_batches<Iter: Iterator<Item = (G, Label, Row)>>(
        &mut self, iter: Iter,
    ) -> GDBResult<usize>;

    /// Add edges in batches, where each item contains the following elements:
    /// * edge's src global id with type `G`
    /// * edge's dst global id with type `G`
    /// * edge's label id
    /// * edge's property if any
    ///
    /// Return the number of edges that were successfully added.  If there is any error while
    /// attempting to add certain edge, `PropertyError` will be thrown.
    fn add_edge_batches<Iter: Iterator<Item = (G, G, LabelId, Row)>>(
        &mut self, iter: Iter,
    ) -> GDBResult<usize>;
}
