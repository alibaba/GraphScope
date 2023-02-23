use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::col_table::ColTable;
use crate::columns::RefItem;
use crate::graph::{Direction, IndexType};
use crate::schema::Schema;
use crate::types::*;
use crate::utils::Iter;
use crate::vertex_map::VertexMap;

/// A data structure to maintain a local view of the vertex.
#[derive(Debug, Clone)]
pub struct LocalVertex<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// The vertex's global id
    index: I,
    /// The vertex's label
    label: LabelId,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    ///
    table: Option<&'a ColTable>,
    id_list: &'a Vec<G>,
    corner_id_list: &'a Vec<G>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalVertex<'a, G, I> {
    pub fn new(index: I, label: LabelId, id_list: &'a Vec<G>, corner_id_list: &'a Vec<G>) -> Self {
        LocalVertex { index, label, id_list, table: None, corner_id_list }
    }

    pub fn with_property(
        index: I, label: LabelId, id_list: &'a Vec<G>, corner_id_list: &'a Vec<G>,
        table: Option<&'a ColTable>,
    ) -> Self {
        LocalVertex { index, label, id_list, table, corner_id_list }
    }

    pub fn get_id(&self) -> G {
        let mut index = self.index.index();
        if self.label == 1 || self.label == 5 {
            index = self.index.index() & (1_usize << 32) - 1_usize;
        }
        if index < self.id_list.len() {
            self.id_list[index]
        } else {
            // should use graph schema to get label id of "PERSON" and "ORGANISATION"
            if self.label == 1 || self.label == 5 {
                self.corner_id_list[I::new((1_usize << 32) - 1_usize).index() - index - 1]
            } else {
                self.corner_id_list[<I as IndexType>::max().index() - index - 1]
            }
        }
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_property(&self, key: &str) -> Option<RefItem> {
        if let Some(prop) = self.table {
            prop.get_item(key, self.index.index())
        } else {
            None
        }
    }

    pub fn get_all_properties(&self) -> Option<HashMap<String, RefItem>> {
        if let Some(prop) = self.table {
            let mut property_table = HashMap::new();
            for head in prop.header.keys() {
                property_table.insert(head.clone(), prop.get_item(head, self.index.index()).unwrap());
            }
            Some(property_table)
        } else {
            None
        }
    }
}

/// A data structure to maintain a local view of the edge.
#[derive(Clone)]
pub struct LocalEdge<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// The start vertex's global id
    start: I,
    /// The end vertex's global id
    end: I,
    /// The edge label id
    label: LabelId,
    src_label: LabelId,
    dst_label: LabelId,

    vertex_map: &'a VertexMap<G, I>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalEdge<'a, G, I> {
    pub fn new(
        start: I, end: I, label: LabelId, src_label: LabelId, dst_label: LabelId,
        vertex_map: &'a VertexMap<G, I>,
    ) -> Self {
        LocalEdge { start, end, label, src_label, dst_label, vertex_map }
    }

    pub fn get_src_id(&self) -> G {
        self.vertex_map
            .get_global_id(self.src_label, self.start)
            .unwrap()
    }

    pub fn get_dst_id(&self) -> G {
        self.vertex_map
            .get_global_id(self.dst_label, self.end)
            .unwrap()
    }

    pub fn get_label(&self) -> LabelId {
        self.label
    }

    pub fn get_src_lid(&self) -> I {
        self.start
    }

    pub fn get_dst_lid(&self) -> I {
        self.end
    }

    pub fn get_encoded_data(&self) -> I {
        self.start.hi().bw_or(self.end.hi())
    }
}

pub trait GlobalCsrTrait<G: IndexType + Sync + Send, I: IndexType + Sync + Send> {
    /// Get all the vertices linked from the given vertex `src_id`. The linked edge must also
    /// satisfy the edge labels `edge_labels` and the direction `dir`.
    ///
    /// # Return
    ///  * An iterator of vertices, if query successfully
    ///  * An empty iterator, if the given vertex does not present or it contains no edge.
    fn get_adj_vertices(
        &self, src_id: G, edge_labels: Option<&Vec<LabelId>>, dir: Direction,
    ) -> Iter<LocalVertex<G, I>>;

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
    fn get_out_vertices(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>> {
        self.get_adj_vertices(src_id, edge_labels, Direction::Outgoing)
    }

    /// A wrapper of `Self::get_adj_vertices()` for incoming direction.
    fn get_in_vertices(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>> {
        self.get_adj_vertices(src_id, edge_labels, Direction::Incoming)
    }

    /// Concatenate `get_out_vertices()` and `get_in_vertices()`
    fn get_both_vertices(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>> {
        Iter::from_iter(
            self.get_out_vertices(src_id, edge_labels)
                .chain(self.get_in_vertices(src_id, edge_labels)),
        )
    }

    /// A wrapper of `Self::get_adj_edges()` for outgoing direction.
    fn get_out_edges(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>> {
        self.get_adj_edges(src_id, edge_labels, Direction::Outgoing)
    }

    /// A wrapper of `Self::get_adj_edges()` for incoming direction.
    fn get_in_edges(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>> {
        self.get_adj_edges(src_id, edge_labels, Direction::Incoming)
    }

    /// A wrapper of `Self::get_adj_edges()` for both directions.
    fn get_both_edges(&self, src_id: G, edge_labels: Option<&Vec<LabelId>>) -> Iter<LocalEdge<G, I>> {
        Iter::from_iter(
            self.get_out_edges(src_id, edge_labels)
                .chain(self.get_in_edges(src_id, edge_labels)),
        )
    }

    /// Get the vertex of given global identity
    fn get_vertex(&self, id: G) -> Option<LocalVertex<G, I>>;

    /// Get all vertices of a given labels. If `None` label is given, return all vertices.
    fn get_all_vertices(&self, labels: Option<&Vec<LabelId>>) -> Iter<LocalVertex<G, I>>;

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
