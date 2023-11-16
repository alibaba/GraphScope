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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ptr;
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
        let index = self.index.index();
        if index < self.id_list.len() {
            self.id_list[index]
        } else {
            self.corner_id_list[<I as IndexType>::max().index() - index - 1]
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

    offset: usize,
    /// A property reference maintains a `Row` view of the properties, which is either
    /// a reference or an owned structure, depending on the form of storage.
    table: Option<&'a ColTable>,

    vertex_map: &'a VertexMap<G, I>,
}

impl<'a, G: IndexType + Sync + Send, I: IndexType + Sync + Send> LocalEdge<'a, G, I> {
    pub fn new(
        start: I, end: I, label: LabelId, src_label: LabelId, dst_label: LabelId,
        vertex_map: &'a VertexMap<G, I>, offset: usize, properties: Option<&'a ColTable>,
    ) -> Self {
        LocalEdge { start, end, label, src_label, dst_label, offset, table: properties, vertex_map }
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

    pub fn get_src_label(&self) -> LabelId {
        self.src_label
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    pub fn get_dst_label(&self) -> LabelId {
        self.dst_label
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

    pub fn get_property(&self, key: &str) -> Option<RefItem> {
        if let Some(prop) = self.table {
            prop.get_item(key, self.offset)
        } else {
            None
        }
    }

    pub fn get_all_properties(&self) -> Option<HashMap<String, RefItem>> {
        if let Some(prop) = self.table {
            let mut property_table = HashMap::new();
            for head in prop.header.keys() {
                property_table.insert(head.clone(), prop.get_item(head, self.offset).unwrap());
            }
            Some(property_table)
        } else {
            None
        }
    }
}

pub struct Nbr<I> {
    pub neighbor: I,
}

impl<I: IndexType> Clone for Nbr<I> {
    fn clone(&self) -> Self {
        Nbr { neighbor: I::new(self.neighbor.index()) }
    }
}

pub struct NbrIter<'a, I> {
    begin: *const Nbr<I>,
    end: *const Nbr<I>,
    _marker: PhantomData<&'a Nbr<I>>,
}

impl<'a, I: IndexType> NbrIter<'a, I> {
    pub fn new(begin: *const Nbr<I>, end: *const Nbr<I>) -> Self {
        Self { begin, end, _marker: PhantomData }
    }

    pub fn new_empty() -> Self {
        Self { begin: ptr::null(), end: ptr::null(), _marker: PhantomData }
    }

    #[inline]
    pub fn empty(&self) -> bool {
        self.begin == self.end
    }

    pub fn new_single(begin: *const Nbr<I>) -> Self {
        Self { begin, end: unsafe { begin.add(1) }, _marker: PhantomData }
    }

    pub fn slice(self, from: usize, to: usize) -> Self {
        let begin = unsafe { self.begin.offset(from as isize) };
        let end = unsafe { self.begin.offset(to as isize) };
        Self { begin, end, _marker: PhantomData }
    }
}

impl<'a, I: IndexType> Iterator for NbrIter<'a, I> {
    type Item = &'a Nbr<I>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.begin >= self.end {
            None
        } else {
            unsafe {
                let cur = self.begin;
                self.begin = self.begin.offset(1);
                Some(&*cur)
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.begin = unsafe { self.begin.offset(n as isize) };
        self.next()
    }
}

unsafe impl<'a, I: IndexType> Send for NbrIter<'a, I> {}

unsafe impl<'a, I: IndexType> Sync for NbrIter<'a, I> {}

pub struct NbrOffsetIter<'a, I> {
    begin: *const Nbr<I>,
    end: *const Nbr<I>,
    offset_begin: *const usize,
    offset_end: *const usize,
    _marker: PhantomData<&'a Nbr<I>>,
}

impl<'a, I: IndexType> NbrOffsetIter<'a, I> {
    pub fn new(
        begin: *const Nbr<I>, end: *const Nbr<I>, offset_begin: *const usize, offset_end: *const usize,
    ) -> Self {
        Self { begin, end, offset_begin, offset_end, _marker: PhantomData }
    }

    pub fn new_empty() -> Self {
        Self {
            begin: ptr::null(),
            end: ptr::null(),
            offset_begin: ptr::null(),
            offset_end: ptr::null(),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn empty(&self) -> bool {
        self.begin == self.end && self.offset_begin == self.offset_end
    }

    pub fn new_single(begin: *const Nbr<I>, offset_begin: *const usize) -> Self {
        if offset_begin == ptr::null() {
            Self {
                begin,
                end: unsafe { begin.add(1) },
                offset_begin: ptr::null(),
                offset_end: ptr::null(),
                _marker: PhantomData,
            }
        } else {
            Self {
                begin,
                end: unsafe { begin.add(1) },
                offset_begin,
                offset_end: unsafe { offset_begin.add(1) },
                _marker: PhantomData,
            }
        }
    }

    pub fn slice(self, from: usize, to: usize) -> Self {
        let begin = unsafe { self.begin.offset(from as isize) };
        let end = unsafe { self.begin.offset(to as isize) };
        if self.offset_begin == ptr::null() {
            Self { begin, end, offset_begin: ptr::null(), offset_end: ptr::null(), _marker: PhantomData }
        } else {
            let offset_begin = unsafe { self.offset_begin.offset(from as isize) };
            let offset_end = unsafe { self.offset_end.offset(to as isize) };
            Self { begin, end, offset_begin, offset_end, _marker: PhantomData }
        }
    }
}

impl<'a, I: IndexType> Iterator for NbrOffsetIter<'a, I> {
    type Item = (&'a Nbr<I>, Option<&'a usize>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.begin == self.end {
            None
        } else {
            unsafe {
                let cur_nbr = self.begin;
                let cur_offset = self.offset_begin;
                self.begin = self.begin.offset(1);
                if cur_offset != ptr::null() {
                    self.offset_begin = self.offset_begin.offset(1);
                    Some(((&*cur_nbr), Some(&*cur_offset)))
                } else {
                    Some(((&*cur_nbr), None))
                }
            }
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.begin = unsafe { self.begin.offset(n as isize) };
        self.next()
    }
}

unsafe impl<'a, I: IndexType> Send for NbrOffsetIter<'a, I> {}

unsafe impl<'a, I: IndexType> Sync for NbrOffsetIter<'a, I> {}

pub trait CsrTrait<I: IndexType>: Send + Sync {
    fn vertex_num(&self) -> I;

    fn edge_num(&self) -> usize;

    fn degree(&self, src: I) -> i64;

    fn get_edges(&self, src: I) -> Option<NbrIter<'_, I>>;

    fn get_edges_with_offset(&self, src: I) -> Option<NbrOffsetIter<'_, I>>;

    fn get_all_edges<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = (I, (&'a Nbr<I>, Option<&'a usize>))> + 'a + Send>;

    fn serialize(&self, path: &String);

    fn as_any(&self) -> &dyn Any;
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
