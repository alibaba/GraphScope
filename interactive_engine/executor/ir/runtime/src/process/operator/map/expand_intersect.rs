//
//! Copyright 2022 Alibaba Group Holding Limited.
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
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::ops::RangeBounds;

use dyn_type::BorrowObject;
use graph_proxy::apis::graph::element::GraphElement;
use graph_proxy::apis::{Direction, Edge, Element, QueryParams, Statement, ID};
use ir_common::error::ParsePbError;
use ir_common::generated::physical as pb;
use ir_common::KeyId;
use itertools::Itertools;
use pegasus::api::function::{FilterMapFunction, FnResult};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::entry::{DynEntry, Entry, EntryType};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

/// An ExpandOrIntersect operator to expand neighbors
/// and intersect with the ones of the same tag found previously (if exists).
/// Notice that edge_or_end_v_tag (the alias of expanded neighbors) must be specified.
struct ExpandOrIntersect<E: Entry> {
    start_v_tag: Option<KeyId>,
    edge_or_end_v_tag: KeyId,
    stmt: Box<dyn Statement<ID, E>>,
}

/// An optimized entry implementation for intersection, which denotes a collection of vertices;
/// Specifically, vertex_vec records the unique vertex ids in the collection,
/// and count_vec records the number of the corresponding vertex, since duplicated vertices are allowed.
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub struct IntersectionEntry {
    vertex_vec: Vec<ID>,
    count_vec: Vec<u32>,
}

impl_as_any!(IntersectionEntry);

impl IntersectionEntry {
    pub fn from_iter<I: Iterator<Item = ID>>(iter: I) -> IntersectionEntry {
        let mut vertex_count_map = BTreeMap::new();
        for vertex in iter {
            let cnt = vertex_count_map.entry(vertex).or_insert(0);
            *cnt += 1;
        }
        let mut vertex_vec = Vec::with_capacity(vertex_count_map.len());
        let mut count_vec = Vec::with_capacity(vertex_count_map.len());
        for (vertex, cnt) in vertex_count_map.into_iter() {
            vertex_vec.push(vertex);
            count_vec.push(cnt);
        }
        IntersectionEntry { vertex_vec, count_vec }
    }

    fn intersect<Iter: Iterator<Item = ID>>(&mut self, seeker: Iter) {
        let len = self.vertex_vec.len();
        let mut s = vec![0; len];
        for vid in seeker {
            if let Ok(idx) = self
                .vertex_vec
                .binary_search_by(|e| e.cmp(&vid))
            {
                s[idx] += 1;
            }
        }
        let mut idx = 0;
        for (i, cnt) in s.into_iter().enumerate() {
            if cnt != 0 {
                self.vertex_vec.swap(idx, i);
                self.count_vec.swap(idx, i);
                self.count_vec[idx] *= cnt;
                idx += 1;
            }
        }
        self.vertex_vec.drain(idx..);
        self.count_vec.drain(idx..);
    }

    fn is_empty(&self) -> bool {
        self.vertex_vec.is_empty()
    }

    fn len(&self) -> usize {
        let mut len = 0;
        for count in self.count_vec.iter() {
            len += *count;
        }
        len as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = &ID> {
        self.vertex_vec
            .iter()
            .zip(&self.count_vec)
            .flat_map(move |(vertex, count)| std::iter::repeat(vertex).take(*count as usize))
    }

    pub fn drain(&mut self) -> impl Iterator<Item = ID> + '_ {
        self.vertex_vec
            .drain(..)
            .zip(&self.count_vec)
            .flat_map(move |(vertex, count)| std::iter::repeat(vertex).take(*count as usize))
    }
}

impl Encode for IntersectionEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.vertex_vec.write_to(writer)?;
        self.count_vec.write_to(writer)?;
        Ok(())
    }
}

impl Decode for IntersectionEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vertex_vec = <Vec<ID>>::read_from(reader)?;
        let count_vec = <Vec<u32>>::read_from(reader)?;
        Ok(IntersectionEntry { vertex_vec, count_vec })
    }
}

impl Element for IntersectionEntry {
    fn len(&self) -> usize {
        self.len()
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl<E: Entry + 'static> FilterMapFunction<Record, Record> for ExpandOrIntersect<E> {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input.get(self.start_v_tag).ok_or_else(|| {
            FnExecError::get_tag_error(&format!(
                "get start_v_tag {:?} from record in `ExpandOrIntersect` operator, the record is {:?}",
                self.start_v_tag, input
            ))
        })?;
        match entry.get_type() {
            EntryType::Vertex => {
                let id = entry.id();
                let iter = self.stmt.exec(id)?.map(|e| {
                    if let Some(vertex) = e.as_vertex() {
                        vertex.id() as ID
                    } else if let Some(edge) = e.as_edge() {
                        edge.get_other_id() as ID
                    } else {
                        unreachable!()
                    }
                });
                if let Some(pre_entry) = input.get_mut(Some(self.edge_or_end_v_tag)) {
                    // the case of expansion and intersection
                    let pre_intersection = pre_entry
                        .as_any_mut()
                        .downcast_mut::<IntersectionEntry>()
                        .ok_or_else(|| {
                            FnExecError::unexpected_data_error(&format!(
                                "entry  is not a intersection in ExpandOrIntersect"
                            ))
                        })?;
                    pre_intersection.intersect(iter);
                    if pre_intersection.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(input))
                    }
                } else {
                    // the case of expansion only
                    let neighbors_intersection = IntersectionEntry::from_iter(iter);
                    if neighbors_intersection.is_empty() {
                        Ok(None)
                    } else {
                        // append columns without changing head
                        let columns = input.get_columns_mut();
                        columns
                            .insert(self.edge_or_end_v_tag as usize, DynEntry::new(neighbors_intersection));
                        Ok(Some(input))
                    }
                }
            }
            _ => Err(FnExecError::unsupported_error(&format!(
                "expand or intersect entry {:?} of tag {:?} failed in ExpandOrIntersect",
                entry, self.edge_or_end_v_tag
            )))?,
        }
    }
}

impl FilterMapFuncGen for pb::EdgeExpand {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        if self.is_optional {
            return Err(FnGenError::unsupported_error("optional edge expand in ExpandIntersection"));
        }
        let graph = graph_proxy::apis::get_graph().ok_or_else(|| FnGenError::NullGraphError)?;
        let start_v_tag = self.v_tag;
        let edge_or_end_v_tag = self
            .alias
            .ok_or_else(|| ParsePbError::from("`EdgeExpand::alias` cannot be empty for intersection"))?;
        let direction_pb: pb::edge_expand::Direction = unsafe { ::std::mem::transmute(self.direction) };
        let direction = Direction::from(direction_pb);
        let query_params: QueryParams = self.params.try_into()?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!(
                "Runtime expand collection operator of edge with start_v_tag {:?}, end_tag {:?}, direction {:?}, query_params {:?}",
                start_v_tag, edge_or_end_v_tag, direction, query_params
            );
        }
        if self.expand_opt != pb::edge_expand::ExpandOpt::Vertex as i32 {
            Err(FnGenError::unsupported_error("expand edges in ExpandIntersection"))
        } else {
            if query_params.filter.is_some() {
                // Expand vertices with filters on edges.
                // This can be regarded as a combination of EdgeExpand (with expand_opt as Edge) + GetV
                let stmt = graph.prepare_explore_edge(direction, &query_params)?;
                let edge_expand_operator = ExpandOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
                Ok(Box::new(edge_expand_operator))
            } else {
                // Expand vertices without any filters
                let stmt = graph.prepare_explore_vertex(direction, &query_params)?;
                let edge_expand_operator = ExpandOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
                Ok(Box::new(edge_expand_operator))
            }
        }
    }
}

// EdgeMatching denotes the matching of edges of one EdgeExpand during the intersection,
// e.g., from a previously matched vertex a1, we expand an edge [a1->c1].
// We define `EdgeMatching` (rather than use Edge directly), to support duplicated edge matchings,
// e.g., we may actually expand two a1->c1 (with different edge types).
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
struct EdgeMatching {
    matching: Vec<Edge>,
}

impl Default for EdgeMatching {
    fn default() -> Self {
        Self { matching: Vec::new() }
    }
}

impl EdgeMatching {
    fn new(matching: Vec<Edge>) -> EdgeMatching {
        EdgeMatching { matching }
    }

    fn is_empty(&self) -> bool {
        self.matching.is_empty()
    }
}

impl Encode for EdgeMatching {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.matching.write_to(writer)
    }
}

impl Decode for EdgeMatching {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let matching = <Vec<Edge>>::read_from(reader)?;
        Ok(EdgeMatching { matching })
    }
}

// The EdgeMatchings starting from the same vertex during the intersection.
// e.g., from a previously matched edge [a1->b1], when expanding `EdgeExpand`s from vertices a1,
// we have EdgeMatchings of [a1->c1, a1->c2, a1->c3]
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Default)]
struct EdgeMatchings {
    matchings: Vec<EdgeMatching>,
}

impl EdgeMatchings {
    fn with_capacity(capacity: usize) -> EdgeMatchings {
        EdgeMatchings { matchings: vec![EdgeMatching::default(); capacity] }
    }

    fn new(matchings: Vec<EdgeMatching>) -> EdgeMatchings {
        EdgeMatchings { matchings }
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.matchings.swap(a, b);
    }

    fn drain<R>(&mut self, range: R)
    where
        R: RangeBounds<usize>,
    {
        self.matchings.drain(range);
    }
}

impl Encode for EdgeMatchings {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.matchings.write_to(writer)
    }
}

impl Decode for EdgeMatchings {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let matchings = <Vec<EdgeMatching>>::read_from(reader)?;
        Ok(EdgeMatchings { matchings })
    }
}

/// A more general entry implementation for intersection, which preserves the matchings of intermediate expanded edges if needed;
///
/// For example, given a triangle pattern [A->B->C<-A],
/// The plan consists of an EdgeExpand{tag: A, alias: B}, and then a Intersection {EdgeExpand{tag: A, alias:C, edge_alias: TagA}, EdgeExpand{tag: B, alias: C, edge_alias: TagB}}
/// Then during execution, based on one matching [a1->b1] of Pattern [A->B], we apply the IntersectionOpr, and save the intermediate results in `GeneralIntersectionEntry`
/// Specifically,
/// 1. To match Expand[A->C], we have `EdgeMatchings` of [a1->c1, a1->c2, a1->c3, a1->c4], saved in `edge_vecs`.
/// and then the intersected vertices in `vertex_vec` is [c1,c2,c3, c4], i.e., matchings of vertex C.
/// A more complicated case is [(a1-knows->c1, a1-family->c1), a1->c2, a1->c3, a1->c4], where the two a1->c1 edges with different types is saved as `EdgeMatching`,
/// and in this case, the intersected vertices is still [c1,c2,c3, c4], but the count_vec would be marked as [2, 1, 1, 1] (2 c1, 1 c2, etc.).
/// Moreover, we save the edge_alias of this EdgeExpand in `edge_tags`, i.e., [TagA]
///
/// 2. Then to match Expand[B->C], we find matchings [b1->c1, b1->c2, b1->c3]. Thus, we updated GeneralIntersectionEntry as:
/// `vertex_vec` to save the intersected vertex as [c1,c2,c3], where c4 is filtered. And `count_vec` would be updated accordingly.
/// `edge_vecs` to save the matchings of the expanded edges as
/// [
///  [a1->c1, a1->c2, a1->c3]
///  [b1->c1, b1->c2, b1->c3]
/// ]
/// , where the a1->c4 is filtered.
/// and `edge_tags` is [TagA, TagB].
///
/// 3. Finally, we can apply the `matchings_iter` function, to flatten the GeneralIntersectionEntry into a series of matchings, in a Record-like format.
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub struct GeneralIntersectionEntry {
    // Preserves the common intersected vertices, e.g., [c1,c2,c3]
    vertex_vec: Vec<ID>,
    // Preserves the EdgeMatchings during the intersection.
    // Each entry is one EdgeMatchings, that is, the adjacent edges targeting to each vertex in the vertex_vec (corresponding to the same index).
    // e.g., from a previously matched edge [a1->b1], to intersect two `EdgeExpand`s from a1 and b1 respectively, we may have results:
    // [
    //  [a1->c1, a1->c2, a1->c3]
    //  [b1->c1, b1->c2, b1->c3]
    // ]
    edge_vecs: Vec<EdgeMatchings>,
    // A list of tags, each tags the results of one EdgeExpand during the intersection.
    // e.g., for two `EdgeExpands`, we may have tags of [TagA, TagB],
    // then, edge matchings a1->c1, a1->c2, a1->c3 has tag of TagA, and b1->c1, b1->c2, b1->c3 has tag of TagB.
    edge_tags: Vec<KeyId>,
    // the number of matchings for each intersected vertices
    count_vec: Vec<u32>,
}

impl_as_any!(GeneralIntersectionEntry);

impl GeneralIntersectionEntry {
    pub fn from_iter<I: Iterator<Item = ID>>(iter: I) -> GeneralIntersectionEntry {
        let mut vertex_count_map = BTreeMap::new();
        for vertex in iter {
            let cnt = vertex_count_map.entry(vertex).or_insert(0);
            *cnt += 1;
        }
        let mut vertex_vec = Vec::with_capacity(vertex_count_map.len());
        let mut count_vec = Vec::with_capacity(vertex_count_map.len());
        for (vertex, cnt) in vertex_count_map.into_iter() {
            vertex_vec.push(vertex);
            count_vec.push(cnt);
        }
        GeneralIntersectionEntry {
            vertex_vec,
            edge_vecs: Vec::with_capacity(0),
            edge_tags: Vec::with_capacity(0),
            count_vec,
        }
    }

    pub fn from_edge_iter<I: Iterator<Item = Edge>>(iter: I, edge_tag: KeyId) -> GeneralIntersectionEntry {
        let mut vertex_edge_map = BTreeMap::new();
        for edge in iter {
            let vertex = edge.get_other_id();
            let edges = vertex_edge_map
                .entry(vertex)
                .or_insert_with(Vec::new);
            edges.push(edge);
        }
        let mut vertex_vec = Vec::with_capacity(vertex_edge_map.len());
        let mut count_vec = Vec::with_capacity(vertex_edge_map.len());
        let mut edge_vec = Vec::with_capacity(vertex_edge_map.len());
        for (vertex, edges) in vertex_edge_map.into_iter() {
            vertex_vec.push(vertex);
            count_vec.push(edges.len() as u32);
            edge_vec.push(EdgeMatching::new(edges));
        }
        GeneralIntersectionEntry {
            vertex_vec,
            edge_vecs: vec![EdgeMatchings::new(edge_vec)],
            edge_tags: vec![edge_tag],
            count_vec,
        }
    }

    // intersect ids indicates no edges need to be preserved, just intersect on the target vertex ids
    fn intersect<Iter: Iterator<Item = ID>>(&mut self, seeker: Iter) {
        let len = self.vertex_vec.len();
        let mut s = vec![0; len];
        for vid in seeker {
            if let Ok(idx) = self
                .vertex_vec
                .binary_search_by(|e| e.cmp(&vid))
            {
                s[idx] += 1;
            }
        }
        let mut idx = 0;
        for (i, cnt) in s.into_iter().enumerate() {
            if cnt != 0 {
                self.vertex_vec.swap(idx, i);
                self.count_vec.swap(idx, i);
                for edge_vec in self.edge_vecs.iter_mut() {
                    edge_vec.swap(idx, i);
                }
                self.count_vec[idx] *= cnt;
                idx += 1;
            }
        }
        self.vertex_vec.drain(idx..);
        self.count_vec.drain(idx..);
        for edge_vec in self.edge_vecs.iter_mut() {
            edge_vec.drain(idx..);
        }
    }

    // intersect edges indicates to intersect on the target vertex ids, while preserving the expanded edges
    fn general_intersect<Iter: Iterator<Item = Edge>>(&mut self, seeker: Iter, edge_tag: KeyId) {
        let len = self.vertex_vec.len();
        let mut e = vec![Vec::new(); len];
        for edge in seeker {
            let vid = edge.get_other_id();
            if let Ok(idx) = self
                .vertex_vec
                .binary_search_by(|e| e.cmp(&vid))
            {
                e[idx].push(edge);
            }
        }
        let mut idx = 0;
        let mut expanded_edge_matchings = EdgeMatchings::with_capacity(len);
        for (i, edges) in e.into_iter().enumerate() {
            let cnt = edges.len() as u32;
            if cnt != 0 {
                self.vertex_vec.swap(idx, i);
                self.count_vec.swap(idx, i);
                for edge_vec in self.edge_vecs.iter_mut() {
                    edge_vec.swap(idx, i);
                }
                self.count_vec[idx] *= cnt;
                expanded_edge_matchings.matchings[idx] = EdgeMatching::new(edges);
                idx += 1;
            }
        }
        self.vertex_vec.drain(idx..);
        self.count_vec.drain(idx..);
        for edge_vec in self.edge_vecs.iter_mut() {
            edge_vec.drain(idx..);
        }
        expanded_edge_matchings.matchings.drain(idx..);
        if !expanded_edge_matchings.matchings.is_empty() {
            self.edge_vecs.push(expanded_edge_matchings);
            self.edge_tags.push(edge_tag);
        }
    }

    fn is_empty(&self) -> bool {
        self.vertex_vec.is_empty()
    }

    fn len(&self) -> usize {
        let mut len = 0;
        for count in self.count_vec.iter() {
            len += *count;
        }
        len as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = &ID> {
        self.vertex_vec
            .iter()
            .zip(&self.count_vec)
            .flat_map(move |(vertex, count)| std::iter::repeat(vertex).take(*count as usize))
    }

    // output the results of matchings in the intersection.
    // e.g., edge_vecs preserves
    // [
    //  [a1->c1, a1->c2, a1->c3]
    //  [b1->c1, b1->c2, b1->c3]
    // ]
    // with edge_tags as [TagA, TagB],
    // then the output looks like:
    // (c1, [(a1->c1, TagA), (b1->c1, TagB)]), (c2, [(a1->c2, TagA), (b1->c2, TagB)]), (c3, [(a1->c3, TagA), (b1->c3, TagB)])
    // Here, each item corresponds to a record (a complete matching).
    pub fn matchings_iter(&self) -> impl Iterator<Item = (ID, Vec<Vec<(&Edge, KeyId)>>)> {
        if self.edge_vecs.is_empty() {
            return vec![].into_iter();
        }
        let mut result = Vec::with_capacity(self.vertex_vec.len());
        for i in 0..self.edge_vecs[0].matchings.len() {
            if self.edge_vecs[0].matchings[i].is_empty() {
                warn!(
                    "The {}-th entry of {:?} is empty in intersection, should be erased",
                    i, self.edge_vecs[0]
                );
                continue;
            }
            // the target vertex id
            let dst = self.edge_vecs[0].matchings[i].matching[0].get_other_id();
            // the records with target dst consists of columns of TagA, TagB, ..., which is a cartesian product of all these tags
            let product = (0..self.edge_vecs.len())
                .map(|tag_idx| &self.edge_vecs[tag_idx].matchings[i].matching)
                .multi_cartesian_product();
            let records_num: usize = (0..self.edge_vecs.len())
                .map(|tag_index| {
                    self.edge_vecs[tag_index].matchings[i]
                        .matching
                        .len()
                })
                .product();
            let mut records = Vec::with_capacity(records_num);
            // each combination can be regarded as multiple columns in record (with no alias, so we need to zip it).
            for combination in product {
                let record: Vec<_> = combination
                    .into_iter()
                    .zip(self.edge_tags.iter().cloned())
                    .collect();
                records.push(record);
            }
            result.push((dst, records));
        }
        return result.into_iter();
    }
}

impl Encode for GeneralIntersectionEntry {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.vertex_vec.write_to(writer)?;
        self.edge_vecs.write_to(writer)?;
        self.edge_tags.write_to(writer)?;
        self.count_vec.write_to(writer)?;
        Ok(())
    }
}

impl Decode for GeneralIntersectionEntry {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vertex_vec = <Vec<ID>>::read_from(reader)?;
        let edge_vecs = <Vec<EdgeMatchings>>::read_from(reader)?;
        let edge_tags = <Vec<KeyId>>::read_from(reader)?;
        let count_vec = <Vec<u32>>::read_from(reader)?;
        Ok(GeneralIntersectionEntry { vertex_vec, edge_vecs, edge_tags, count_vec })
    }
}

impl Element for GeneralIntersectionEntry {
    fn len(&self) -> usize {
        self.len()
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

// a more general version of ExpandOrIntersect operator
// to expand neighbors and intersect with the ones of the same tag found previously (if exists).
// If edge_tag (the alias of expanded edges) is specified, the intermediate expanded edges will also be preserved.
// Notice that do not mix the usage of GeneralExpandOrIntersect and ExpandOrIntersect:
// if during the intersection, all the expanded edges are not needed, use the optimized version of ExpandOrIntersect,
// otherwise, use GeneralExpandOrIntersect.
pub struct GeneralExpandOrIntersect {
    start_v_tag: Option<KeyId>,
    end_v_tag: KeyId,
    edge_tag: Option<KeyId>,
    stmt: Box<dyn Statement<ID, Edge>>,
}

impl FilterMapFunction<Record, Record> for GeneralExpandOrIntersect {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input.get(self.start_v_tag).ok_or_else(|| {
            FnExecError::get_tag_error(&format!(
                "get start_v_tag {:?} from record in `ExpandOrIntersect` operator, the record is {:?}",
                self.start_v_tag, input
            ))
        })?;
        match entry.get_type() {
            EntryType::Vertex => {
                let id = entry.id();
                let edge_iter = self.stmt.exec(id)?;
                if let Some(pre_entry) = input.get_mut(Some(self.end_v_tag)) {
                    // the case of expansion and intersection
                    let pre_intersection = pre_entry
                        .as_any_mut()
                        .downcast_mut::<GeneralIntersectionEntry>()
                        .ok_or_else(|| {
                            FnExecError::unexpected_data_error(&format!(
                                "entry  is not a intersection in ExpandOrIntersect"
                            ))
                        })?;
                    if let Some(edge_tag) = self.edge_tag {
                        pre_intersection.general_intersect(edge_iter, edge_tag);
                    } else {
                        pre_intersection.intersect(edge_iter.map(|e| e.get_other_id()));
                    }
                    if pre_intersection.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(input))
                    }
                } else {
                    // the case of expansion only
                    let neighbors_intersection = if let Some(edge_tag) = self.edge_tag {
                        GeneralIntersectionEntry::from_edge_iter(edge_iter, edge_tag)
                    } else {
                        GeneralIntersectionEntry::from_iter(edge_iter.map(|e| e.get_other_id()))
                    };
                    if neighbors_intersection.is_empty() {
                        Ok(None)
                    } else {
                        // append columns without changing head
                        let columns = input.get_columns_mut();
                        columns.insert(self.end_v_tag as usize, DynEntry::new(neighbors_intersection));
                        Ok(Some(input))
                    }
                }
            }
            _ => Err(FnExecError::unsupported_error(&format!(
                "expand or intersect entry {:?} of tag {:?} failed in ExpandOrIntersect",
                entry, self.end_v_tag
            )))?,
        }
    }
}

impl FilterMapFuncGen for (pb::EdgeExpand, Option<pb::GetV>) {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        if self.1.is_none() && self.0.expand_opt != pb::edge_expand::ExpandOpt::Vertex as i32 {
            return Err(FnGenError::unsupported_error(&format!(
                "GeneralExpandOrIntersect with {:?}",
                self
            )));
        }
        let graph = graph_proxy::apis::get_graph().ok_or_else(|| FnGenError::NullGraphError)?;
        let start_v_tag = self.0.v_tag;
        let edge_tag = self.0.alias;
        let end_v_tag = if let Some(getv) = self.1 { getv.alias } else { self.0.alias }
            .ok_or_else(|| ParsePbError::from("`GetV::alias` cannot be empty for intersection"))?;
        let direction_pb: pb::edge_expand::Direction = unsafe { ::std::mem::transmute(self.0.direction) };
        let direction = Direction::from(direction_pb);
        let query_params: QueryParams = self.0.params.try_into()?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!(
                "Runtime expand collection operator of edge with start_v_tag {:?}, edge_tag {:?}, end_v_tag {:?}, direction {:?}, query_params {:?}",
                start_v_tag, edge_tag, end_v_tag, direction, query_params
            );
        }

        // Expand edges, since we need to preserve the edge information
        let stmt = graph.prepare_explore_edge(direction, &query_params)?;
        let edge_expand_operator = GeneralExpandOrIntersect { start_v_tag, edge_tag, end_v_tag, stmt };
        Ok(Box::new(edge_expand_operator))
    }
}

#[cfg(test)]
mod tests {
    use graph_proxy::apis::{Edge, ID};
    use ir_common::KeyId;

    use super::IntersectionEntry;
    use crate::process::operator::map::GeneralIntersectionEntry;

    const EDGE_TAG_A: KeyId = 0;
    const EDGE_TAG_B: KeyId = 1;

    fn to_vertex_iter(id_vec: Vec<ID>) -> impl Iterator<Item = ID> {
        id_vec.into_iter()
    }

    fn to_edge_iter(eid_vec: Vec<(ID, ID)>) -> impl Iterator<Item = Edge> {
        eid_vec
            .into_iter()
            .map(|(src, dst)| Edge::new(0, None, src, dst, Default::default()))
    }

    #[test]
    fn intersect_test_01() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 2, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 2, 3])
    }

    #[test]
    fn intersect_test_02() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![3, 2, 1]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 2, 3])
    }

    #[test]
    fn intersect_test_03() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![9, 7, 5, 3, 1]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 3, 5])
    }

    #[test]
    fn intersect_test_04() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![9, 8, 7, 6]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), Vec::<ID>::new())
    }

    #[test]
    fn intersect_test_05() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5, 1]));
        let seeker = to_vertex_iter(vec![1, 2, 3]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 1, 2, 3])
    }

    #[test]
    fn intersect_test_06() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 2, 3, 4, 5, 1]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 1, 2, 3])
    }

    #[test]
    fn intersect_test_07() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![1, 2, 3]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 1, 2, 2, 3, 3])
    }

    #[test]
    fn intersect_test_08() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 1, 2, 2, 3, 3])
    }

    #[test]
    fn intersect_test_09() {
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 1, 2, 2, 3, 3]));
        let seeker = to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3])
    }

    fn general_intersect_test(iter1: Vec<(ID, ID)>, iter2: Vec<(ID, ID)>) -> Vec<Vec<(ID, ID, KeyId)>> {
        let mut intersection = GeneralIntersectionEntry::from_edge_iter(to_edge_iter(iter1), EDGE_TAG_A);
        println!("intersection: {:?}", intersection);
        let seeker = to_edge_iter(iter2);
        intersection.general_intersect(seeker, EDGE_TAG_B);
        println!("intersection: {:?}", intersection);
        let mut records = vec![];
        let matchings_collect = intersection.matchings_iter();
        for (vid, matchings) in matchings_collect {
            for matching in matchings {
                let mut record = vec![];
                for (edge, tag) in matching {
                    assert_eq!(vid, edge.get_other_id() as ID);
                    record.push((edge.src_id, edge.dst_id, tag));
                }
                records.push(record);
            }
        }
        records.sort();
        records
    }

    #[test]
    fn general_intersect_test_01() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 2), (0, 3)],
                vec![(10, 1), (10, 2), (10, 3), (10, 4), (10, 5)]
            ),
            vec![
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)]
            ]
        );
    }

    #[test]
    fn general_intersect_test_02() {
        assert_eq!(
            general_intersect_test(
                vec![(10, 1), (10, 2), (10, 3), (10, 4), (10, 5)],
                vec![(0, 3), (0, 2), (0, 1)]
            ),
            vec![
                vec![(10, 1, EDGE_TAG_A), (0, 1, EDGE_TAG_B)],
                vec![(10, 2, EDGE_TAG_A), (0, 2, EDGE_TAG_B)],
                vec![(10, 3, EDGE_TAG_A), (0, 3, EDGE_TAG_B)]
            ]
        );
    }

    #[test]
    fn general_intersect_test_03() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 2), (0, 3), (0, 4), (0, 5)],
                vec![(10, 9), (10, 7), (10, 5), (10, 3), (10, 1)]
            ),
            vec![
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(0, 5, EDGE_TAG_A), (10, 5, EDGE_TAG_B)]
            ]
        );
    }

    #[test]
    fn general_intersect_test_04() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 2), (0, 3), (0, 4), (0, 5)],
                vec![(0, 9), (0, 8), (0, 7), (0, 6)]
            )
            .len(),
            0
        );
    }

    #[test]
    fn general_intersect_test_05() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (6, 1)],
                vec![(10, 1), (10, 2), (10, 3)]
            ),
            vec![
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(6, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
            ]
        );
    }

    #[test]
    fn general_intersect_test_06() {
        assert_eq!(
            general_intersect_test(
                vec![(10, 1), (10, 2), (10, 3)],
                vec![(0, 1), (0, 1), (0, 2), (0, 3), (0, 4), (0, 5)]
            ),
            vec![
                vec![(10, 1, EDGE_TAG_A), (0, 1, EDGE_TAG_B)],
                vec![(10, 1, EDGE_TAG_A), (0, 1, EDGE_TAG_B)],
                vec![(10, 2, EDGE_TAG_A), (0, 2, EDGE_TAG_B)],
                vec![(10, 3, EDGE_TAG_A), (0, 3, EDGE_TAG_B)]
            ]
        );
    }

    #[test]
    fn general_intersect_test_07() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 1), (0, 2), (0, 2), (0, 3), (0, 3), (0, 4), (0, 5)],
                vec![(10, 1), (10, 2), (10, 3)]
            ),
            vec![
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)]
            ]
        );
    }

    #[test]
    fn general_intersect_test_08() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 2), (0, 3)],
                vec![(10, 1), (10, 1), (10, 2), (10, 2), (10, 3), (10, 3), (10, 4), (10, 5)]
            ),
            vec![
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)]
            ]
        );
        let mut intersection = IntersectionEntry::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(intersection.drain().collect::<Vec<ID>>(), vec![1, 1, 2, 2, 3, 3])
    }

    #[test]
    fn general_intersect_test_09() {
        assert_eq!(
            general_intersect_test(
                vec![(0, 1), (0, 1), (0, 2), (0, 2), (0, 3), (0, 3)],
                vec![(10, 1), (10, 1), (10, 2), (10, 2), (10, 3), (10, 3), (10, 4), (10, 5)]
            ),
            vec![
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 1, EDGE_TAG_A), (10, 1, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 2, EDGE_TAG_A), (10, 2, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)],
                vec![(0, 3, EDGE_TAG_A), (10, 3, EDGE_TAG_B)]
            ]
        );
    }
}
