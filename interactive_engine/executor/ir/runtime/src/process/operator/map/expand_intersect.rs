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

/// A more general entry implementation for intersection, which preserves the expanded edges if needed;
#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub struct GeneralIntersectionEntry {
    vertex_vec: Vec<ID>,
    // A list of results output by the EdgeExpand in the intersection.
    // Each entry is a Vec<Vec<Edge>>, that is, the adjacent edges targeting to each vertex in the vertex_vec (corresponding to the same index).
    // Note that each entry would be tagged, and the tags are stored in edge_tags.
    edge_vecs: Vec<Vec<Vec<Edge>>>,
    edge_tags: Vec<KeyId>,
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
            edge_vec.push(edges);
        }
        GeneralIntersectionEntry {
            vertex_vec,
            edge_vecs: vec![edge_vec],
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
        let mut new_edge_vec = vec![Vec::new(); len];
        for (i, edges) in e.into_iter().enumerate() {
            let cnt = edges.len() as u32;
            if cnt != 0 {
                self.vertex_vec.swap(idx, i);
                self.count_vec.swap(idx, i);
                for edge_vec in self.edge_vecs.iter_mut() {
                    edge_vec.swap(idx, i);
                }
                self.count_vec[idx] *= cnt;
                new_edge_vec[idx] = edges;
                idx += 1;
            }
        }
        self.vertex_vec.drain(idx..);
        self.count_vec.drain(idx..);
        for edge_vec in self.edge_vecs.iter_mut() {
            edge_vec.drain(idx..);
        }
        new_edge_vec.drain(idx..);
        if !new_edge_vec.is_empty() {
            self.edge_vecs.push(new_edge_vec);
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

    // output the results of matchings in the intersection, which is a vec of (ID, Vec<Vec<(&Edge, KeyId)>>),
    // that ID is the target vertex id, and Vec<Vec<(&Edge, KeyId)>> is the matched edges (with aliases) adjacent to the target vertex.
    // Specifically, each matching is a vec of (Edge, KeyId) pairs, denoting the columns in a record.
    pub fn matchings_iter(&self) -> impl Iterator<Item = (ID, Vec<Vec<(&Edge, KeyId)>>)> {
        if self.edge_vecs.is_empty() {
            return vec![].into_iter();
        }
        let mut result = Vec::with_capacity(self.vertex_vec.len());
        for i in 0..self.edge_vecs[0].len() {
            if self.edge_vecs[0][i].len() == 0 {
                warn!("Empty edge_vecs[0][{}], should be erased", i);
                continue;
            }
            // the target vertex id
            let dst = self.edge_vecs[0][i][0].get_other_id();
            // the records with target dst consists of columns of TagA, TagB, ..., which is a cartesian product of all these tags
            let product = (0..self.edge_vecs.len())
                .map(|tag_idx| &self.edge_vecs[tag_idx][i])
                .multi_cartesian_product();
            let records_num: usize = (0..self.edge_vecs.len())
                .map(|tag_index| self.edge_vecs[tag_index][i].len())
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

    pub fn drain(&mut self) -> impl Iterator<Item = ID> + '_ {
        self.vertex_vec
            .drain(..)
            .zip(&self.count_vec)
            .flat_map(move |(vertex, count)| std::iter::repeat(vertex).take(*count as usize))
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
        let edge_vecs = <Vec<Vec<Vec<Edge>>>>::read_from(reader)?;
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
        let seeker = to_edge_iter(iter2);
        intersection.general_intersect(seeker, EDGE_TAG_B);
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
