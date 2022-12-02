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
use graph_proxy::apis::{Direction, DynDetails, Edge, Element, QueryParams, Statement, Vertex, ID};
use ir_common::error::ParsePbError;
use ir_common::generated::algebra as algebra_pb;
use ir_common::KeyId;
use pegasus::api::function::{FilterMapFunction, FnResult};
use pegasus::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::error::{FnExecError, FnGenError, FnGenResult};
use crate::process::entry::{DynEntry, Entry};
use crate::process::operator::map::FilterMapFuncGen;
use crate::process::record::Record;

/// An ExpandOrIntersect operator to expand neighbors
/// and intersect with the ones of the same tag found previously (if exists).
/// Notice that edge_or_end_v_tag (the alias of expanded neighbors) must be specified.
struct ExpandVertexOrIntersect {
    start_v_tag: Option<KeyId>,
    edge_or_end_v_tag: KeyId,
    stmt: Box<dyn Statement<ID, Vertex>>,
}

struct ExpandEdgeVOrIntersect {
    start_v_tag: Option<KeyId>,
    edge_or_end_v_tag: KeyId,
    stmt: Box<dyn Statement<ID, Edge>>,
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd)]
pub struct Intersection {
    vertex_vec: Vec<Vertex>,
    count_vec: Vec<u32>,
}

impl_as_any!(Intersection);

impl Intersection {
    pub fn from_iter<I: Iterator<Item = Vertex>>(iter: I) -> Intersection {
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
        Intersection { vertex_vec, count_vec }
    }

    fn intersect<Iter: Iterator<Item = Vertex>>(&mut self, seeker: Iter) {
        let len = self.vertex_vec.len();
        let mut s = vec![0; len];
        for item in seeker {
            let vid = item.id();
            if let Ok(idx) = self
                .vertex_vec
                .binary_search_by(|e| e.id().cmp(&vid))
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

    pub fn is_empty(&self) -> bool {
        self.vertex_vec.is_empty()
    }

    pub fn len(&self) -> usize {
        let mut len = 0;
        for count in self.count_vec.iter() {
            len += *count;
        }
        len as usize
    }

    pub fn iter(&self) -> impl Iterator<Item = &Vertex> {
        self.vertex_vec
            .iter()
            .zip(&self.count_vec)
            .flat_map(move |(vertex, count)| std::iter::repeat(vertex).take(*count as usize))
    }

    pub fn drain(&mut self) -> impl Iterator<Item = Vertex> + '_ {
        self.vertex_vec
            .drain(..)
            .zip(&self.count_vec)
            .flat_map(move |(vertex, count)| std::iter::repeat(vertex).take(*count as usize))
    }
}

impl Encode for Intersection {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> std::io::Result<()> {
        self.vertex_vec.write_to(writer)?;
        self.count_vec.write_to(writer)?;
        Ok(())
    }
}

impl Decode for Intersection {
    fn read_from<R: ReadExt>(reader: &mut R) -> std::io::Result<Self> {
        let vertex_vec = <Vec<Vertex>>::read_from(reader)?;
        let count_vec = <Vec<u32>>::read_from(reader)?;
        Ok(Intersection { vertex_vec, count_vec })
    }
}

impl Element for Intersection {
    fn len(&self) -> usize {
        self.len()
    }

    fn as_borrow_object(&self) -> BorrowObject {
        BorrowObject::None
    }
}

impl FilterMapFunction<Record, Record> for ExpandVertexOrIntersect {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input
            .get(self.start_v_tag)
            .ok_or(FnExecError::get_tag_error(&format!(
                "get start_v_tag {:?} from record in `ExpandOrIntersect` operator, the record is {:?}",
                self.start_v_tag, input
            )))?;
        if let Some(id) = entry.as_id() {
            let iter = self.stmt.exec(id)?;
            if let Some(pre_entry) = input.get_column_mut(&self.edge_or_end_v_tag) {
                // the case of expansion and intersection
                let pre_intersection = pre_entry
                    .as_any_mut()
                    .downcast_mut::<Intersection>()
                    .ok_or(FnExecError::unexpected_data_error(&format!(
                        "entry  is not a intersection in ExpandOrIntersect"
                    )))?;
                pre_intersection.intersect(iter);
                if pre_intersection.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(input))
                }
            } else {
                // the case of expansion only
                let neighbors_intersection = Intersection::from_iter(iter);
                if neighbors_intersection.is_empty() {
                    Ok(None)
                } else {
                    // append columns without changing head
                    let columns = input.get_columns_mut();
                    columns.insert(self.edge_or_end_v_tag as usize, DynEntry::new(neighbors_intersection));
                    Ok(Some(input))
                }
            }
        } else {
            Err(FnExecError::unsupported_error(&format!(
                "expand or intersect entry {:?} of tag {:?} failed in ExpandOrIntersect",
                entry, self.edge_or_end_v_tag
            )))?
        }
    }
}

impl FilterMapFunction<Record, Record> for ExpandEdgeVOrIntersect {
    fn exec(&self, mut input: Record) -> FnResult<Option<Record>> {
        let entry = input
            .get(self.start_v_tag)
            .ok_or(FnExecError::get_tag_error(&format!(
                "get start_v_tag {:?} from record in `ExpandOrIntersect` operator, the record is {:?}",
                self.start_v_tag, input
            )))?;
        if let Some(id) = entry.as_id() {
            let iter = self
                .stmt
                .exec(id)?
                .map(|e| Vertex::new(e.get_other_id(), None, DynDetails::default()));
            if let Some(pre_entry) = input.get_column_mut(&self.edge_or_end_v_tag) {
                // the case of expansion and intersection
                let pre_intersection = pre_entry
                    .as_any_mut()
                    .downcast_mut::<Intersection>()
                    .ok_or(FnExecError::unexpected_data_error(&format!(
                        "entry  is not a intersection in ExpandOrIntersect"
                    )))?;
                pre_intersection.intersect(iter);
                if pre_intersection.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(input))
                }
            } else {
                // the case of expansion only
                let neighbors_intersection = Intersection::from_iter(iter);
                if neighbors_intersection.is_empty() {
                    Ok(None)
                } else {
                    // append columns without changing head
                    let columns = input.get_columns_mut();
                    columns.insert(self.edge_or_end_v_tag as usize, DynEntry::new(neighbors_intersection));
                    Ok(Some(input))
                }
            }
        } else {
            Err(FnExecError::unsupported_error(&format!(
                "expand or intersect entry {:?} of tag {:?} failed in ExpandOrIntersect",
                entry, self.edge_or_end_v_tag
            )))?
        }
    }
}

impl FilterMapFuncGen for algebra_pb::EdgeExpand {
    fn gen_filter_map(self) -> FnGenResult<Box<dyn FilterMapFunction<Record, Record>>> {
        let graph = graph_proxy::apis::get_graph().ok_or(FnGenError::NullGraphError)?;
        let start_v_tag = self
            .v_tag
            .map(|tag| tag.try_into())
            .transpose()?;
        let edge_or_end_v_tag = self
            .alias
            .ok_or(ParsePbError::from("`EdgeExpand::alias` cannot be empty for intersection"))?
            .try_into()?;
        let direction_pb: algebra_pb::edge_expand::Direction =
            unsafe { ::std::mem::transmute(self.direction) };
        let direction = Direction::from(direction_pb);
        let query_params: QueryParams = self.params.try_into()?;
        if log_enabled!(log::Level::Debug) && pegasus::get_current_worker().index == 0 {
            debug!(
                "Runtime expand collection operator of edge with start_v_tag {:?}, end_tag {:?}, direction {:?}, query_params {:?}",
                start_v_tag, edge_or_end_v_tag, direction, query_params
            );
        }
        if self.expand_opt != algebra_pb::edge_expand::ExpandOpt::Vertex as i32 {
            Err(FnGenError::unsupported_error("expand edges in ExpandIntersection"))
        } else {
            if query_params.filter.is_some() {
                // Expand vertices with filters on edges.
                // This can be regarded as a combination of EdgeExpand (with expand_opt as Edge) + GetV
                let stmt = graph.prepare_explore_edge(direction, &query_params)?;
                let edge_expand_operator = ExpandEdgeVOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
                Ok(Box::new(edge_expand_operator))
            } else {
                // Expand vertices without any filters
                let stmt = graph.prepare_explore_vertex(direction, &query_params)?;
                let edge_expand_operator = ExpandVertexOrIntersect { start_v_tag, edge_or_end_v_tag, stmt };
                Ok(Box::new(edge_expand_operator))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use graph_proxy::apis::{GraphElement, Vertex, ID};

    use super::Intersection;

    fn to_vertex_iter(id_vec: Vec<ID>) -> impl Iterator<Item = Vertex> {
        id_vec.into_iter().map(|id| id.into())
    }

    #[test]
    fn intersect_test_01() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 2, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_02() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![3, 2, 1]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_03() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![9, 7, 5, 3, 1]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 3, 5]
        )
    }

    #[test]
    fn intersect_test_04() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![9, 8, 7, 6]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            Vec::<ID>::new()
        )
    }

    #[test]
    fn intersect_test_05() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3, 4, 5, 1]));
        let seeker = to_vertex_iter(vec![1, 2, 3]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_06() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 2, 3, 4, 5, 1]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 3]
        )
    }

    #[test]
    fn intersect_test_07() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]));
        let seeker = to_vertex_iter(vec![1, 2, 3]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 2, 3, 3]
        )
    }

    #[test]
    fn intersect_test_08() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 2, 3]));
        let seeker = to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 2, 2, 3, 3]
        )
    }

    #[test]
    fn intersect_test_09() {
        let mut intersection = Intersection::from_iter(to_vertex_iter(vec![1, 1, 2, 2, 3, 3]));
        let seeker = to_vertex_iter(vec![1, 1, 2, 2, 3, 3, 4, 5]);
        intersection.intersect(seeker);
        assert_eq!(
            intersection
                .iter()
                .map(|vertex| vertex.id())
                .collect::<Vec<ID>>(),
            vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]
        )
    }
}
