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

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::iter::Iterator;

use ir_common::generated::algebra as pb;
use serde::{Deserialize, Serialize};

use crate::catalogue::pattern::Pattern;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternLabelId, PatternRankId};
use crate::error::{IrError, IrResult};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtendEdge {
    src_vertex_rank: PatternRankId,
    edge_label: PatternLabelId,
    dir: PatternDirection,
}

/// Initializer of ExtendEdge
impl ExtendEdge {
    pub fn new(src_vertex_rank: usize, edge_label: PatternLabelId, dir: PatternDirection) -> ExtendEdge {
        ExtendEdge { src_vertex_rank, edge_label, dir }
    }
}

/// Methods for access fields of VagueExtendEdge
impl ExtendEdge {
    #[inline]
    pub fn get_src_vertex_rank(&self) -> PatternRankId {
        self.src_vertex_rank
    }

    #[inline]
    pub fn get_edge_label(&self) -> PatternLabelId {
        self.edge_label
    }

    #[inline]
    pub fn get_direction(&self) -> PatternDirection {
        self.dir
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtendStep {
    target_vertex_label: PatternLabelId,
    extend_edges: Vec<ExtendEdge>,
}

/// Initializer of ExtendStep
impl ExtendStep {
    /// Initialization of a ExtendStep needs
    /// 1. a target vertex label
    /// 2. all extend edges connect to the target vertex label
    pub fn new(target_vertex_label: PatternLabelId, extend_edges: Vec<ExtendEdge>) -> ExtendStep {
        ExtendStep { target_vertex_label, extend_edges }
    }
}

/// Methods for access fields or get info from ExtendStep
impl ExtendStep {
    /// For the iteration over all the extend edges
    pub fn iter(&self) -> DynIter<&ExtendEdge> {
        Box::new(self.extend_edges.iter())
    }

    #[inline]
    pub fn get_target_vertex_label(&self) -> PatternLabelId {
        self.target_vertex_label
    }

    #[inline]
    pub fn get_extend_edges_num(&self) -> usize {
        self.extend_edges.len()
    }

    #[allow(dead_code)]
    pub(crate) fn sort_extend_edges<F>(&mut self, compare: F)
    where
        F: FnMut(&ExtendEdge, &ExtendEdge) -> Ordering,
    {
        self.extend_edges.sort_by(compare)
    }
}

/// Given a DefiniteExtendEdge, we can uniquely locate an edge with dir in the pattern
#[derive(Debug, Clone)]
pub struct DefiniteExtendEdge {
    src_vertex_id: PatternId,
    edge_id: PatternId,
    edge_label: PatternLabelId,
    dir: PatternDirection,
}

/// Initializer of DefiniteExtendEdge
impl DefiniteExtendEdge {
    pub fn new(
        src_vertex_id: PatternId, edge_id: PatternId, edge_label: PatternLabelId, dir: PatternDirection,
    ) -> DefiniteExtendEdge {
        DefiniteExtendEdge { src_vertex_id, edge_id, edge_label, dir }
    }

    pub fn from_extend_edge(extend_edge: &ExtendEdge, pattern: &Pattern) -> Option<DefiniteExtendEdge> {
        if let Some(src_vertex_id) = pattern
            .get_vertex_from_rank(extend_edge.get_src_vertex_rank())
            .map(|src_vertex| src_vertex.get_id())
        {
            let edge_id = pattern.get_max_edge_id() + 1;
            let edge_label = extend_edge.get_edge_label();
            let dir = extend_edge.get_direction();
            Some(DefiniteExtendEdge { src_vertex_id, edge_id, edge_label, dir })
        } else {
            None
        }
    }
}

impl DefiniteExtendEdge {
    pub fn get_src_vertex_id(&self) -> PatternId {
        self.src_vertex_id
    }

    pub fn get_edge_id(&self) -> PatternId {
        self.edge_id
    }

    pub fn get_edge_label(&self) -> PatternLabelId {
        self.edge_label
    }

    pub fn get_direction(&self) -> PatternDirection {
        self.dir
    }
}

/// Given a DefiniteExtendStep, we can uniquely find which part of the pattern to extend
#[derive(Debug, Clone)]
pub struct DefiniteExtendStep {
    target_vertex_id: PatternId,
    target_vertex_label: PatternLabelId,
    extend_edges: Vec<DefiniteExtendEdge>,
}

/// Transform a one-vertex pattern to DefiniteExtendStep
/// It is usually to use such DefiniteExtendStep to generate Source operator
impl TryFrom<Pattern> for DefiniteExtendStep {
    type Error = IrError;

    fn try_from(pattern: Pattern) -> IrResult<Self> {
        if pattern.get_vertices_num() == 1 {
            let target_vertex = pattern.vertices_iter().last().unwrap();
            let target_v_id = target_vertex.get_id();
            let target_v_label = target_vertex.get_label();
            Ok(DefiniteExtendStep {
                target_vertex_id: target_v_id,
                target_vertex_label: target_v_label,
                extend_edges: vec![],
            })
        } else {
            Err(IrError::Unsupported(
                "Can only convert pattern with one vertex to Definite Extend Step".to_string(),
            ))
        }
    }
}

impl DefiniteExtendStep {
    /// Given a target pattern with a vertex id, pick all its neighboring edges and vertices to generate a definite extend step
    pub fn from_target_pattern(target_pattern: &Pattern, target_vertex_id: PatternId) -> Option<Self> {
        if let Some(target_vertex) = target_pattern.get_vertex(target_vertex_id) {
            let target_vertex_label = target_vertex.get_label();
            let mut extend_edges = vec![];
            for adjacency in target_pattern.adjacencies_iter(target_vertex_id) {
                let edge_id = adjacency.get_edge_id();
                let dir = adjacency.get_direction();
                let edge = target_pattern.get_edge(edge_id).unwrap();
                if let PatternDirection::In = dir {
                    extend_edges.push(DefiniteExtendEdge::new(
                        edge.get_start_vertex().get_id(),
                        edge_id,
                        edge.get_label(),
                        PatternDirection::Out,
                    ));
                } else {
                    extend_edges.push(DefiniteExtendEdge::new(
                        edge.get_end_vertex().get_id(),
                        edge_id,
                        edge.get_label(),
                        PatternDirection::In,
                    ));
                }
            }
            Some(DefiniteExtendStep { target_vertex_id, target_vertex_label, extend_edges })
        } else {
            None
        }
    }

    pub fn from_src_pattern(
        src_pattern: &Pattern, extend_step: &ExtendStep, target_vertex_id: PatternId,
        edge_id_map: HashMap<(PatternId, PatternLabelId, PatternDirection), PatternId>,
    ) -> Option<Self> {
        let mut definite_extend_edges = Vec::with_capacity(extend_step.get_extend_edges_num());
        let vertex_id_to_assign = target_vertex_id;
        for extend_edge in extend_step.iter() {
            if let Some(vertex) = src_pattern.get_vertex_from_rank(extend_edge.get_src_vertex_rank()) {
                let src_vertex_label = vertex.get_label();
                let src_vertex_group = src_pattern
                    .get_vertex_group(vertex.get_id())
                    .unwrap();
                let src_vertex_candidates =
                    src_pattern.get_equivalent_vertices(src_vertex_label, src_vertex_group);
                let mut found_src_vertex = false;
                for src_vertex_candidate in src_vertex_candidates {
                    if let Some(edge_id_to_assign) = edge_id_map
                        .get(&(
                            src_vertex_candidate.get_id(),
                            extend_edge.get_edge_label(),
                            extend_edge.get_direction(),
                        ))
                        .cloned()
                    {
                        definite_extend_edges.push(DefiniteExtendEdge::new(
                            src_vertex_candidate.get_id(),
                            edge_id_to_assign,
                            extend_edge.get_edge_label(),
                            extend_edge.get_direction(),
                        ));
                        found_src_vertex = true;
                        break;
                    }
                }
                if !found_src_vertex {
                    return None;
                }
            } else {
                return None;
            }
        }
        Some(DefiniteExtendStep {
            target_vertex_id: vertex_id_to_assign,
            target_vertex_label: extend_step.get_target_vertex_label(),
            extend_edges: definite_extend_edges,
        })
    }
}

/// Methods of accessing some fields of DefiniteExtendStep
impl DefiniteExtendStep {
    #[inline]
    pub fn get_target_vertex_id(&self) -> PatternId {
        self.target_vertex_id
    }

    #[inline]
    pub fn get_target_vertex_label(&self) -> PatternLabelId {
        self.target_vertex_label
    }

    pub fn iter(&self) -> DynIter<&DefiniteExtendEdge> {
        Box::new(self.extend_edges.iter())
    }
}

impl DefiniteExtendStep {
    /// Use the DefiniteExtendStep to generate corresponding edge expand operator
    pub fn generate_expand_operators(&self, origin_pattern: &Pattern) -> Vec<pb::EdgeExpand> {
        let mut expand_operators = vec![];
        let target_v_id = self.get_target_vertex_id();
        for extend_edge in self.extend_edges.iter() {
            // pick edge's property and predicate from origin pattern
            let edge_id = extend_edge.edge_id;
            let edge_predicate = origin_pattern
                .get_edge_predicate(edge_id)
                .cloned();

            let edge_expand = pb::EdgeExpand {
                // use start vertex id as tag
                v_tag: Some((extend_edge.src_vertex_id as i32).into()),
                direction: extend_edge.dir as i32,
                params: Some(query_params(vec![extend_edge.edge_label.into()], vec![], edge_predicate)),
                // expand vertex
                expand_opt: pb::edge_expand::ExpandOpt::Vertex as i32,
                // use target vertex id as alias
                alias: Some((target_v_id as i32).into()),
            };
            expand_operators.push(edge_expand);
        }
        expand_operators
    }

    /// Generate the intersect operator for DefiniteExtendStep;s target vertex
    /// It needs its parent EdgeExpand Operator's node ids
    pub fn generate_intersect_operator(&self, parents: Vec<i32>) -> pb::Intersect {
        pb::Intersect { parents, key: Some((self.target_vertex_id as i32).into()) }
    }

    /// Generate the filter operator for DefiniteExtendStep's target vertex
    pub fn generate_vertex_filter_operator(&self, origin_pattern: &Pattern) -> Option<pb::Select> {
        // pick target vertex's property and predicate info from origin pattern
        let target_v_id = self.target_vertex_id;
        if let Some(target_v_predicate) = origin_pattern.get_vertex_predicate(target_v_id) {
            Some(pb::Select { predicate: Some(target_v_predicate.clone()) })
        } else {
            None
        }
    }
}
/// Get all the subsets of given Vec<T>
/// The algorithm is BFS
pub fn get_subsets<T, F>(origin_vec: Vec<T>, filter: F) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T, &Vec<T>) -> bool,
{
    let n = origin_vec.len();
    let mut set_collections = Vec::with_capacity((2 as usize).pow(n as u32));
    let mut queue = VecDeque::new();
    for (i, element) in origin_vec.iter().enumerate() {
        queue.push_back((vec![element.clone()], i + 1));
    }
    while let Some((subset, max_index)) = queue.pop_front() {
        set_collections.push(subset.clone());
        for i in max_index..n {
            let mut new_subset = subset.clone();
            if filter(&origin_vec[i], &subset) {
                continue;
            }
            new_subset.push(origin_vec[i].clone());
            queue.push_back((new_subset, i + 1));
        }
    }
    set_collections
}

pub(crate) fn limit_repeated_element_num<'a, T, U>(
    add_element: &'a U, subset_to_be_added: T, limit_num: usize,
) -> bool
where
    T: Iterator<Item = &'a U>,
    U: Eq,
{
    let mut repeaded_num = 0;
    for element in subset_to_be_added {
        if *add_element == *element {
            repeaded_num += 1;
            if repeaded_num >= limit_num {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::catalogue::extend_step::*;
    use crate::catalogue::PatternDirection;

    #[test]
    fn test_extend_step_case1_structure() {
        let extend_edge1 = ExtendEdge::new(0, 1, PatternDirection::Out);
        let extend_edge2 = ExtendEdge::new(1, 1, PatternDirection::Out);
        let extend_step1 = ExtendStep::new(1, vec![extend_edge1, extend_edge2]);
        assert_eq!(extend_step1.get_target_vertex_label(), 1);
        assert_eq!(extend_step1.extend_edges.len(), 2);
    }
}
