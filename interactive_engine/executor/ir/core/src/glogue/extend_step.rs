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

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as pb;

use crate::glogue::error::{IrPatternError, IrPatternResult};
use crate::glogue::pattern::Pattern;
use crate::glogue::{query_params, DynIter, PatternDirection, PatternId};

/// An ExactExtendEdge denotes an edge to be extended during the pattern matching.
/// Given a ExactExtendEdge, we can uniquely locate an edge with dir in the pattern
#[derive(Debug, Clone)]
pub struct ExactExtendEdge {
    src_vertex_id: PatternId,
    edge_id: PatternId,
    dir: PatternDirection,
}

/// Initializer of ExactExtendEdge
impl ExactExtendEdge {
    pub fn new(src_vertex_id: PatternId, edge_id: PatternId, dir: PatternDirection) -> ExactExtendEdge {
        ExactExtendEdge { src_vertex_id, edge_id, dir }
    }
}

impl ExactExtendEdge {
    pub fn get_src_vertex_id(&self) -> PatternId {
        self.src_vertex_id
    }

    pub fn get_edge_id(&self) -> PatternId {
        self.edge_id
    }

    pub fn get_direction(&self) -> PatternDirection {
        self.dir
    }
}

/// An ExactExtendStep contains the vertex to be extended,
/// together with all its adjacent edges that needs to be extended as well, during the pattern matching.
/// Given a ExactExtendStep, we can uniquely find which part of the pattern to extend
#[derive(Debug, Clone)]
pub struct ExactExtendStep {
    target_vertex_id: PatternId,
    extend_edges: Vec<ExactExtendEdge>,
}

impl ExactExtendStep {
    /// Given a target pattern with a vertex id, pick all its neighboring edges and vertices to generate a exact extend step
    pub fn from_target_pattern(
        target_pattern: &Pattern, target_vertex_id: PatternId,
    ) -> IrPatternResult<Self> {
        let mut extend_edges = vec![];
        for adjacency in target_pattern.adjacencies_iter(target_vertex_id) {
            let edge_id = adjacency.get_edge_id();
            let edge = target_pattern
                .get_edge(edge_id)
                .ok_or(IrPatternError::MissingPatternEdge(edge_id))?;
            let edge_dst_vertex_id = edge.get_end_vertex().get_id();
            let src_vertex_id = if edge_dst_vertex_id == target_vertex_id {
                edge.get_start_vertex().get_id()
            } else {
                edge_dst_vertex_id
            };
            let edge_dir = adjacency.get_direction();
            extend_edges.push(ExactExtendEdge::new(src_vertex_id, edge_id, edge_dir.reverse()));
        }
        Ok(ExactExtendStep { target_vertex_id, extend_edges })
    }
}

/// Methods of accessing some fields of ExactExtendStep
impl ExactExtendStep {
    #[inline]
    pub fn get_target_vertex_id(&self) -> PatternId {
        self.target_vertex_id
    }

    pub fn iter(&self) -> DynIter<&ExactExtendEdge> {
        Box::new(self.extend_edges.iter())
    }

    pub fn len(&self) -> usize {
        self.extend_edges.len()
    }
}

impl ExactExtendStep {
    /// Use the ExactExtendStep to generate corresponding edge_expand operator
    pub fn generate_edge_expand(
        &self, extend_edge: &ExactExtendEdge, mut edge_opr: pb::EdgeExpand,
    ) -> IrPatternResult<pb::logical_plan::Operator> {
        // use start vertex id as tag
        edge_opr.v_tag = Some((extend_edge.src_vertex_id as i32).into());
        // use target vertex id as alias
        edge_opr.alias = Some((self.target_vertex_id as i32).into());
        edge_opr.direction = extend_edge.dir as i32;

        Ok(edge_opr.into())
    }

    /// Use the ExactExtendStep to generate corresponding path_expand related operators
    /// Specifically, path_expand will be translated to:
    /// If path_expand is the one to be intersected, translate path_expand(l,h) to path_expand(l-1, h-1) + endV() + edge_expand
    /// Otherwise, treat path_expand as the same as edge_expand (except add endV() back for path_expand)
    pub fn generate_path_expand(
        &self, extend_edge: &ExactExtendEdge, mut path_opr: pb::PathExpand, is_intersect: bool,
    ) -> IrPatternResult<Vec<pb::logical_plan::Operator>> {
        let mut expand_operators = vec![];
        let start_tag = Some((extend_edge.src_vertex_id as i32).into());
        let direction = extend_edge.dir as i32;
        let alias = Some((self.target_vertex_id as i32).into());

        path_opr.start_tag = start_tag.clone();
        path_opr.alias = None;
        let mut base_edge_expand = path_opr
            .base
            .as_mut()
            .ok_or(ParsePbError::EmptyFieldError("PathExpand::base in Pattern".to_string()))?;
        (*base_edge_expand).direction = direction;
        let mut end_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(vec![], vec![], None)),
            alias: alias.clone(),
        };
        if !is_intersect {
            // if not intersect, build as path_expand + endV()
            expand_operators.push(path_opr.into());
            expand_operators.push(end_v.into())
        } else {
            let mut last_edge_expand = base_edge_expand.clone();
            let hop_range = path_opr
                .hop_range
                .as_mut()
                .ok_or(ParsePbError::EmptyFieldError("pb::PathExpand::hop_range".to_string()))?;
            // out(1..2) = out()
            if hop_range.lower == 1 && hop_range.upper == 2 {
                last_edge_expand.v_tag = start_tag;
                last_edge_expand.alias = alias;
                expand_operators.push(last_edge_expand.into());
            } else {
                // out(low..high) = out(low-1..high-1) + endV() + out()
                hop_range.lower -= 1;
                hop_range.upper -= 1;
                end_v.alias = None;
                last_edge_expand.alias = alias;
                expand_operators.push(path_opr.into());
                expand_operators.push(end_v.into());
                expand_operators.push(last_edge_expand.into());
            }
        }

        Ok(expand_operators)
    }

    /// Generate the intersect operator for ExactExtendStep's target vertex
    /// It needs its parent EdgeExpand Operator's node ids
    pub fn generate_intersect_operator(
        &self, parents: Vec<i32>,
    ) -> IrPatternResult<pb::logical_plan::Operator> {
        Ok((pb::Intersect { parents, key: Some((self.target_vertex_id as i32).into()) }).into())
    }

    /// Generate the filter operator for ExactExtendStep's target vertex if it has filters
    pub fn generate_vertex_filter_operator(
        &self, origin_pattern: &Pattern,
    ) -> IrPatternResult<Option<pb::logical_plan::Operator>> {
        // pick target vertex's property and predicate info from origin pattern
        let target_v_id = self.target_vertex_id;
        if let Some(vertex_data) = origin_pattern.get_vertex_data(target_v_id) {
            if vertex_data.predicate.is_some() {
                return Ok(Some(vertex_data.clone().into()));
            }
        }
        Ok(None)
    }
}
