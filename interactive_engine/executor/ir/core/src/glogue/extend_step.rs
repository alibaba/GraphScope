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
use ir_common::KeyId;

use crate::glogue::error::{IrPatternError, IrPatternResult};
use crate::glogue::pattern::{Pattern, PbEdgeOrPath};
use crate::glogue::{query_params_to_get_v, DynIter, PatternDirection, PatternId};

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

    /// Use the ExactExtendEdge to generate corresponding edge_expand operator
    pub fn generate_edge_expand(
        &self, mut edge_opr: pb::EdgeExpand,
    ) -> IrPatternResult<pb::logical_plan::Operator> {
        // use start vertex id as tag
        edge_opr.v_tag = Some((self.src_vertex_id as KeyId).into());
        edge_opr.direction = self.dir as i32;
        edge_opr.expand_opt = pb::edge_expand::ExpandOpt::Edge as i32;
        Ok(edge_opr.into())
    }

    /// Use the ExactExtendEdge to generate corresponding path_expand related operators
    /// Specifically, path_expand will be translated to:
    /// If path_expand is the one to be intersected, translate path_expand(l,h) to path_expand(l-1, h-1) + endV() + edge_expand
    /// Otherwise, treat path_expand as the same as edge_expand (except add endV() back for path_expand)
    pub fn generate_path_expands(
        &self, mut path_opr: pb::PathExpand, is_intersect: bool,
    ) -> IrPatternResult<Vec<pb::logical_plan::Operator>> {
        let mut expand_operators = vec![];
        let start_tag = Some((self.src_vertex_id as KeyId).into());
        let direction = self.dir as i32;

        path_opr.start_tag = start_tag.clone();
        path_opr.alias = None;
        let path_expand_base = path_opr
            .base
            .as_mut()
            .ok_or(ParsePbError::EmptyFieldError("PathExpand::base in Pattern".to_string()))?;
        let get_v = path_expand_base.get_v.clone();
        let mut base_edge_expand = path_expand_base
            .edge_expand
            .as_mut()
            .ok_or(ParsePbError::EmptyFieldError("PathExpand::base::edge_expand in Pattern".to_string()))?;
        // Ensure the base is ExpandV or ExpandE + GetV
        if get_v == None && base_edge_expand.expand_opt == pb::edge_expand::ExpandOpt::Edge as i32 {
            Err(IrPatternError::Unsupported(
                "Edge Only PathExpand in Pattern has not been supported yet".to_string(),
            ))?;
        }
        (*base_edge_expand).direction = direction;
        (*base_edge_expand).expand_opt = pb::edge_expand::ExpandOpt::Edge as i32;
        if !is_intersect {
            // if not intersect, build as pure path_expand
            expand_operators.push(path_opr.into());
        } else {
            let mut last_edge_expand = base_edge_expand.clone();
            last_edge_expand.v_tag = None;
            last_edge_expand.alias = None;
            let hop_range = path_opr
                .hop_range
                .as_mut()
                .ok_or(ParsePbError::EmptyFieldError("pb::PathExpand::hop_range".to_string()))?;
            // out(1..2) = out()
            if hop_range.lower == 1 && hop_range.upper == 2 {
                last_edge_expand.v_tag = start_tag;
                expand_operators.push(last_edge_expand.into());
            } else {
                // out(low..high) = out(low-1..high-1) + endV() + out()
                hop_range.lower -= 1;
                hop_range.upper -= 1;
                // pick end vertex out from path collections
                let mut end_v = pb::GetV::default();
                end_v.opt = pb::get_v::VOpt::End as i32;
                // add path expand + endV + last edge expand
                expand_operators.push(path_opr.into());
                expand_operators.push(end_v.into());
                expand_operators.push(last_edge_expand.into());
            }
        }

        Ok(expand_operators)
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
            let edge = target_pattern.get_edge(edge_id)?;
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
    /// Use a 2D vector to store all the operators used for expand of an ExactExtendStep
    /// every Vec<Operator> in the outside vector belongs a ExactExtendEdge
    pub fn generate_expand_operators_vec(
        &self, origin_pattern: &Pattern, is_intersect: bool,
    ) -> IrPatternResult<Vec<Vec<pb::logical_plan::Operator>>> {
        let mut expand_oprs_vec = vec![];
        for extend_edge in self.iter() {
            let edge_id = extend_edge.get_edge_id();
            let edge_data = origin_pattern.get_edge_data(edge_id)?.clone();
            let mut is_pure_path = false;
            let mut expand_oprs = match edge_data {
                PbEdgeOrPath::Edge(edge_opr) => {
                    vec![extend_edge.generate_edge_expand(edge_opr)?]
                }
                PbEdgeOrPath::Path(path_opr) => {
                    // For path expansion that doesn't have further intersection,
                    // it is a pure path expansion
                    is_pure_path = !is_intersect;
                    extend_edge.generate_path_expands(path_opr, is_intersect)?
                }
            };
            // every exapand should followed by an getV operator to close
            let get_v =
                self.generate_get_v_operator(origin_pattern, extend_edge.get_direction(), is_pure_path)?;
            expand_oprs.push(get_v.clone());
            expand_oprs_vec.push(expand_oprs);
        }
        Ok(expand_oprs_vec)
    }

    /// Generate the intersect operator for ExactExtendStep's target vertex
    /// It needs its parent EdgeExpand Operator's node ids
    pub fn generate_intersect_operator(
        &self, parents: Vec<KeyId>,
    ) -> IrPatternResult<pb::logical_plan::Operator> {
        Ok((pb::Intersect { parents, key: Some((self.target_vertex_id as KeyId).into()) }).into())
    }

    /// Generate a getV operator to close the outE
    pub fn generate_get_v_operator(
        &self, origin_pattern: &Pattern, expand_direction: PatternDirection, is_pure_path: bool,
    ) -> IrPatternResult<pb::logical_plan::Operator> {
        let target_v_id = self.target_vertex_id;
        let vertex_params = origin_pattern
            .get_vertex_parameters(target_v_id)?
            .cloned();
        // when meet pure path expand, the VOpt should always be EndV
        let get_v_opt = if is_pure_path {
            pb::get_v::VOpt::End as i32
        } else {
            match expand_direction {
                PatternDirection::Out => pb::get_v::VOpt::End as i32,
                PatternDirection::In => pb::get_v::VOpt::Start as i32,
                PatternDirection::Both => pb::get_v::VOpt::Both as i32,
            }
        };
        Ok(query_params_to_get_v(vertex_params, Some(target_v_id as KeyId), get_v_opt).into())
    }
}
