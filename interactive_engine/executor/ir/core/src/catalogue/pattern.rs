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

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::iter::FromIterator;

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use vec_map::VecMap;

use crate::catalogue::canonical_label::CanonicalLabelManager;
use crate::catalogue::extend_step::DefiniteExtendStep;
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternLabelId};
use crate::error::{IrError, IrResult};
use crate::plan::meta::{PlanMeta, TagId};

#[derive(Debug, Clone, Copy)]
pub struct PatternVertex {
    id: PatternId,
    label: PatternLabelId,
}

impl PatternVertex {
    pub fn new(id: PatternId, label: PatternLabelId) -> Self {
        PatternVertex { id, label }
    }

    #[inline]
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    #[inline]
    pub fn get_label(&self) -> PatternLabelId {
        self.label
    }
}

/// Each PatternVertex of a Pattern has a related PatternVertexData struct
/// - These data heavily relies on Pattern and has no meaning without a Pattern
#[derive(Debug, Clone, Default)]
struct PatternVertexData {
    /// Identify whether two vertices are structurally equivalent in the pattern
    group: PatternId,
    /// DFS Rank ID assigned to the vertex during canonical labeling
    rank: PatternId,
    /// Outgoing adjacent edges and vertices related to this vertex
    out_adjacencies: Vec<Adjacency>,
    /// Incoming adjacent edges and vertices related to this vertex
    in_adjacencies: Vec<Adjacency>,
    /// Tag (alias) assigned to this vertex by user
    tag: Option<TagId>,
    /// Predicate(filter or other expressions) this vertex has
    data: pb::Select,
}

#[derive(Debug, Clone)]
pub struct PatternEdge {
    id: PatternId,
    label: PatternLabelId,
    start_vertex: PatternVertex,
    end_vertex: PatternVertex,
}

impl PatternEdge {
    pub fn new(
        id: PatternId, label: PatternLabelId, start_vertex: PatternVertex, end_vertex: PatternVertex,
    ) -> PatternEdge {
        PatternEdge { id, label, start_vertex, end_vertex }
    }

    /// If the given direction is incoming, reverse the start and end vertex
    pub fn with_direction(mut self, direction: PatternDirection) -> PatternEdge {
        if direction == PatternDirection::In {
            std::mem::swap(&mut self.start_vertex, &mut self.end_vertex);
        }
        self
    }

    #[inline]
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    #[inline]
    pub fn get_label(&self) -> PatternLabelId {
        self.label
    }

    #[inline]
    pub fn get_start_vertex(&self) -> PatternVertex {
        self.start_vertex
    }

    #[inline]
    pub fn get_end_vertex(&self) -> PatternVertex {
        self.end_vertex
    }
}

#[derive(Debug, Clone)]
pub enum EdgeData {
    Edge(pb::EdgeExpand),
    Path(pb::PathExpand),
}

impl Default for EdgeData {
    fn default() -> Self {
        EdgeData::Edge(pb::EdgeExpand::default())
    }
}

impl From<pb::EdgeExpand> for EdgeData {
    fn from(edge: pb::EdgeExpand) -> Self {
        EdgeData::Edge(edge)
    }
}

impl From<pb::PathExpand> for EdgeData {
    fn from(path: pb::PathExpand) -> Self {
        EdgeData::Path(path)
    }
}

impl EdgeData {
    fn has_filters(&self) -> bool {
        match self {
            EdgeData::Edge(e) => e.params.is_some() && e.params.as_ref().unwrap().predicate.is_some(),
            EdgeData::Path(p) => {
                let expand_base = p.base.as_ref();
                expand_base
                    .map(|e| e.params.is_some() && e.params.as_ref().unwrap().predicate.is_some())
                    .unwrap_or(false)
            }
        }
    }

    fn is_path(&self) -> bool {
        match self {
            EdgeData::Edge(_) => false,
            EdgeData::Path(_) => true,
        }
    }
}

/// Each PatternEdge of a Pattern has a related PatternEdgeData struct
/// - These data heavily relies on Pattern and has no meaning without a Pattern
#[derive(Debug, Clone, Default)]
struct PatternEdgeData {
    /// DFS Rank ID assigned to the edge during canonical labeling
    rank: PatternId,
    /// Tag (alias) assigned to this edge by user
    tag: Option<TagId>,
    /// Predicate(filter or other expressions) this edge has
    data: EdgeData,
}

/// Adjacency records a vertex's neighboring edge and vertex
#[derive(Debug, Clone, Copy)]
pub struct Adjacency {
    /// the source vertex connect to the adjacent vertex through this edge
    edge_id: PatternId,
    /// connecting edge's label
    edge_label: PatternLabelId,
    /// the adjacent vertex
    adj_vertex: PatternVertex,
    /// the connecting direction: outgoing or incoming
    direction: PatternDirection,
}

impl Adjacency {
    fn new(src_vertex: &PatternVertex, edge: &PatternEdge) -> Option<Adjacency> {
        let start_vertex = edge.get_start_vertex();
        let end_vertex = edge.get_end_vertex();
        if (src_vertex.id, src_vertex.label) == (start_vertex.id, start_vertex.label) {
            Some(Adjacency {
                edge_id: edge.get_id(),
                edge_label: edge.get_label(),
                adj_vertex: edge.get_end_vertex(),
                direction: PatternDirection::Out,
            })
        } else if (src_vertex.id, src_vertex.label) == (end_vertex.id, end_vertex.label) {
            Some(Adjacency {
                edge_id: edge.get_id(),
                edge_label: edge.get_label(),
                adj_vertex: edge.get_start_vertex(),
                direction: PatternDirection::In,
            })
        } else {
            None
        }
    }

    #[inline]
    pub fn get_edge_id(&self) -> PatternId {
        self.edge_id
    }

    #[inline]
    pub fn get_edge_label(&self) -> PatternLabelId {
        self.edge_label
    }

    #[inline]
    pub fn get_adj_vertex(&self) -> PatternVertex {
        self.adj_vertex
    }

    #[inline]
    pub fn get_direction(&self) -> PatternDirection {
        self.direction
    }
}

#[derive(Debug, Clone, Default)]
pub struct Pattern {
    /// Key: edge id, Value: struct PatternEdge
    edges: VecMap<PatternEdge>,
    /// Key: vertex id, Value: the vertex in the pattern
    vertices: VecMap<PatternVertex>,
    /// Key: edge id, Value: struct PatternEdgeData
    /// - store data attaching to PatternEdge
    edges_data: VecMap<PatternEdgeData>,
    /// Key: vertex id, Value: struct PatternVertexData
    /// - store data attaching to PatternVertex
    vertices_data: VecMap<PatternVertexData>,
    /// Key: edge rank, Value: edge id
    /// - use an edge rank to locate an Edge
    rank_edge_map: VecMap<PatternId>,
    /// Key: vertex rank, Value: vertex id
    /// - use a vertex rank to locate a Vertex
    rank_vertex_map: VecMap<PatternId>,
    /// Key: edge's Tag info, Value: edge id
    /// - use a Tag to locate an Edge
    tag_edge_map: BTreeMap<TagId, PatternId>,
    /// Key: vertex's Tag info, Value: vertex id
    /// - use a Tag to locate a vertex
    tag_vertex_map: BTreeMap<TagId, PatternId>,
}

/// Initialze a Pattern from just a single Pattern Vertex
impl From<PatternVertex> for Pattern {
    fn from(vertex: PatternVertex) -> Pattern {
        Pattern {
            edges: VecMap::new(),
            vertices: VecMap::from_iter([(vertex.id, vertex)]),
            edges_data: VecMap::new(),
            vertices_data: VecMap::from_iter([(vertex.id, PatternVertexData::default())]),
            rank_edge_map: VecMap::new(),
            rank_vertex_map: VecMap::from_iter([(0, vertex.id)]),
            tag_edge_map: BTreeMap::new(),
            tag_vertex_map: BTreeMap::new(),
        }
    }
}

/// Initialize a Pattern from a vertor of Pattern Edges
impl TryFrom<Vec<PatternEdge>> for Pattern {
    type Error = IrError;

    fn try_from(edges: Vec<PatternEdge>) -> IrResult<Pattern> {
        if !edges.is_empty() {
            let mut new_pattern = Pattern::default();
            for edge in edges {
                // Add the new Pattern Edge to the new Pattern
                new_pattern
                    .edges
                    .insert(edge.get_id(), edge.clone());
                new_pattern
                    .edges_data
                    .insert(edge.get_id(), PatternEdgeData::default());
                // Add or update the start vertex to the new Pattern
                let start_vertex = new_pattern
                    .vertices
                    .entry(edge.get_start_vertex().get_id())
                    .or_insert(edge.get_start_vertex());
                // Update start vertex's outgoing info
                new_pattern
                    .vertices_data
                    .entry(start_vertex.get_id())
                    .or_insert(PatternVertexData::default())
                    .out_adjacencies
                    .push(Adjacency::new(start_vertex, &edge).unwrap());
                // Add or update the end vertex to the new Pattern
                let end_vertex = new_pattern
                    .vertices
                    .entry(edge.get_end_vertex().get_id())
                    .or_insert(edge.get_end_vertex());
                // Update end vertex's incoming info
                new_pattern
                    .vertices_data
                    .entry(end_vertex.get_id())
                    .or_insert(PatternVertexData::default())
                    .in_adjacencies
                    .push(Adjacency::new(end_vertex, &edge).unwrap());
            }
            new_pattern.canonical_labeling();
            Ok(new_pattern)
        } else {
            Err(IrError::InvalidPattern("Empty pattern".to_string()))
        }
    }
}

/// Initialize a Pattern from a protobuf Pattern
impl Pattern {
    pub fn from_pb_pattern(
        pb_pattern: &pb::Pattern, pattern_meta: &PatternMeta, plan_meta: &mut PlanMeta,
    ) -> IrResult<Pattern> {
        use pb::pattern::binder::Item as BinderItem;
        // next vertex id assign to the vertex picked from the pb pattern
        let mut next_vertex_id = plan_meta.get_max_tag_id() as PatternId;
        // next edge id assign to the edge picked from the pb pattern
        let mut next_edge_id = 0;
        // pattern edges picked from the pb pattern
        let mut pattern_edges = vec![];
        // record the vertices from the pb pattern having tags
        let tag_set = get_all_tags_from_pb_pattern(pb_pattern)?;
        // record the label for each vertex from the pb pattern
        let mut id_label_map: BTreeMap<PatternId, PatternLabelId> = BTreeMap::new();
        // record the vertices from the pb pattern has predicates
        let mut vertex_data_map: BTreeMap<PatternId, pb::Select> = BTreeMap::new();
        // record the edges from the pb pattern has predicates
        let mut edge_data_map: BTreeMap<PatternId, EdgeData> = BTreeMap::new();
        for sentence in &pb_pattern.sentences {
            if sentence.binders.is_empty() {
                return Err(IrError::MissingData("pb::Pattern::Sentence::binders".to_string()));
            }
            if sentence.join_kind != pb::join::JoinKind::Inner as i32 {
                return Err(IrError::InvalidPattern(format!("Only support join_kind of `InnerJoin` in ExtendStrategy in Pattern Match, while the join_kind is {:?}", sentence.join_kind)));
            }
            // pb pattern sentence must have start tag
            let start_tag = get_tag_from_name_or_id(
                sentence
                    .start
                    .clone()
                    .ok_or(IrError::MissingData("pb::Pattern::Sentence::start".to_string()))?,
            )?;
            // just use the start tag id as its pattern vertex id
            let start_tag_v_id = start_tag as PatternId;
            // check whether the start tag label is already determined or not
            let start_tag_label = id_label_map.get(&start_tag_v_id).cloned();
            // it is allowed that the pb pattern sentence doesn't have an end tag
            let end_tag = if let Some(name_or_id) = sentence.end.clone() {
                Some(get_tag_from_name_or_id(name_or_id)?)
            } else {
                None
            };
            // if the end tag exists, just use the end tag id as its pattern vertex id
            let end_tag_v_id = if let Some(tag) = end_tag { Some(tag as PatternId) } else { None };
            // check the end tag label is already determined or not
            let end_tag_label = end_tag_v_id.and_then(|v_id| id_label_map.get(&v_id).cloned());
            // record previous pattern edge's destinated vertex's id
            // init as start vertex's id
            let mut pre_dst_vertex_id: PatternId = start_tag_v_id;
            // record previous pattern edge's destinated vertex's label
            // init as start vertex's label
            let mut pre_dst_vertex_label = start_tag_label;
            // find the first edge expand's index and last edge expand's index;
            let last_expand_index = get_sentence_last_expand_index(sentence);
            // iterate over the binders
            for (i, binder) in sentence.binders.iter().enumerate() {
                match binder.item.as_ref() {
                    Some(BinderItem::Edge(_)) | Some(BinderItem::Path(_)) => {
                        // assign the new pattern edge with a new id
                        let edge_id = assign_id(&mut next_edge_id, None);
                        let edge_expand = if let Some(BinderItem::Path(path_expand)) = binder.item.as_ref()
                        {
                            edge_data_map.insert(edge_id, EdgeData::from(path_expand.clone()));
                            path_expand
                                .base
                                .as_ref()
                                .ok_or(IrError::MissingData("PathExpand::base in Pattern".to_string()))?
                        } else if let Some(BinderItem::Edge(edge_expand)) = binder.item.as_ref() {
                            edge_data_map.insert(edge_id, EdgeData::from(edge_expand.clone()));
                            edge_expand
                        } else {
                            unreachable!()
                        };
                        // get edge label's id
                        let edge_label = get_edge_expand_label(edge_expand)?;
                        // get edge direction
                        let edge_direction = unsafe {
                            std::mem::transmute::<i32, pb::edge_expand::Direction>(edge_expand.direction)
                        };
                        // assign/pick the source vertex id and destination vertex id of the pattern edge
                        let src_vertex_id = pre_dst_vertex_id;
                        let dst_vertex_id = assign_expand_dst_vertex_id(
                            i == last_expand_index.unwrap(),
                            end_tag_v_id,
                            edge_expand,
                            &tag_set,
                            &mut next_vertex_id,
                        )?;
                        pre_dst_vertex_id = dst_vertex_id;
                        // assign vertices labels
                        // check which label candidate can connect to the previous determined partial pattern
                        let required_src_vertex_label = pre_dst_vertex_label;
                        let required_dst_vertex_label =
                            if i == last_expand_index.unwrap() { end_tag_label } else { None };
                        // check whether we find proper src vertex label and dst vertex label
                        let (src_vertex_label, dst_vertex_label, direction) = assign_src_dst_vertex_labels(
                            pattern_meta,
                            edge_label,
                            edge_direction,
                            required_src_vertex_label,
                            required_dst_vertex_label,
                        )?;
                        id_label_map.insert(src_vertex_id, src_vertex_label);
                        id_label_map.insert(dst_vertex_id, dst_vertex_label);
                        // generate the new pattern edge and add to the pattern_edges_collection
                        let new_pattern_edge = PatternEdge::new(
                            edge_id,
                            edge_label,
                            PatternVertex::new(src_vertex_id, src_vertex_label),
                            PatternVertex::new(dst_vertex_id, dst_vertex_label),
                        )
                        .with_direction(direction);
                        pattern_edges.push(new_pattern_edge);
                        pre_dst_vertex_label = Some(dst_vertex_label);
                    }
                    Some(BinderItem::Select(select)) => {
                        vertex_data_map.insert(pre_dst_vertex_id, select.clone());
                    }
                    Some(BinderItem::Vertex(_get_v)) => {
                        // GetV() when path().endV(); ignore endV() here, and add endV() back when build_logical_plan()
                    }
                    None => {
                        return Err(IrError::MissingData("pb::pattern::binder::Item".to_string()));
                    }
                }
            }
        }
        plan_meta.set_max_tag_id(next_vertex_id as TagId);
        Pattern::try_from(pattern_edges).and_then(|mut pattern| {
            for tag in tag_set {
                pattern.set_vertex_tag(tag as PatternId, tag);
            }
            for (v_id, vertex_data) in vertex_data_map {
                pattern.set_vertex_data(v_id, vertex_data);
            }
            for (e_id, edge_data) in edge_data_map {
                pattern.set_edge_data(e_id, edge_data);
            }
            Ok(pattern)
        })
    }
}

/// Methods for Pattern to generate pb Logical plan of pattern matching
impl Pattern {
    /// Generate a naive extend based pattern match plan
    pub fn generate_simple_extend_match_plan(&self) -> IrResult<pb::LogicalPlan> {
        let mut trace_pattern = self.clone();
        let mut definite_extend_steps = vec![];
        while trace_pattern.get_vertices_num() > 1 {
            let mut all_vertex_ids: Vec<PatternId> = trace_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            // Sort the vertices by:
            // 1. vertex has predicates will be extended first;
            // 2. vertex adjacent to more number of edges with predicates should be extended first; TODO: consider path with filters;
            // 3. vertex adjacent to more number of edges with data of path_expand should be extended later;
            // 4. vertex with larger degree will be extended later
            all_vertex_ids.sort_by(|&v1_id, &v2_id| {
                let v1_has_predicate = trace_pattern.has_vertex_filter(v1_id);
                let v2_has_predicate = trace_pattern.has_vertex_filter(v2_id);
                if v1_has_predicate && !v2_has_predicate {
                    Ordering::Greater
                } else if !v1_has_predicate && v2_has_predicate {
                    Ordering::Less
                } else {
                    // TODO: further consider path_expand with filters
                    let v1_edges_predicate_num = trace_pattern
                        .adjacencies_iter(v1_id)
                        .filter(|adj| trace_pattern.has_edge_filters(adj.get_edge_id()))
                        .count();
                    let v2_edges_predicate_num = trace_pattern
                        .adjacencies_iter(v2_id)
                        .filter(|adj| trace_pattern.has_edge_filters(adj.get_edge_id()))
                        .count();
                    if v1_edges_predicate_num > v2_edges_predicate_num {
                        Ordering::Greater
                    } else if v1_edges_predicate_num < v2_edges_predicate_num {
                        Ordering::Less
                    } else {
                        let v1_path_data_num = trace_pattern
                            .adjacencies_iter(v1_id)
                            .filter(|adj| trace_pattern.is_pathxpd_data(adj.get_edge_id()))
                            .count();
                        let v2_path_data_num = trace_pattern
                            .adjacencies_iter(v2_id)
                            .filter(|adj| trace_pattern.is_pathxpd_data(adj.get_edge_id()))
                            .count();
                        if v1_path_data_num > v2_path_data_num {
                            Ordering::Less
                        } else if v1_path_data_num < v2_path_data_num {
                            Ordering::Greater
                        } else {
                            let degree_order = trace_pattern
                                .get_vertex_degree(v1_id)
                                .cmp(&trace_pattern.get_vertex_degree(v2_id));
                            if let Ordering::Equal = degree_order {
                                trace_pattern
                                    .get_vertex_out_degree(v1_id)
                                    .cmp(&trace_pattern.get_vertex_out_degree(v2_id))
                            } else {
                                degree_order
                            }
                        }
                    }
                }
            });
            let select_vertex_id = *all_vertex_ids.first().unwrap();
            let definite_extend_step =
                DefiniteExtendStep::from_target_pattern(&trace_pattern, select_vertex_id).unwrap();
            definite_extend_steps.push(definite_extend_step);
            trace_pattern.remove_vertex(select_vertex_id);
        }
        definite_extend_steps.push(trace_pattern.try_into()?);
        build_logical_plan(self, definite_extend_steps)
    }
}

/// Build logical plan for extend based pattern match plan
///             source
///           /   |    \
///           \   |    /
///            intersect
fn build_logical_plan(
    origin_pattern: &Pattern, mut definite_extend_steps: Vec<DefiniteExtendStep>,
) -> IrResult<pb::LogicalPlan> {
    let mut match_plan = pb::LogicalPlan::default();
    let mut child_offset: i32 = 1;
    let source_extend = match definite_extend_steps.pop() {
        Some(src_extend) => src_extend,
        None => {
            return Err(IrError::InvalidPattern(
                "Build logical plan error: from empty extend steps!".to_string(),
            ))
        }
    };
    let source_vertex_id = source_extend.get_target_vertex_id();
    let source_vertex_label = source_extend.get_target_vertex_label();
    let source_vertex_filter = origin_pattern
        .get_vertex_data(source_vertex_id)
        .cloned()
        .and_then(|select| select.predicate);
    // Fuse `source_vertex_label` into source, for efficiently scan
    // TODO: However, we may still have `hasLabel(xx)` in predicate of source_vertex_filter, which is not necessary.
    // TODO: btw, index scan case (e.g., `hasLabel(xx).has('id',xx)`), if exists, should be in the first priority.
    let query_params = query_params(vec![source_vertex_label.into()], vec![], source_vertex_filter);
    let source_opr = pb::Scan {
        scan_opt: 0,
        alias: Some((source_vertex_id as i32).into()),
        params: Some(query_params),
        idx_predicate: None,
    };
    let mut pre_node = pb::logical_plan::Node { opr: Some(source_opr.into()), children: vec![] };
    for definite_extend_step in definite_extend_steps.into_iter().rev() {
        let edge_expands_num = definite_extend_step.len();
        // the case that needs intersection;
        if edge_expands_num > 1 {
            let mut expand_child_offset = child_offset;
            // record the opr ids that are pre_node's children
            let mut expand_ids = vec![];
            // record the opr ids that are intersect's parents
            let mut intersect_ids = vec![];
            // record the oprs to expand. e.g., [[EdgeExpand], [PathExpand, GetV, EdgeExpand]]
            let mut expand_oprs = vec![vec![]];
            for definite_extend_edge in definite_extend_step.iter() {
                let edge_id = definite_extend_edge.get_edge_id();
                let edge_data = origin_pattern
                    .get_edge_data(edge_id)
                    .cloned()
                    .ok_or(IrError::MissingData("edge_data in Pattern".to_string()))?;
                match edge_data {
                    EdgeData::Edge(edge_expand) => {
                        expand_ids.push(expand_child_offset);
                        intersect_ids.push(expand_child_offset);
                        let edge_expand_opr =
                            definite_extend_step.generate_edge_expand(definite_extend_edge, edge_expand)?;
                        expand_oprs.push(vec![edge_expand_opr]);
                        expand_child_offset += 1;
                    }
                    EdgeData::Path(path_expand) => {
                        let path_expand_oprs = definite_extend_step.generate_path_expand(
                            definite_extend_edge,
                            path_expand,
                            true,
                        )?;
                        let path_expand_oprs_num = path_expand_oprs.len() as i32;
                        // pre_node's child should be the id of the first op in path_expand_oprs
                        expand_ids.push(expand_child_offset);
                        // intersect's parent should be the id of the last op in path_expand_oprs
                        let last_expand_id = expand_child_offset + path_expand_oprs_num - 1;
                        intersect_ids.push(last_expand_id);
                        expand_oprs.push(path_expand_oprs);
                        expand_child_offset += path_expand_oprs_num;
                    }
                }
            }
            for &i in expand_ids.iter() {
                pre_node.children.push(i);
            }
            match_plan.nodes.push(pre_node);

            let intersect_opr_id = expand_child_offset;
            for expand_opr_vec in expand_oprs {
                let expand_opr_vec_len = expand_opr_vec.len();
                for (idx, expand_opr) in expand_opr_vec.into_iter().enumerate() {
                    child_offset += 1;
                    let node = if idx == expand_opr_vec_len - 1 {
                        // the last node is the one to intersect
                        pb::logical_plan::Node { opr: Some(expand_opr), children: vec![intersect_opr_id] }
                    } else {
                        pb::logical_plan::Node { opr: Some(expand_opr), children: vec![child_offset] }
                    };
                    match_plan.nodes.push(node);
                }
            }
            let intersect = definite_extend_step.generate_intersect_operator(intersect_ids)?;
            pre_node = pb::logical_plan::Node { opr: Some(intersect), children: vec![] };
            child_offset += 1;
        } else if edge_expands_num == 1 {
            let definite_extend_edge = definite_extend_step
                .iter()
                .last()
                .ok_or(IrError::MissingData("extend_edge in definite_extend_step".to_string()))?;
            let edge_id = definite_extend_edge.get_edge_id();
            let edge_data = origin_pattern
                .get_edge_data(edge_id)
                .cloned()
                .ok_or(IrError::MissingData("edge_data in Pattern".to_string()))?;
            match edge_data {
                EdgeData::Edge(edge_expand) => {
                    let edge_expand_opr =
                        definite_extend_step.generate_edge_expand(definite_extend_edge, edge_expand)?;
                    append_opr(&mut match_plan, &mut pre_node, edge_expand_opr, &mut child_offset);
                }
                EdgeData::Path(path_expand) => {
                    let path_expand_oprs = definite_extend_step.generate_path_expand(
                        definite_extend_edge,
                        path_expand,
                        false,
                    )?;
                    for opr in path_expand_oprs {
                        append_opr(&mut match_plan, &mut pre_node, opr, &mut child_offset);
                    }
                }
            }
        } else {
            return Err(IrError::InvalidPattern(
                "Build logical plan error: extend step is not source but has 0 edges".to_string(),
            ));
        }
        if let Some(filter) = definite_extend_step.generate_vertex_filter_operator(origin_pattern)? {
            append_opr(&mut match_plan, &mut pre_node, filter, &mut child_offset);
        }
    }
    // finally, we project those pattern vertices whose aliases are user-given, i.e., those that may be referred later.
    let mut mappings = Vec::with_capacity(origin_pattern.tag_vertex_map.len());
    for (tag_id, _) in &origin_pattern.tag_vertex_map {
        let expr = str_to_expr_pb(format!("@{}", tag_id)).ok();
        let mapping = pb::project::ExprAlias { expr, alias: Some((*tag_id as i32).into()) };
        mappings.push(mapping);
    }
    let project = pb::Project { mappings, is_append: false }.into();
    append_opr(&mut match_plan, &mut pre_node, project, &mut child_offset);
    // and append the final project op
    pre_node.children.push(child_offset);
    match_plan.nodes.push(pre_node);
    Ok(match_plan)
}

/// Append opr into match_plan in a traversal way (i.e., a->b->c->d ...).
/// Specifically, append pre_node into match_plan after set its child as the new_opr; and then update pre_node as the new_opr.
fn append_opr(
    match_plan: &mut pb::LogicalPlan, pre_node: &mut pb::logical_plan::Node,
    new_opr: pb::logical_plan::Operator, child_offset: &mut i32,
) {
    let new_opr_id = *child_offset;
    pre_node.children.push(new_opr_id);
    (*match_plan).nodes.push(pre_node.clone());
    *pre_node = pb::logical_plan::Node { opr: Some(new_opr), children: vec![] };
    *child_offset += 1;
}

/// Get the tag info from the given name_or_id
/// - in pb::Pattern transformation, tag is required to be id instead of str
fn get_tag_from_name_or_id(name_or_id: common_pb::NameOrId) -> IrResult<TagId> {
    let tag: ir_common::NameOrId = name_or_id.try_into()?;
    match tag {
        ir_common::NameOrId::Id(tag_id) => Ok(tag_id as TagId),
        _ => Err(IrError::TagNotExist(tag)),
    }
}

/// Get all the tags from the pb Pattern and store in a set
fn get_all_tags_from_pb_pattern(pb_pattern: &pb::Pattern) -> IrResult<BTreeSet<TagId>> {
    use pb::pattern::binder::Item as BinderItem;
    let mut tag_id_set = BTreeSet::new();
    for sentence in pb_pattern.sentences.iter() {
        if let Some(start_tag) = sentence.start.as_ref().cloned() {
            let start_tag_id = get_tag_from_name_or_id(start_tag)?;
            tag_id_set.insert(start_tag_id);
        }
        if let Some(end_tag) = sentence.end.as_ref().cloned() {
            let end_tag_id = get_tag_from_name_or_id(end_tag)?;
            tag_id_set.insert(end_tag_id);
        }
        // TODO: not sure if it is necessary. After pattern matching, seems only the tags of 'start_tag' and 'end_tag' should be preserved?
        for binder in sentence.binders.iter() {
            let alias = match binder.item.as_ref() {
                Some(BinderItem::Edge(edge_expand)) => edge_expand.alias.clone(),
                Some(BinderItem::Path(path_expand)) => path_expand.alias.clone(),
                Some(BinderItem::Vertex(get_v)) => get_v.alias.clone(),
                _ => None,
            };
            if let Some(tag) = alias {
                let tag_id = get_tag_from_name_or_id(tag)?;
                tag_id_set.insert(tag_id);
            }
        }
    }
    Ok(tag_id_set)
}

/// Get the last edge expand's index of a pb pattern sentence among all of its binders
fn get_sentence_last_expand_index(sentence: &pb::pattern::Sentence) -> Option<usize> {
    match sentence
        .binders
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, binder)| match binder.item.as_ref() {
            Some(pb::pattern::binder::Item::Edge(_)) | Some(pb::pattern::binder::Item::Path(_)) => true,
            _ => false,
        })
        .next()
    {
        Some((id, _)) => Some(id),
        _ => None,
    }
}

/// Get the edge expand's label
/// - in current realization, edge_expand only allows to have one label
/// - if it has no label or more than one label, give Error
fn get_edge_expand_label(edge_expand: &pb::EdgeExpand) -> IrResult<PatternLabelId> {
    if edge_expand.expand_opt != pb::edge_expand::ExpandOpt::Vertex as i32 {
        return Err(IrError::Unsupported("Expand edge in pattern".to_string()));
    }
    if let Some(params) = edge_expand.params.as_ref() {
        // TODO: Support Fuzzy Pattern
        if params.tables.is_empty() {
            return Err(IrError::Unsupported("FuzzyPattern: no specific edge expand label".to_string()));
        } else if params.tables.len() > 1 {
            return Err(IrError::Unsupported("FuzzyPattern: more than 1 edge expand label".to_string()));
        }
        // get edge label's id
        match params.tables[0].item.as_ref() {
            Some(common_pb::name_or_id::Item::Id(e_label_id)) => Ok(*e_label_id),
            _ => Err(IrError::InvalidPattern("edge expand doesn't have valid label".to_string())),
        }
    } else {
        Err(IrError::MissingData("pb::EdgeExpand.params".to_string()))
    }
}

/// Assign a vertex or edge with the next_id, and add the next_id by one
/// - For a vertex:
/// - - if the vertex has tag, just use its tag id as the pattern id
/// - - the next_id cannot be the same as another vertex's tag id (pattern id)
/// - - otherwise the assigned pattern id will be repeated
fn assign_id(next_id: &mut PatternId, tag_set_opt: Option<&BTreeSet<TagId>>) -> PatternId {
    if let Some(tag_set) = tag_set_opt {
        while tag_set.contains(&(*next_id as TagId)) {
            *next_id += 1;
        }
    }
    let id_to_assign = *next_id;
    *next_id += 1;
    id_to_assign
}

/// Assign an id the dst vertex of an edge expand
/// - firstly, check whether the edge expand is the tail of the sentence or not
///   - if it is sentence's end vertex
///     - if the sentence's end vertex's id is already assigned, just use it
///     - else, assign it with a new id
///   - else
///     - if the dst vertex is related with the tag, assign its id by tag
///     - else, assign it with a new id
fn assign_expand_dst_vertex_id(
    is_tail: bool, sentence_end_id: Option<PatternId>, edge_expand: &pb::EdgeExpand,
    tag_set: &BTreeSet<TagId>, next_vertex_id: &mut PatternId,
) -> IrResult<PatternId> {
    if is_tail {
        if let Some(v_id) = sentence_end_id {
            Ok(v_id)
        } else {
            Ok(assign_id(next_vertex_id, Some(tag_set)))
        }
    } else {
        // check alias tag
        let dst_vertex_tag = if let Some(name_or_id) = edge_expand.alias.clone() {
            Some(get_tag_from_name_or_id(name_or_id)?)
        } else {
            None
        };
        // if the dst vertex has tag, just use the tag id as its pattern id
        if let Some(tag) = dst_vertex_tag {
            Ok(tag as PatternId)
        } else {
            Ok(assign_id(next_vertex_id, Some(tag_set)))
        }
    }
}

/// Based on the vertex labels candidates and required src/dst vertex label,
/// assign the src and dst vertex with vertex labels meeting the requirement
fn assign_src_dst_vertex_labels(
    pattern_meta: &PatternMeta, edge_label: PatternLabelId, edge_direction: pb::edge_expand::Direction,
    required_src_label: Option<PatternLabelId>, required_dst_label: Option<PatternLabelId>,
) -> IrResult<(PatternLabelId, PatternLabelId, PatternDirection)> {
    // Based on the pattern meta info, find all possible vertex labels candidates:
    // (src vertex label, dst vertex label) with the given edge label and edge direction
    // - if the edge direction is Outgoing:
    //   we use (start vertex label, end vertex label) as (src vertex label, dst vertex label)
    // - if the edge direction is Incoming:
    //   we use (end vertex label, start vertex label) as (src vertex label, dst vertex label)
    // - if the edge direction is Both:
    //   we connect the iterators returned by Outgoing and Incoming together as they all can be the candidates
    let vertex_labels_candis_iter: DynIter<(PatternLabelId, PatternLabelId, PatternDirection)> =
        match edge_direction {
            pb::edge_expand::Direction::Out => Box::new(
                pattern_meta
                    .associated_vlabels_iter_by_elabel(edge_label)
                    .map(|(start_v_label, end_v_label)| {
                        (start_v_label, end_v_label, PatternDirection::Out)
                    }),
            ),
            pb::edge_expand::Direction::In => Box::new(
                pattern_meta
                    .associated_vlabels_iter_by_elabel(edge_label)
                    .map(|(start_v_label, end_v_label)| (end_v_label, start_v_label, PatternDirection::In)),
            ),
            pb::edge_expand::Direction::Both => {
                return Err(IrError::Unsupported("Both Direction leads to Fuzzy Pattern".to_string()));
            }
        };
    // For a chosen candidates:
    // - if the required src label is some, its src vertex label must match the requirement
    // - if the required dst label is some, its dst vertex label must match the requirement
    let vertex_label_candis: Vec<(PatternLabelId, PatternLabelId, PatternDirection)> =
        if let (Some(src_label), Some(dst_label)) = (required_src_label, required_dst_label) {
            vertex_labels_candis_iter
                .filter(|&(src_label_candi, dst_label_candi, _)| {
                    src_label_candi == src_label && dst_label_candi == dst_label
                })
                .collect()
        } else if let Some(src_label) = required_src_label {
            vertex_labels_candis_iter
                .filter(|&(src_label_candi, _, _)| src_label_candi == src_label)
                .collect()
        } else if let Some(dst_label) = required_dst_label {
            vertex_labels_candis_iter
                .filter(|&(_, dst_label_candi, _)| dst_label_candi == dst_label)
                .collect()
        } else {
            vertex_labels_candis_iter.collect()
        };
    if vertex_label_candis.len() == 0 {
        Err(IrError::InvalidPattern("Cannot find valid label for some vertices".to_string()))
    } else if vertex_label_candis.len() > 1 {
        Err(IrError::Unsupported(
            "Some vertices may have more than one valid Label, which leads to Fuzzy Pattern".to_string(),
        ))
    } else {
        Ok(vertex_label_candis[0])
    }
}

/// Getters of fields of Pattern
impl Pattern {
    /// Get a PatternEdge struct from an edge id
    #[inline]
    pub fn get_edge(&self, edge_id: PatternId) -> Option<&PatternEdge> {
        self.edges.get(edge_id)
    }

    /// Get PatternEdge from Given Edge Rank
    #[inline]
    pub fn get_edge_from_rank(&self, edge_rank: PatternId) -> Option<&PatternEdge> {
        self.rank_edge_map
            .get(edge_rank)
            .and_then(|&edge_id| self.get_edge(edge_id))
    }

    /// Get PatternEdge from Given Tag
    #[inline]
    pub fn get_edge_from_tag(&self, edge_tag: TagId) -> Option<&PatternEdge> {
        self.tag_edge_map
            .get(&edge_tag)
            .and_then(|&edge_id| self.get_edge(edge_id))
    }

    /// Get the total number of edges in the pattern
    #[inline]
    pub fn get_edges_num(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    pub fn get_min_edge_id(&self) -> PatternId {
        self.edges
            .iter()
            .map(|(edge_id, _)| edge_id)
            .next()
            .unwrap_or(0)
    }

    #[inline]
    pub fn get_max_edge_id(&self) -> PatternId {
        self.edges
            .iter()
            .map(|(edge_id, _)| edge_id)
            .last()
            .unwrap_or(0)
    }

    /// Get the minimum edge label id of the current pattern
    #[inline]
    pub fn get_min_edge_label(&self) -> Option<PatternLabelId> {
        self.edges
            .iter()
            .map(|(_, edge)| edge.get_label())
            .min()
    }

    /// Get the maximum edge label id of the current pattern
    #[inline]
    pub fn get_max_edge_label(&self) -> Option<PatternLabelId> {
        self.edges
            .iter()
            .map(|(_, edge)| edge.get_label())
            .max()
    }

    /// Get a PatternEdge's Rank info
    #[inline]
    pub fn get_edge_rank(&self, edge_id: PatternId) -> Option<PatternId> {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| edge_data.rank)
    }

    /// Get a PatternEdge's Tag info
    #[inline]
    pub fn get_edge_tag(&self, edge_id: PatternId) -> Option<TagId> {
        self.edges_data
            .get(edge_id)
            .and_then(|edge_data| edge_data.tag)
    }

    /// Get the data of a PatternEdge
    #[inline]
    pub fn get_edge_data(&self, edge_id: PatternId) -> Option<&EdgeData> {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| &edge_data.data)
    }

    /// test if the data is a path
    #[inline]
    pub fn is_pathxpd_data(&self, edge_id: PatternId) -> bool {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| (&edge_data.data).is_path())
            .unwrap_or(false)
    }

    /// test if PatternEdge has filters
    #[inline]
    pub fn has_edge_filters(&self, edge_id: PatternId) -> bool {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| (&edge_data.data).has_filters())
            .unwrap_or(false)
    }

    /// Get a PatternVertex struct from a vertex id
    #[inline]
    pub fn get_vertex(&self, vertex_id: PatternId) -> Option<&PatternVertex> {
        self.vertices.get(vertex_id)
    }

    /// Get PatternVertex Reference from Given Rank
    #[inline]
    pub fn get_vertex_from_rank(&self, vertex_rank: PatternId) -> Option<&PatternVertex> {
        self.rank_vertex_map
            .get(vertex_rank)
            .and_then(|&vertex_id| self.get_vertex(vertex_id))
    }

    /// Get PatternVertex Reference from Given Tag
    #[inline]
    pub fn get_vertex_from_tag(&self, vertex_tag: TagId) -> Option<&PatternVertex> {
        self.tag_vertex_map
            .get(&vertex_tag)
            .and_then(|&vertex_id| self.get_vertex(vertex_id))
    }

    /// Get the total number of vertices in the pattern
    #[inline]
    pub fn get_vertices_num(&self) -> usize {
        self.vertices.len()
    }

    #[inline]
    pub fn get_min_vertex_id(&self) -> PatternId {
        self.vertices
            .iter()
            .map(|(vertex_id, _)| vertex_id)
            .next()
            .unwrap()
    }

    #[inline]
    pub fn get_max_vertex_id(&self) -> PatternId {
        self.vertices
            .iter()
            .map(|(vertex_id, _)| vertex_id)
            .last()
            .unwrap()
    }

    /// Get the minimum vertex label id of the current pattern
    #[inline]
    pub fn get_min_vertex_label(&self) -> Option<PatternLabelId> {
        self.vertices
            .iter()
            .map(|(_, vertex)| vertex.get_label())
            .min()
    }

    /// Get the maximum vertex label id of the current pattern
    pub fn get_max_vertex_label(&self) -> Option<PatternLabelId> {
        self.vertices
            .iter()
            .map(|(_, vertex)| vertex.get_label())
            .max()
    }

    /// Get Vertex Rank from Vertex ID Reference
    #[inline]
    pub fn get_vertex_group(&self, vertex_id: PatternId) -> Option<PatternId> {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.group)
    }

    /// Get Vertex Rank from Vertex ID Reference
    #[inline]
    pub fn get_vertex_rank(&self, vertex_id: PatternId) -> Option<PatternId> {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.rank)
    }

    /// Get a PatternVertex's Tag info
    #[inline]
    pub fn get_vertex_tag(&self, vertex_id: PatternId) -> Option<TagId> {
        self.vertices_data
            .get(vertex_id)
            .and_then(|vertex_data| vertex_data.tag)
    }

    /// Get the data of a PatternVertex
    #[inline]
    pub fn get_vertex_data(&self, vertex_id: PatternId) -> Option<&pb::Select> {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| &vertex_data.data)
    }

    // Test if PatternVertex has filters
    #[inline]
    pub fn has_vertex_filter(&self, vertex_id: PatternId) -> bool {
        self.get_vertex_data(vertex_id)
            .map(|select| select.predicate.is_some())
            .unwrap_or(false)
    }

    /// Count how many outgoing edges connect to this vertex
    #[inline]
    pub fn get_vertex_out_degree(&self, vertex_id: PatternId) -> usize {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.out_adjacencies.len())
            .unwrap_or(0)
    }

    /// Count how many incoming edges connect to this vertex
    #[inline]
    pub fn get_vertex_in_degree(&self, vertex_id: PatternId) -> usize {
        self.vertices_data
            .get(vertex_id)
            .map(|vertex_data| vertex_data.in_adjacencies.len())
            .unwrap_or(0)
    }

    /// Count how many edges connect to this vertex
    #[inline]
    pub fn get_vertex_degree(&self, vertex_id: PatternId) -> usize {
        self.get_vertex_out_degree(vertex_id) + self.get_vertex_in_degree(vertex_id)
    }
}

/// Iterators of fields of Pattern
impl Pattern {
    /// Iterate Edges
    pub fn edges_iter(&self) -> DynIter<&PatternEdge> {
        Box::new(self.edges.iter().map(|(_, edge)| edge))
    }

    /// Iterate Edges with the given edge label
    pub fn edges_iter_by_label(&self, edge_label: PatternLabelId) -> DynIter<&PatternEdge> {
        Box::new(
            self.edges
                .iter()
                .map(|(_, edge)| edge)
                .filter(move |edge| edge.get_label() == edge_label),
        )
    }

    /// Iterate over edges that has tag
    pub fn edges_with_tag_iter(&self) -> DynIter<&PatternEdge> {
        Box::new(
            self.tag_edge_map
                .iter()
                .map(move |(_, &edge_id)| self.get_edge(edge_id).unwrap()),
        )
    }

    /// Iterate Vertices
    pub fn vertices_iter(&self) -> DynIter<&PatternVertex> {
        Box::new(self.vertices.iter().map(|(_, vertex)| vertex))
    }

    /// Iterate Vertices with the given vertex label
    pub fn vertices_iter_by_label(&self, vertex_label: PatternLabelId) -> DynIter<&PatternVertex> {
        Box::new(
            self.vertices
                .iter()
                .map(|(_, vertex)| vertex)
                .filter(move |vertex| vertex.get_label() == vertex_label),
        )
    }

    /// Iterate over vertices that has tag
    pub fn vertices_with_tag_iter(&self) -> DynIter<&PatternVertex> {
        Box::new(
            self.tag_vertex_map
                .iter()
                .map(move |(_, &vertex_id)| self.get_vertex(vertex_id).unwrap()),
        )
    }

    /// Iterate all outgoing edges from the given vertex
    pub fn out_adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        if let Some(vertex_data) = self.vertices_data.get(vertex_id) {
            Box::new(vertex_data.out_adjacencies.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Iterate all incoming edges to the given vertex
    pub fn in_adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        if let Some(vertex_data) = self.vertices_data.get(vertex_id) {
            Box::new(vertex_data.in_adjacencies.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }

    /// Iterate both outgoing and incoming edges of the given vertex
    pub fn adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        Box::new(
            self.out_adjacencies_iter(vertex_id)
                .chain(self.in_adjacencies_iter(vertex_id)),
        )
    }
}

/// Setters of fields of Pattern
impl Pattern {
    /// Assign a PatternEdge with the given group
    fn set_edge_rank(&mut self, edge_id: PatternId, edge_rank: PatternId) {
        // Assign the rank to the edge
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            self.rank_edge_map.insert(edge_rank, edge_id);
            edge_data.rank = edge_rank;
        }
    }

    /// Assign a PatternEdge of the Pattern with the Given Tag
    pub fn set_edge_tag(&mut self, edge_tag: TagId, edge_id: PatternId) {
        // If the tag is previously assigned to another edge, remove it
        if let Some(&old_edge_id) = self.tag_edge_map.get(&edge_tag) {
            self.edges_data
                .get_mut(old_edge_id)
                .unwrap()
                .tag = None;
        }
        // Assign the tag to the edge
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            self.tag_edge_map.insert(edge_tag, edge_id);
            edge_data.tag = Some(edge_tag);
        }
    }

    /// Set predicate requirement of a PatternEdge
    pub fn set_edge_data(&mut self, edge_id: PatternId, opr: EdgeData) {
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            edge_data.data = opr;
        }
    }

    /// Assign a PatternVertex with the given group
    fn set_vertex_group(&mut self, vertex_id: PatternId, group: PatternId) {
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            vertex_data.group = group;
        }
    }

    fn set_vertex_rank(&mut self, vertex_id: PatternId, vertex_rank: PatternId) {
        // Assign the rank to the vertex
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            self.rank_vertex_map
                .insert(vertex_rank, vertex_id);
            vertex_data.rank = vertex_rank;
        }
    }

    /// Assign a PatternVertex with the given tag
    pub fn set_vertex_tag(&mut self, vertex_id: PatternId, vertex_tag: TagId) {
        // If the tag is previously assigned to another vertex, remove it
        if let Some(&old_vertex_id) = self.tag_vertex_map.get(&vertex_tag) {
            self.vertices_data
                .get_mut(old_vertex_id)
                .unwrap()
                .tag = None;
        }
        // Assign the tag to the vertex
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            self.tag_vertex_map
                .insert(vertex_tag, vertex_id);
            vertex_data.tag = Some(vertex_tag);
        }
    }

    /// Set predicate requirement of a PatternVertex
    pub fn set_vertex_data(&mut self, vertex_id: PatternId, opr: pb::Select) {
        if let Some(vertex_data) = self.vertices_data.get_mut(vertex_id) {
            vertex_data.data = opr;
        }
    }
}

/// Methods for Canonical Labeling
impl Pattern {
    /// Canonical Labeling gives each vertex a unique ID (rank), which is used to encode the pattern.
    ///
    /// It consists of two parts:
    /// - Vertex Grouping (Partition): vertices in the same group (partition) are equivalent in structure.
    /// - Pattern Ranking: given the vertex groups, rank each vertex and edge with a unique ID.
    fn canonical_labeling(&mut self) {
        let mut canonical_label_manager = CanonicalLabelManager::from(&*self);
        canonical_label_manager.vertex_grouping(self);
        canonical_label_manager.pattern_ranking(self);
        self.update_vertex_groups(&canonical_label_manager);
        self.update_pattern_ranks(&canonical_label_manager);
    }

    /// Update vertex groups
    fn update_vertex_groups(&mut self, canonical_label_manager: &CanonicalLabelManager) {
        canonical_label_manager
            .vertex_groups_iter()
            .for_each(|(v_id, v_group)| {
                self.set_vertex_group(v_id, v_group);
            });
    }

    /// Update ranks for vertices and edges
    fn update_pattern_ranks(&mut self, canonical_label_manager: &CanonicalLabelManager) {
        // update vertex ranks
        self.rank_vertex_map.clear();
        canonical_label_manager
            .vertex_ranks_iter()
            .for_each(|(v_id, v_rank)| {
                self.set_vertex_rank(v_id, v_rank.unwrap());
            });

        // Update edge ranks
        self.rank_edge_map.clear();
        canonical_label_manager
            .edge_ranks_iter()
            .for_each(|(e_id, e_rank)| {
                self.set_edge_rank(e_id, e_rank.unwrap());
            });
    }
}

/// Methods for Pattern Extension
impl Pattern {
    /// Get all the vertices(id) with the same vertex label and vertex group
    ///
    /// These vertices are equivalent in the Pattern
    pub fn get_equivalent_vertices(
        &self, v_label: PatternLabelId, v_group: PatternId,
    ) -> Vec<PatternVertex> {
        self.vertices_iter()
            .filter(|vertex| {
                vertex.get_label() == v_label && self.get_vertex_group(vertex.get_id()).unwrap() == v_group
            })
            .cloned()
            .collect()
    }

    /// Edit the pattern by connect some edges to the current pattern
    fn add_edge(&mut self, edge: &PatternEdge) -> IrResult<()> {
        // Error that the adding edge already exist
        if self.edges.contains_key(edge.get_id()) {
            return Err(IrError::InvalidCode("The adding edge already existed".to_string()));
        }
        let start_vertex = edge.get_start_vertex();
        let end_vertex = edge.get_end_vertex();
        // Error that cannot connect the edge to the pattern
        if let (None, None) =
            (self.vertices.get(start_vertex.get_id()), self.vertices.get(end_vertex.get_id()))
        {
            return Err(IrError::InvalidCode("The adding edge cannot connect to the pattern".to_string()));
        } else if let None = self.vertices.get(start_vertex.get_id()) {
            // end vertex already exists in the pattern, use it to connect
            // add start vertex
            self.vertices
                .insert(start_vertex.get_id(), start_vertex);
            self.vertices_data
                .insert(start_vertex.get_id(), PatternVertexData::default());
        } else if let None = self.vertices.get(end_vertex.get_id()) {
            // start vertex already exists in the pattern, use it to connect
            // add end vertex
            self.vertices
                .insert(end_vertex.get_id(), end_vertex);
            self.vertices_data
                .insert(end_vertex.get_id(), PatternVertexData::default());
        }
        // update start vertex's connection info
        if let Some(start_vertex_data) = self
            .vertices_data
            .get_mut(start_vertex.get_id())
        {
            start_vertex_data
                .out_adjacencies
                .push(Adjacency::new(&start_vertex, &edge).unwrap());
        }
        // update end vertex's connection info
        if let Some(end_vertex_data) = self.vertices_data.get_mut(end_vertex.get_id()) {
            end_vertex_data
                .in_adjacencies
                .push(Adjacency::new(&end_vertex, &edge).unwrap());
        }
        // add edge to the pattern
        self.edges.insert(edge.get_id(), edge.clone());
        self.edges_data
            .insert(edge.get_id(), PatternEdgeData::default());
        Ok(())
    }

    /// Add a series of edges to the current pattern to get a new pattern
    pub fn extend_by_edges<'a, T>(&self, edges: T) -> IrResult<Pattern>
    where
        T: Iterator<Item = &'a PatternEdge>,
    {
        let mut new_pattern = self.clone();
        for edge in edges {
            new_pattern.add_edge(edge)?;
        }
        new_pattern.canonical_labeling();
        Ok(new_pattern)
    }

    /// Remove a vertex with all its adjacent edges in the current pattern
    pub fn remove_vertex(&mut self, vertex_id: PatternId) {
        if self.get_vertex(vertex_id).is_some() {
            let adjacencies: Vec<Adjacency> = self
                .adjacencies_iter(vertex_id)
                .cloned()
                .collect();
            // delete target vertex
            // delete in vertices
            self.vertices.remove(vertex_id);
            // delete in vertex tag map
            if let Some(tag) = self.get_vertex_tag(vertex_id) {
                self.tag_vertex_map.remove(&tag);
            }
            // delete in vertices data
            self.vertices_data.remove(vertex_id);
            for adjacency in adjacencies {
                let adjacent_vertex_id = adjacency.get_adj_vertex().get_id();
                let adjacent_edge_id = adjacency.get_edge_id();
                // delete adjacent edges
                // delete in edges
                self.edges.remove(adjacent_edge_id);
                // delete in edge tag map
                if let Some(tag) = self.get_edge_tag(adjacent_edge_id) {
                    self.tag_edge_map.remove(&tag);
                }
                // delete in edges data
                self.edges_data.remove(adjacent_edge_id);
                // update adjcent vertices's info
                if let PatternDirection::Out = adjacency.get_direction() {
                    self.vertices_data
                        .get_mut(adjacent_vertex_id)
                        .unwrap()
                        .in_adjacencies
                        .retain(|adj| adj.get_edge_id() != adjacent_edge_id)
                } else {
                    self.vertices_data
                        .get_mut(adjacent_vertex_id)
                        .unwrap()
                        .out_adjacencies
                        .retain(|adj| adj.get_edge_id() != adjacent_edge_id)
                }
            }

            self.canonical_labeling();
        }
    }
}
