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

use ir_common::error::ParsePbError;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use vec_map::VecMap;

use crate::catalogue::error::{IrPatternError, IrPatternResult};
use crate::catalogue::extend_step::ExactExtendStep;
use crate::catalogue::pattern_meta::PatternMeta;
use crate::catalogue::{query_params, DynIter, PatternDirection, PatternId, PatternLabelId};
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
pub enum PbEdgeOrPath {
    Edge(pb::EdgeExpand),
    Path(pb::PathExpand),
}

impl Default for PbEdgeOrPath {
    fn default() -> Self {
        PbEdgeOrPath::Edge(pb::EdgeExpand::default())
    }
}

impl From<pb::EdgeExpand> for PbEdgeOrPath {
    fn from(edge: pb::EdgeExpand) -> Self {
        PbEdgeOrPath::Edge(edge)
    }
}

impl From<pb::PathExpand> for PbEdgeOrPath {
    fn from(path: pb::PathExpand) -> Self {
        PbEdgeOrPath::Path(path)
    }
}

impl PbEdgeOrPath {
    fn get_predicates(&self) -> Option<&common_pb::Expression> {
        let edge_expand = match self {
            PbEdgeOrPath::Edge(e) => Some(e),
            PbEdgeOrPath::Path(p) => p.base.as_ref(),
        };
        edge_expand
            .and_then(|e| e.params.as_ref())
            .and_then(|params| params.predicate.as_ref())
    }

    fn get_path(&self) -> Option<&pb::PathExpand> {
        match self {
            PbEdgeOrPath::Edge(_) => None,
            PbEdgeOrPath::Path(p) => Some(p),
        }
    }
}

/// Each PatternEdge of a Pattern has a related PatternEdgeData struct
/// - These data heavily relies on Pattern and has no meaning without a Pattern
#[derive(Debug, Clone, Default)]
struct PatternEdgeData {
    /// Tag (alias) assigned to this edge by user
    tag: Option<TagId>,
    /// Predicate(filter or other expressions) this edge has
    data: PbEdgeOrPath,
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
    fn new(edge: &PatternEdge, direction: PatternDirection) -> Adjacency {
        Adjacency {
            edge_id: edge.get_id(),
            edge_label: edge.get_label(),
            adj_vertex: if direction == PatternDirection::Out {
                edge.get_end_vertex()
            } else {
                edge.get_start_vertex()
            },
            direction,
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
            tag_edge_map: BTreeMap::new(),
            tag_vertex_map: BTreeMap::new(),
        }
    }
}

/// Initialize a Pattern from a vertor of Pattern Edges
impl TryFrom<Vec<PatternEdge>> for Pattern {
    type Error = IrPatternError;

    fn try_from(edges: Vec<PatternEdge>) -> IrPatternResult<Pattern> {
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
                    .push(Adjacency::new(&edge, PatternDirection::Out));
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
                    .push(Adjacency::new(&edge, PatternDirection::In));
            }
            Ok(new_pattern)
        } else {
            Err(IrPatternError::InvalidPattern("Empty pattern".to_string()))
        }
    }
}

/// Initialize a Pattern from a protobuf Pattern
impl Pattern {
    pub fn from_pb_pattern(
        pb_pattern: &pb::Pattern, pattern_meta: &PatternMeta, plan_meta: &mut PlanMeta,
    ) -> IrPatternResult<Pattern> {
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
        let mut edge_data_map: BTreeMap<PatternId, PbEdgeOrPath> = BTreeMap::new();
        for sentence in &pb_pattern.sentences {
            if sentence.binders.is_empty() {
                return Err(
                    ParsePbError::EmptyFieldError("pb::Pattern::Sentence::binders".to_string()).into()
                );
            }
            if sentence.join_kind != pb::join::JoinKind::Inner as i32 {
                return Err(IrPatternError::Unsupported(format!("Only support join_kind of `InnerJoin` in ExtendStrategy in Pattern Match, while the join_kind is {:?}", sentence.join_kind)).into());
            }
            // pb pattern sentence must have start tag
            let start_tag = get_tag_from_name_or_id(
                sentence
                    .start
                    .clone()
                    .ok_or(ParsePbError::EmptyFieldError("pb::Pattern::Sentence::start".to_string()))?,
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
            let last_expand_index =
                get_sentence_last_expand_index(sentence).ok_or(IrPatternError::Unsupported(format!(
                    "Sentence contains only Vertex/Select in `ExtendStrategy`, the sentence is  {:?}",
                    sentence
                )))?;
            // iterate over the binders
            for (i, binder) in sentence.binders.iter().enumerate() {
                match binder.item.as_ref() {
                    Some(BinderItem::Edge(_)) | Some(BinderItem::Path(_)) => {
                        // assign the new pattern edge with a new id
                        let edge_id = assign_id(&mut next_edge_id, None);
                        let edge_expand = if let Some(BinderItem::Path(path_expand)) = binder.item.as_ref()
                        {
                            edge_data_map.insert(edge_id, PbEdgeOrPath::from(path_expand.clone()));
                            path_expand
                                .base
                                .as_ref()
                                .ok_or(ParsePbError::EmptyFieldError(
                                    "PathExpand::base in Pattern".to_string(),
                                ))?
                        } else if let Some(BinderItem::Edge(edge_expand)) = binder.item.as_ref() {
                            edge_data_map.insert(edge_id, PbEdgeOrPath::from(edge_expand.clone()));
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
                            i == last_expand_index,
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
                            if i == last_expand_index { end_tag_label } else { None };
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
                        return Err(
                            ParsePbError::EmptyFieldError("pb::pattern::binder::Item".to_string()).into()
                        );
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
    pub fn generate_simple_extend_match_plan(&self) -> IrPatternResult<pb::LogicalPlan> {
        let mut trace_pattern = self.clone();
        let mut exact_extend_steps = vec![];
        while trace_pattern.get_vertices_num() > 1 {
            let mut all_vertex_ids: Vec<PatternId> = trace_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            // Sort the vertices in a heuristic way, e.g., vertices with filters should be expanded first.
            all_vertex_ids.sort_by(|&v1_id, &v2_id| compare_vertices(v1_id, v2_id, &trace_pattern));
            // Ensure the selected vertex won't break the pattern.
            let mut select_vertex_id = usize::MAX;
            for id in all_vertex_ids {
                let connected = check_connectivity(id, &trace_pattern)?;
                if connected {
                    select_vertex_id = id;
                    break;
                }
            }
            if select_vertex_id == usize::MAX {
                Err(IrPatternError::InvalidPattern("The pattern is not connected".to_string()))?
            }
            let exact_extend_step = ExactExtendStep::from_target_pattern(&trace_pattern, select_vertex_id)?;
            exact_extend_steps.push(exact_extend_step);
            trace_pattern.remove_vertex(select_vertex_id)?;
        }
        exact_extend_steps.push(trace_pattern.try_into()?);
        build_logical_plan(self, exact_extend_steps)
    }
}

/// Build logical plan for extend based pattern match plan
///             source
///           /   |    \
///           \   |    /
///            intersect
fn build_logical_plan(
    origin_pattern: &Pattern, mut exact_extend_steps: Vec<ExactExtendStep>,
) -> IrPatternResult<pb::LogicalPlan> {
    let mut match_plan = pb::LogicalPlan::default();
    let mut child_offset: i32 = 1;
    let source_extend = match exact_extend_steps.pop() {
        Some(src_extend) => src_extend,
        None => {
            return Err(IrPatternError::InvalidPattern(
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
    let query_param = query_params(vec![source_vertex_label.into()], vec![], source_vertex_filter);
    let source_opr = pb::Scan {
        scan_opt: 0,
        alias: Some((source_vertex_id as i32).into()),
        params: Some(query_param),
        idx_predicate: None,
    };
    let mut pre_node = pb::logical_plan::Node { opr: Some(source_opr.into()), children: vec![] };
    for exact_extend_step in exact_extend_steps.into_iter().rev() {
        let edge_expands_num = exact_extend_step.len();
        // the case that needs intersection;
        if edge_expands_num > 1 {
            let mut expand_child_offset = child_offset;
            // record the opr ids that are pre_node's children
            let mut expand_ids = vec![];
            // record the opr ids that are intersect's parents
            let mut intersect_ids = vec![];
            // record the oprs to expand. e.g., [[EdgeExpand], [PathExpand, GetV, EdgeExpand]]
            let mut expand_oprs = vec![vec![]];
            for exact_extend_edge in exact_extend_step.iter() {
                let edge_id = exact_extend_edge.get_edge_id();
                let edge_data = origin_pattern
                    .get_edge_data(edge_id)
                    .cloned()
                    .ok_or(ParsePbError::EmptyFieldError("edge_data in Pattern".to_string()))?;
                match edge_data {
                    PbEdgeOrPath::Edge(edge_expand) => {
                        expand_ids.push(expand_child_offset);
                        intersect_ids.push(expand_child_offset);
                        let edge_expand_opr =
                            exact_extend_step.generate_edge_expand(exact_extend_edge, edge_expand)?;
                        expand_oprs.push(vec![edge_expand_opr]);
                        expand_child_offset += 1;
                    }
                    PbEdgeOrPath::Path(path_expand) => {
                        let path_expand_oprs =
                            exact_extend_step.generate_path_expand(exact_extend_edge, path_expand, true)?;
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
            let intersect = exact_extend_step.generate_intersect_operator(intersect_ids)?;
            pre_node = pb::logical_plan::Node { opr: Some(intersect), children: vec![] };
            child_offset += 1;
        } else if edge_expands_num == 1 {
            let definite_extend_edge = exact_extend_step
                .iter()
                .last()
                .ok_or(ParsePbError::EmptyFieldError("extend_edge in definite_extend_step".to_string()))?;
            let edge_id = definite_extend_edge.get_edge_id();
            let edge_data = origin_pattern
                .get_edge_data(edge_id)
                .cloned()
                .ok_or(ParsePbError::EmptyFieldError("edge_data in Pattern".to_string()))?;
            match edge_data {
                PbEdgeOrPath::Edge(edge_expand) => {
                    let edge_expand_opr =
                        exact_extend_step.generate_edge_expand(definite_extend_edge, edge_expand)?;
                    append_opr(&mut match_plan, &mut pre_node, edge_expand_opr, &mut child_offset);
                }
                PbEdgeOrPath::Path(path_expand) => {
                    let path_expand_oprs =
                        exact_extend_step.generate_path_expand(definite_extend_edge, path_expand, false)?;
                    for opr in path_expand_oprs {
                        append_opr(&mut match_plan, &mut pre_node, opr, &mut child_offset);
                    }
                }
            }
        } else {
            return Err(IrPatternError::InvalidPattern(
                "Build logical plan error: extend step is not source but has 0 edges".to_string(),
            ));
        }
        if let Some(filter) = exact_extend_step.generate_vertex_filter_operator(origin_pattern)? {
            append_opr(&mut match_plan, &mut pre_node, filter, &mut child_offset);
        }
    }
    // Finally, if the results contain any pattern vertices with system-given aliases,
    // we additional project the user-given aliases, i.e., those may be referred later.
    // Here, origin_pattern.vertices.len() indicates total number of pattern vertices;
    // and origin_pattern.tag_vertex_map.len() indicates the number of pattern vertices with user-given aliases
    if origin_pattern.vertices.len() > origin_pattern.tag_vertex_map.len() {
        let mut remove_tags = vec![];
        for (_, vertex) in origin_pattern.vertices.iter() {
            if !origin_pattern
                .tag_vertex_map
                .contains_key(&(vertex.id as TagId))
            {
                remove_tags.push((vertex.id as i32).into());
            }
        }
        let auxilia = pb::Auxilia {
            tag: None,
            params: Some(query_params(vec![], vec![], None)),
            alias: None,
            remove_tags,
        };
        append_opr(&mut match_plan, &mut pre_node, auxilia.into(), &mut child_offset);
    }
    // and append the final op
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

/// Compare two vertices in a heuristic way:
///  1. vertex has predicates will be extended first; Specifically, predicates of eq compare is in high priority.
///  2. vertex adjacent to more edges with predicates should be extended first; Specifically, predicates of eq compare is in high priority.
///  3. vertex adjacent to more path_expand should be extended later;
///  4. vertex with larger degree will be extended later
fn compare_vertices(v1_id: PatternId, v2_id: PatternId, trace_pattern: &Pattern) -> Ordering {
    let v1_weight = estimate_vertex_weight(v1_id, trace_pattern);
    let v2_weight = estimate_vertex_weight(v2_id, trace_pattern);
    if v1_weight > v2_weight {
        Ordering::Greater
    } else if v1_weight < v2_weight {
        Ordering::Less
    } else {
        compare_adjacencies(v1_id, v2_id, trace_pattern)
    }
}

fn compare_adjacencies(v1_id: PatternId, v2_id: PatternId, trace_pattern: &Pattern) -> Ordering {
    let v1_adjacencies_weight = estimate_adjacencies_weight(v1_id, trace_pattern);
    let v2_adjacencies_weight = estimate_adjacencies_weight(v2_id, trace_pattern);
    if v1_adjacencies_weight > v2_adjacencies_weight {
        Ordering::Greater
    } else if v1_adjacencies_weight < v2_adjacencies_weight {
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

fn has_expr_eq(expr: &common_pb::Expression) -> bool {
    let equal_opr = common_pb::ExprOpr {
        item: Some(common_pb::expr_opr::Item::Logical(0)) // eq
    };
    expr.operators.contains(&equal_opr)
}

fn estimate_vertex_weight(vid: PatternId, trace_pattern: &Pattern) -> f64 {
    // VERTEX_EQ_WEIGHT has the first priority
    const PREDICATE_EQ_WEIGHT: f64 = 10.0;
    const VERTEX_PREDICATE_WEIGHT: f64 = 1.0;
    let mut vertex_weight = 0.0;
    if let Some(vertex_data) = trace_pattern.get_vertex_data(vid) {
        if let Some(predicate) = &vertex_data.predicate {
            if has_expr_eq(predicate) {
                vertex_weight = PREDICATE_EQ_WEIGHT;
            } else {
                vertex_weight = VERTEX_PREDICATE_WEIGHT;
            }
        }
    }
    vertex_weight
}

fn estimate_adjacencies_weight(vid: PatternId, trace_pattern: &Pattern) -> f64 {
    // PREDICATE_EQ_WEIGHT has the first priority
    // Besides, EdgeExpand with predicates is in prior to PathExpand with predicates,
    // since PathExpand is assumed to expand more intermediate results.
    const PREDICATE_EQ_WEIGHT: f64 = 10.0;
    const EDGE_PREDICATE_WEIGHT: f64 = 1.0;
    const PATH_PREDICATE_WEIGHT: f64 = 0.9;
    trace_pattern
        .adjacencies_iter(vid)
        .map(|adj| {
            let edge_data = trace_pattern.get_edge_data(adj.get_edge_id());
            let mut edge_weight = 0.0;
            if let Some(edge_data) = edge_data {
                let hop_num = if let Some(path) = edge_data.get_path() {
                    path.hop_range.as_ref().unwrap().lower as u32
                } else {
                    1
                };
                if let Some(edge_predicate) = edge_data.get_predicates() {
                    edge_weight = if has_expr_eq(edge_predicate) {
                        PREDICATE_EQ_WEIGHT
                    } else {
                        if hop_num == 1 {
                            EDGE_PREDICATE_WEIGHT
                        } else {
                            PATH_PREDICATE_WEIGHT.powf(hop_num as f64)
                        }
                    };
                }
            }
            edge_weight
        })
        .sum()
}

/// check if the pattern is still connected by removing `vertex_to_remove`
fn check_connectivity(vertex_to_remove: PatternId, pattern: &Pattern) -> IrPatternResult<bool> {
    let mut visited = vec![false; pattern.get_max_vertex_id().unwrap() + 1];
    let mut s = vec![];
    for vertex in pattern.vertices_iter() {
        let vid = vertex.get_id();
        if vid != vertex_to_remove {
            visited[vid] = true;
            s.push(vid);
            while !s.is_empty() {
                let vid = s.pop().unwrap();
                let vertex_data = pattern
                    .vertices_data
                    .get(vid)
                    .ok_or(IrPatternError::MissingPatternVertex(vid))?;
                let mut adjacencies = vertex_data
                    .out_adjacencies
                    .iter()
                    .chain(&vertex_data.in_adjacencies);
                while let Some(adj) = adjacencies.next() {
                    let adj_id = adj.adj_vertex.get_id();
                    if adj_id != vertex_to_remove && !visited[adj_id] {
                        s.push(adj_id);
                        visited[adj_id] = true;
                    }
                }
            }
            break;
        }
    }
    let mut all_visited = true;
    for vertex in pattern.vertices_iter() {
        let id = vertex.get_id();
        if id != vertex_to_remove && !visited[id] {
            all_visited = false;
        }
    }

    Ok(all_visited)
}

/// Get the tag info from the given name_or_id
/// - in pb::Pattern transformation, tag is required to be id instead of str
fn get_tag_from_name_or_id(name_or_id: common_pb::NameOrId) -> IrPatternResult<TagId> {
    let tag: ir_common::NameOrId = name_or_id.try_into()?;
    match tag {
        ir_common::NameOrId::Id(tag_id) => Ok(tag_id as TagId),
        _ => Err(ParsePbError::ParseError(format!("tag should be id, while it is {:?}", tag)).into()),
    }
}

/// Get all the tags from the pb Pattern and store in a set.
/// Notice that these tags are user-given tags.
fn get_all_tags_from_pb_pattern(pb_pattern: &pb::Pattern) -> IrPatternResult<BTreeSet<TagId>> {
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
fn get_edge_expand_label(edge_expand: &pb::EdgeExpand) -> IrPatternResult<PatternLabelId> {
    if edge_expand.expand_opt != pb::edge_expand::ExpandOpt::Vertex as i32 {
        return Err(IrPatternError::Unsupported("Expand edge in pattern".to_string()));
    }
    if let Some(params) = edge_expand.params.as_ref() {
        // TODO: Support Fuzzy Pattern
        if params.tables.is_empty() {
            return Err(IrPatternError::Unsupported(
                "FuzzyPattern: no specific edge expand label".to_string(),
            ));
        } else if params.tables.len() > 1 {
            return Err(IrPatternError::Unsupported(
                "FuzzyPattern: more than 1 edge expand label".to_string(),
            ));
        }
        // get edge label's id
        match params.tables[0].item.as_ref() {
            Some(common_pb::name_or_id::Item::Id(e_label_id)) => Ok(*e_label_id),
            _ => {
                Err(IrPatternError::InvalidPattern("edge expand doesn't have valid label".to_string())
                    .into())
            }
        }
    } else {
        Err(ParsePbError::EmptyFieldError("pb::EdgeExpand.params".to_string()).into())
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
) -> IrPatternResult<PatternId> {
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
) -> IrPatternResult<(PatternLabelId, PatternLabelId, PatternDirection)> {
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
                return Err(IrPatternError::Unsupported(
                    "Both Direction leads to Fuzzy Pattern".to_string(),
                ));
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
        Err(IrPatternError::InvalidPattern("Cannot find valid label for some vertices".to_string()).into())
    } else if vertex_label_candis.len() > 1 {
        Err(IrPatternError::Unsupported(
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

    /// Get a PatternEdge's Tag info
    #[inline]
    pub fn get_edge_tag(&self, edge_id: PatternId) -> Option<TagId> {
        self.edges_data
            .get(edge_id)
            .and_then(|edge_data| edge_data.tag)
    }

    /// Get the data of a PatternEdge
    #[inline]
    pub fn get_edge_data(&self, edge_id: PatternId) -> Option<&PbEdgeOrPath> {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| &edge_data.data)
    }

    /// test if the edge data is a path
    #[inline]
    pub fn is_pathxpd_data(&self, edge_id: PatternId) -> bool {
        self.edges_data
            .get(edge_id)
            .map(|edge_data| (&edge_data.data).get_path().is_some())
            .unwrap_or(false)
    }

    /// Get a PatternVertex struct from a vertex id
    #[inline]
    pub fn get_vertex(&self, vertex_id: PatternId) -> Option<&PatternVertex> {
        self.vertices.get(vertex_id)
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
    pub fn get_min_vertex_id(&self) -> Option<PatternId> {
        self.vertices
            .iter()
            .map(|(vertex_id, _)| vertex_id)
            .next()
    }

    #[inline]
    pub fn get_max_vertex_id(&self) -> Option<PatternId> {
        self.vertices
            .iter()
            .map(|(vertex_id, _)| vertex_id)
            .last()
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
    pub fn edges_with_tag_iter(&self) -> DynIter<Option<&PatternEdge>> {
        Box::new(
            self.tag_edge_map
                .iter()
                .map(move |(_, &edge_id)| self.get_edge(edge_id)),
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
                .filter_map(move |(_, &vertex_id)| self.get_vertex(vertex_id)),
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
    /// Assign a PatternEdge of the Pattern with the Given Tag
    pub fn set_edge_tag(&mut self, edge_tag: TagId, edge_id: PatternId) {
        // If the tag is previously assigned to another edge, remove it
        if let Some(&old_edge_id) = self.tag_edge_map.get(&edge_tag) {
            self.edges_data
                .get_mut(old_edge_id)
                .map(|e| e.tag = None);
        }
        // Assign the tag to the edge
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            self.tag_edge_map.insert(edge_tag, edge_id);
            edge_data.tag = Some(edge_tag);
        }
    }

    /// Set predicate requirement of a PatternEdge
    pub fn set_edge_data(&mut self, edge_id: PatternId, opr: PbEdgeOrPath) {
        if let Some(edge_data) = self.edges_data.get_mut(edge_id) {
            edge_data.data = opr;
        }
    }

    /// Assign a PatternVertex with the given tag
    pub fn set_vertex_tag(&mut self, vertex_id: PatternId, vertex_tag: TagId) {
        // If the tag is previously assigned to another vertex, remove it
        if let Some(&old_vertex_id) = self.tag_vertex_map.get(&vertex_tag) {
            self.vertices_data
                .get_mut(old_vertex_id)
                .map(|v| v.tag = None);
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

/// Methods for Pattern Extension
impl Pattern {
    /// Remove a vertex with all its adjacent edges in the current pattern
    pub fn remove_vertex(&mut self, vertex_id: PatternId) -> IrPatternResult<()> {
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
                        .ok_or(IrPatternError::MissingPatternVertex(adjacent_vertex_id))?
                        .in_adjacencies
                        .retain(|adj| adj.get_edge_id() != adjacent_edge_id)
                } else {
                    self.vertices_data
                        .get_mut(adjacent_vertex_id)
                        .ok_or(IrPatternError::MissingPatternVertex(adjacent_vertex_id))?
                        .out_adjacencies
                        .retain(|adj| adj.get_edge_id() != adjacent_edge_id)
                }
            }

            Ok(())
        } else {
            Err(IrPatternError::MissingPatternVertex(vertex_id))
        }
    }
}
