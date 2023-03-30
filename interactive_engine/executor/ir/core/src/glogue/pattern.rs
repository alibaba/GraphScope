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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;

use ir_common::error::ParsePbError;
use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;
use vec_map::VecMap;

use crate::glogue::error::{IrPatternError, IrPatternResult};
use crate::glogue::extend_step::ExactExtendStep;
use crate::glogue::{
    combine_query_params, query_params, DynIter, PatternDirection, PatternId, PatternLabelId,
    PatternOrderTrait, PatternWeightTrait,
};
use crate::plan::meta::{PlanMeta, TagId, STORE_META};

#[derive(Debug, Clone)]
pub struct PatternVertex {
    id: PatternId,
    // Vertex label can be set when:
    // 1. with user-given required labels;
    // 2. inferred from its adjacent edges with user-given labels, together with PatternMeta
    labels: Vec<PatternLabelId>,
}

impl PatternVertex {
    pub fn new(id: PatternId) -> Self {
        PatternVertex { id, labels: vec![] }
    }

    pub fn with_label(id: PatternId, label: PatternLabelId) -> Self {
        PatternVertex { id, labels: vec![label] }
    }

    pub fn with_labels(id: PatternId, labels: Vec<PatternLabelId>) -> Self {
        PatternVertex { id, labels }
    }

    #[inline]
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    #[inline]
    pub fn get_labels(&self) -> &Vec<PatternLabelId> {
        &self.labels
    }
}

/// Each PatternVertex of a Pattern has a related PatternVertexData struct
/// - These data heavily relies on Pattern and has no meaning without a Pattern
#[derive(Debug, Clone, Default)]
struct PatternVertexData {
    /// Outgoing/Incoming/Undirected adjacent edges and vertices related to this vertex
    adjacencies: Vec<Adjacency>,
    /// Tag (alias) assigned to this vertex by user
    tag: Option<TagId>,
    /// Parameters of the vertex
    parameters: Option<pb::QueryParams>,
}

impl PatternVertexData {
    /// insert an pattern_edge as adjacency, and `is_start` denotes whether the adj vertex is the start_vertex in pattern_edge
    pub fn insert_adjacency(&mut self, pattern_edge: &PatternEdge, is_start: bool) {
        let adjacency = Adjacency::new(&pattern_edge, is_start);
        self.adjacencies.push(adjacency);
    }

    pub fn remove_adjacency(&mut self, pattern_edge_id: PatternId) {
        self.adjacencies
            .retain(|adj| adj.get_edge_id() != pattern_edge_id);
    }
}

/// A Pattern Edge from start_vertex to end_vertex
#[derive(Debug, Clone)]
pub struct PatternEdge {
    id: PatternId,
    labels: Vec<PatternLabelId>,
    start_vertex: PatternVertex,
    end_vertex: PatternVertex,
    // denote the direction from start_vertex to end_vertex, including out/in/both
    dir: PatternDirection,
}

impl PatternEdge {
    pub fn new(
        id: PatternId, labels: Vec<PatternLabelId>, start_vertex: PatternVertex, end_vertex: PatternVertex,
    ) -> PatternEdge {
        PatternEdge { id, labels, start_vertex, end_vertex, dir: PatternDirection::Out }
    }

    pub fn with_direction(mut self, direction: PatternDirection) -> Self {
        self.dir = direction;
        self
    }

    #[inline]
    pub fn get_id(&self) -> PatternId {
        self.id
    }

    #[inline]
    pub fn get_labels(&self) -> &Vec<PatternLabelId> {
        &self.labels
    }

    #[inline]
    pub fn get_start_vertex(&self) -> &PatternVertex {
        &self.start_vertex
    }

    #[inline]
    pub fn get_end_vertex(&self) -> &PatternVertex {
        &self.end_vertex
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
            PbEdgeOrPath::Path(p) => p
                .base
                .as_ref()
                .and_then(|expand_base| expand_base.edge_expand.as_ref()),
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
    /// This edge is either edge expand(only) or path expand
    data: PbEdgeOrPath,
}

/// Adjacency records a vertex's neighboring edge and vertex
#[derive(Debug, Clone)]
pub struct Adjacency {
    /// the source vertex connect to the adjacent vertex through this edge
    edge_id: PatternId,
    /// the adjacent vertex
    adj_vertex: PatternVertex,
    /// the connecting direction, including out/in/both
    direction: PatternDirection,
}

impl Adjacency {
    fn new(edge: &PatternEdge, is_start: bool) -> Adjacency {
        Adjacency {
            edge_id: edge.get_id(),
            adj_vertex: if is_start {
                edge.get_start_vertex().clone()
            } else {
                edge.get_end_vertex().clone()
            },
            direction: if is_start { edge.dir.reverse() } else { edge.dir },
        }
    }

    #[inline]
    pub fn get_edge_id(&self) -> PatternId {
        self.edge_id
    }

    #[inline]
    pub fn get_adj_vertex(&self) -> &PatternVertex {
        &self.adj_vertex
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
    /// max_tag_id denotes the max alias given by user
    max_tag_id: TagId,
}

/// Initialze a Pattern from just a single Pattern Vertex
impl From<PatternVertex> for Pattern {
    fn from(vertex: PatternVertex) -> Pattern {
        let vid = vertex.id;
        Pattern {
            edges: VecMap::new(),
            vertices: VecMap::from_iter([(vid, vertex)]),
            edges_data: VecMap::new(),
            vertices_data: VecMap::from_iter([(vid, PatternVertexData::default())]),
            max_tag_id: 0,
        }
    }
}

/// Initialize a Pattern from a vertor of Pattern Edges
impl From<Vec<PatternEdge>> for Pattern {
    fn from(edges: Vec<PatternEdge>) -> Pattern {
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
                .or_insert(edge.get_start_vertex().clone());
            // Update start vertex's adjacency info
            new_pattern
                .vertices_data
                .entry(start_vertex.get_id())
                .or_insert(PatternVertexData::default())
                .insert_adjacency(&edge, false);
            // Add or update the end vertex to the new Pattern
            let end_vertex = new_pattern
                .vertices
                .entry(edge.get_end_vertex().get_id())
                .or_insert(edge.get_end_vertex().clone());
            // Update end vertex's adjacency info
            new_pattern
                .vertices_data
                .entry(end_vertex.get_id())
                .or_insert(PatternVertexData::default())
                .insert_adjacency(&edge, true);
        }
        new_pattern
    }
}

/// Initialize a Pattern from a protobuf Pattern
impl Pattern {
    pub fn from_pb_pattern(pb_pattern: &pb::Pattern, plan_meta: &PlanMeta) -> IrPatternResult<Pattern> {
        use pb::pattern::binder::Item as BinderItem;
        // max_tag_id is the max tag has been seen in pb::Pattern (in preprocess for pattern),
        // i.e., for any tag_id that < max_tag_id is a user-given tags
        // next (system-given) vertex id assign to the vertex picked from the pb pattern
        // notice that the next_vertex_id (system-given) is the max_tag_id after processing all user-given alias in pattern
        let max_tag_id = plan_meta.get_max_tag_id();
        let mut next_vertex_id = max_tag_id as PatternId;
        // next (system-given) edge id assign to the edge picked from the pb pattern
        // Notice that we start next_edge_id from 0 because we have not support expanding edges only yet (in Intersection),
        // so we assume no user-given alias for expanding edges. i.e., g.V().match(__.as("a").outE().as("b")) is not supported.
        let mut next_edge_id = 0;
        // pattern edges picked from the pb pattern
        let mut pattern_edges = vec![];
        // record the label for each vertex from the pb pattern
        let mut vertex_labels_map: BTreeMap<PatternId, BTreeSet<PatternLabelId>> = BTreeMap::new();
        // record the vertex data from the pb pattern, i.e., pb::Select
        let mut vertex_params_map: BTreeMap<PatternId, Vec<pb::QueryParams>> = BTreeMap::new();
        // record the edge data from the pb pattern, i.e., pb::Edge or pb::Path
        let mut edge_data_map: BTreeMap<PatternId, PbEdgeOrPath> = BTreeMap::new();
        // record all the vertices after outE that haven't been closed by GetV
        // its expand direction is also recorded to check whether the futural GetV is meaningless
        let mut edge_expands_to_close = HashMap::new();
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
            let start_tag = pb_name_or_id_to_id(
                sentence
                    .start
                    .as_ref()
                    .ok_or(ParsePbError::EmptyFieldError("pb::Pattern::Sentence::start".to_string()))?,
            )?;
            // just use the start tag id as its pattern vertex id
            let start_tag_v_id = start_tag as PatternId;
            // check whether the start tag label is already determined or not
            let start_tag_label = vertex_labels_map.get(&start_tag_v_id).cloned();
            // it is allowed that the pb pattern sentence doesn't have an end tag
            let end_tag: Option<TagId> = sentence
                .end
                .as_ref()
                .map(|tag| pb_name_or_id_to_id(tag))
                .transpose()?;
            // if the end tag exists, just use the end tag id as its pattern vertex id
            let end_tag_v_id = end_tag.map(|tag| tag as PatternId);
            // check the end tag label is already determined or not
            let end_tag_label = end_tag_v_id.and_then(|v_id| vertex_labels_map.get(&v_id).cloned());
            // record previous pattern edge's destinated vertex's id
            // init as start vertex's id
            let mut pre_dst_vertex_id: PatternId = start_tag_v_id;
            // record previous pattern edge's destinated vertex's label
            // init as start vertex's label
            let mut pre_dst_vertex_label = start_tag_label;
            // find the last edge expand's index if exists;
            let last_expand_index = get_sentence_last_expand_index(sentence);
            // iterate over the binders
            for (i, binder) in sentence.binders.iter().enumerate() {
                match binder.item.as_ref() {
                    Some(BinderItem::Edge(_)) | Some(BinderItem::Path(_)) => {
                        // assign the new pattern edge with a new id
                        let edge_id = assign_id(&mut next_edge_id);
                        let edge_expand = get_edge_expand_from_binder(binder, edge_id, &mut edge_data_map)?;
                        // get edge label's id
                        let edge_labels = get_edge_expand_labels(edge_expand)?;
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
                            &mut next_vertex_id,
                        )?;
                        if let Some(BinderItem::Edge(_)) = binder.item.as_ref() {
                            if edge_expand.expand_opt == pb::edge_expand::ExpandOpt::Edge as i32 {
                                edge_expands_to_close.insert(dst_vertex_id, edge_direction);
                            }
                        } else if let Some(BinderItem::Path(_)) = binder.item.as_ref() {
                            edge_expands_to_close.insert(dst_vertex_id, edge_direction);
                        }
                        pre_dst_vertex_id = dst_vertex_id;
                        // assign vertices labels
                        // check which label candidate can connect to the previous determined partial pattern
                        let required_src_vertex_label = pre_dst_vertex_label.clone();
                        let required_dst_vertex_label =
                            if i == last_expand_index.unwrap() { end_tag_label.clone() } else { None };
                        //  infer src/dst vertex label with information of required_src_vertex_label, required_dst_vertex_label, edge_labels and pattern_meta
                        let (src_vertex_label, dst_vertex_label) = assign_src_dst_vertex_labels(
                            edge_labels.clone(),
                            edge_direction,
                            required_src_vertex_label,
                            required_dst_vertex_label,
                        )?;
                        vertex_labels_map.insert(src_vertex_id, src_vertex_label.clone());
                        vertex_labels_map.insert(dst_vertex_id, dst_vertex_label.clone());
                        pre_dst_vertex_label = Some(dst_vertex_label.clone());
                        // generate the new pattern edge and add to the pattern_edges_collection
                        let new_pattern_edge = PatternEdge::new(
                            edge_id,
                            edge_labels,
                            PatternVertex::with_labels(
                                src_vertex_id,
                                src_vertex_label.into_iter().collect(),
                            ),
                            PatternVertex::with_labels(
                                dst_vertex_id,
                                dst_vertex_label.into_iter().collect(),
                            ),
                        )
                        .with_direction(edge_direction);

                        pattern_edges.push(new_pattern_edge);
                    }
                    Some(BinderItem::Select(_select)) => {
                        // Be carefully that we assume `Select` denotes a filter for vertices;
                        // Otherwise, it should be fused into `EdgeExpand` (if it is a filter for edges).
                        // TODO: it's better to extract vertex label filter in `Select` if exists, to update id_labels_map
                        if let Some(vertex_params) = get_vertex_parameters_from_binder(binder) {
                            if !vertex_params.is_empty() {
                                vertex_params_map
                                    .entry(pre_dst_vertex_id)
                                    .or_default()
                                    .push(vertex_params)
                            };
                        }
                    }
                    Some(BinderItem::Vertex(get_v)) => {
                        // check whether the GetV's opt can match edge direction
                        // (OutE <-> EndV), (InE <-> StartV), (BothE <-> BothV)
                        if let Some(edge_direction) = edge_expands_to_close.remove(&pre_dst_vertex_id) {
                            if (edge_direction as i32 == 0 && get_v.opt != 1)
                                || (edge_direction as i32 == 1 && get_v.opt != 0)
                                || (edge_direction as i32 == 2 && get_v.opt != 2)
                            {
                                Err(IrPatternError::Unsupported(
                                    "Meaningless GetV choosing the original source vertex".to_string(),
                                ))?
                            }
                            if Some(pre_dst_vertex_id) != end_tag_v_id {
                                if let Some(new_vertex_id) = pick_vertex_id_from_get_v(get_v) {
                                    edit_vertex_id_in_translation(
                                        new_vertex_id,
                                        pre_dst_vertex_id,
                                        &mut pattern_edges,
                                        &mut vertex_labels_map,
                                        &mut vertex_params_map,
                                    );
                                    pre_dst_vertex_id = new_vertex_id;
                                }
                            }
                            if let Some(vertex_param) = get_vertex_parameters_from_binder(binder) {
                                if !vertex_param.is_empty() {
                                    vertex_params_map
                                        .entry(pre_dst_vertex_id)
                                        .or_default()
                                        .push(vertex_param);
                                }
                            }
                        } else {
                            Err(IrPatternError::ParsePbError(ParsePbError::ParseError(
                                "GetV doesn't follow corresponding Edge Expand Only Operator".to_string(),
                            )))?
                        }
                    }
                    None => {
                        return Err(
                            ParsePbError::EmptyFieldError("pb::pattern::binder::Item".to_string()).into()
                        );
                    }
                }
            }
        }
        if edge_expands_to_close.len() > 0 {
            Err(IrPatternError::Unsupported(
                "Some Expand Edge Only Operator doesn't followed by GetV to close".to_string(),
            ))?
        }
        let mut pattern = if !pattern_edges.is_empty() {
            Pattern::from(pattern_edges)
        } else {
            if vertex_params_map.is_empty() {
                Err(IrPatternError::InvalidExtendPattern("Empty Pattern".to_string()))?
            } else if vertex_params_map.len() == 1 {
                let vertex_id = *vertex_params_map.iter().last().unwrap().0;
                Pattern::from(PatternVertex::new(vertex_id))
            } else {
                Err(IrPatternError::InvalidExtendPattern("The pattern is not connected".to_string()))?
            }
        };
        for (v_id, vertex_params_vec) in vertex_params_map {
            for vertex_params in vertex_params_vec {
                pattern.add_vertex_params(v_id, vertex_params)?;
            }
        }
        for (e_id, edge_data) in edge_data_map {
            pattern.set_edge_data(e_id, edge_data)?;
        }
        pattern.set_max_tag_id(max_tag_id);
        Ok(pattern)
    }
}

/// Methods for Pattern to generate pb Logical plan of pattern matching
impl Pattern {
    /// Generate a naive extend based pattern match plan
    pub fn generate_simple_extend_match_plan(&self) -> IrPatternResult<pb::LogicalPlan> {
        if self.get_vertices_num() == 0 {
            Err(IrPatternError::InvalidExtendPattern(format!("Empty pattern {:?}", self)))?
        }
        let mut trace_pattern = self.clone();
        let mut exact_extend_steps = vec![];
        while trace_pattern.get_vertices_num() > 0 {
            let mut all_vertex_ids: Vec<PatternId> = trace_pattern
                .vertices_iter()
                .map(|v| v.get_id())
                .collect();
            if all_vertex_ids.len() == 1 {
                // Add the last vertex into exact_extend_steps
                let select_vertex_id = all_vertex_ids[0];
                let exact_extend_step =
                    ExactExtendStep::from_target_pattern(&trace_pattern, select_vertex_id)?;
                exact_extend_steps.push(exact_extend_step);
                break;
            } else {
                // Sort the vertices in a heuristic way, e.g., vertices with filters should be expanded first.
                // Unwrap here as we ensure that all the vertices are in the pattern
                all_vertex_ids.sort_by(|&v1_id, &v2_id| trace_pattern.compare(&v1_id, &v2_id).unwrap());
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
                    Err(IrPatternError::InvalidExtendPattern("The pattern is not connected".to_string()))?
                }
                let exact_extend_step =
                    ExactExtendStep::from_target_pattern(&trace_pattern, select_vertex_id)?;
                exact_extend_steps.push(exact_extend_step);
                trace_pattern.remove_vertex(select_vertex_id)?;
            }
        }
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
    let source_extend = exact_extend_steps
        .pop()
        .ok_or(IrPatternError::InvalidExtendPattern(
            "Build logical plan error: from empty extend steps!".to_string(),
        ))?;
    append_opr(&mut match_plan, generate_source_operator(origin_pattern, &source_extend)?)?;
    for exact_extend_step in exact_extend_steps.into_iter().rev() {
        let edge_expands_num = exact_extend_step.len();
        // store all the expand operators in a 2D vector
        // each 1D vector represent the expand operators correspond to a expand operator
        let expand_oprs_vec =
            exact_extend_step.generate_expand_operators_vec(origin_pattern, edge_expands_num > 1)?;
        // store the current length of the match plan
        let child_offset = match_plan.nodes.len();
        // add all the expand operators to the match plan
        for expand_oprs in expand_oprs_vec.iter() {
            for expand_opr in expand_oprs.iter() {
                append_opr(&mut match_plan, expand_opr.clone())?;
            }
        }
        // if expand num > 1, means it needs to add intersect operator, and reorganize nodes' children
        if edge_expands_num > 1 {
            // record the opr ids that are pre_node's children
            let expand_chidren = get_expand_children(&expand_oprs_vec, child_offset);
            // record the opr ids that are intersect's parents
            let intersect_parents = get_intersect_parents(&expand_oprs_vec, child_offset);
            // the id of the intersect operator
            let intersect_id = get_intersect_id(&expand_oprs_vec, child_offset);
            // add intersect operator to the match plan
            let intersect_opr = exact_extend_step.generate_intersect_operator(intersect_parents.clone())?;
            append_opr(&mut match_plan, intersect_opr)?;
            // reset the children of the previous nodes before this exact extend step
            set_node_children_at_index(&mut match_plan, expand_chidren, child_offset - 1)?;
            // reset all intersect's parents' children as the id of the intersect operator
            for parent_id in intersect_parents {
                set_node_children_at_index(&mut match_plan, vec![intersect_id], parent_id as usize)?;
            }
        }
    }
    if let Some(project_opr) = generate_project_operator(origin_pattern)? {
        append_opr(&mut match_plan, project_opr)?;
    }
    // and append the final op
    let child_offset = match_plan.nodes.len();
    set_last_node_children(&mut match_plan, vec![child_offset as KeyId])?;
    Ok(match_plan)
}

/// Append opr into match_plan in a traversal way (i.e., a->b->c->d ...).
/// Specifically, append pre_node into match_plan after set its child as the new_opr; and then update pre_node as the new_opr.
fn append_opr(
    match_plan: &mut pb::LogicalPlan, new_opr: pb::logical_plan::Operator,
) -> IrPatternResult<()> {
    let match_plan_len = match_plan.nodes.len();
    if match_plan_len > 0 {
        set_last_node_children(match_plan, vec![match_plan_len as KeyId])?;
    }
    match_plan
        .nodes
        .push(pb::logical_plan::Node { opr: Some(new_opr), children: vec![] });
    Ok(())
}

fn generate_vertex_table(
    pattern: &Pattern, vertex_id: PatternId,
) -> IrPatternResult<Vec<common_pb::NameOrId>> {
    Ok(pattern
        .get_vertex(vertex_id)?
        .get_labels()
        .clone()
        .into_iter()
        .map(|label| common_pb::NameOrId::from(label))
        .collect())
}

fn generate_source_operator(
    pattern: &Pattern, source_extend: &ExactExtendStep,
) -> IrPatternResult<pb::logical_plan::Operator> {
    let source_vertex_id = source_extend.get_target_vertex_id();
    let source_vertex_table = generate_vertex_table(pattern, source_vertex_id)?;
    // Fuse `source_vertex_label` into source, for efficiently scan
    let source_vertex_param = if let Some(mut vertex_param) = pattern
        .get_vertex_parameters(source_vertex_id)?
        .cloned()
    {
        vertex_param.tables = source_vertex_table;
        vertex_param
    } else {
        query_params(source_vertex_table, vec![], None)
    };
    let source_scan = pb::Scan {
        scan_opt: 0,
        alias: Some((source_vertex_id as KeyId).into()),
        params: Some(source_vertex_param),
        idx_predicate: None,
        meta_data: None,
    };
    Ok(source_scan.into())
}

/// Finally, if the results contain any pattern vertices with system-given aliases,
/// we additional project the user-given aliases, i.e., those may be referred later.
///
/// Here, origin_pattern.vertices.len() indicates total number of pattern vertices;
/// and origin_pattern.tag_vertex_map.len() indicates the number of pattern vertices with user-given aliases
fn generate_project_operator(pattern: &Pattern) -> IrPatternResult<Option<pb::logical_plan::Operator>> {
    let max_tag_id = pattern.get_max_tag_id() as KeyId;
    let max_vertex_id = pattern
        .get_max_vertex_id()
        .ok_or(IrPatternError::InvalidExtendPattern(format!("Empty pattern {:?}", pattern)))?
        as KeyId;
    if max_vertex_id >= max_tag_id {
        let mut mappings = Vec::with_capacity(max_tag_id as usize);
        for tag_id in 0..max_tag_id {
            let expr = str_to_expr_pb(format!("@{}", tag_id)).ok();
            let mapping = pb::project::ExprAlias { expr, alias: Some(tag_id.into()) };
            mappings.push(mapping);
        }
        // TODO: the meta_data of project is identical with the meta_data of "Pattern"
        Ok(Some(pb::Project { mappings, is_append: false, meta_data: vec![] }.into()))
    } else {
        Ok(None)
    }
}

fn get_expand_children(
    expand_oprs_vec: &Vec<Vec<pb::logical_plan::Operator>>, child_offset: usize,
) -> Vec<KeyId> {
    let mut pre_id = child_offset as KeyId;
    let mut expand_ids = vec![];
    for expand_oprs_num in expand_oprs_vec
        .iter()
        .map(|expand_oprs| expand_oprs.len())
    {
        expand_ids.push(pre_id);
        pre_id += expand_oprs_num as KeyId;
    }
    expand_ids
}

fn get_intersect_parents(
    expand_oprs_vec: &Vec<Vec<pb::logical_plan::Operator>>, child_offset: usize,
) -> Vec<KeyId> {
    let mut pre_id = (child_offset as KeyId) - 1;
    let mut intersect_ids = vec![];
    for expand_oprs_num in expand_oprs_vec
        .iter()
        .map(|expand_oprs| expand_oprs.len())
    {
        pre_id += (expand_oprs_num) as KeyId;
        intersect_ids.push(pre_id);
    }
    intersect_ids
}

fn get_intersect_id(expand_oprs_vec: &Vec<Vec<pb::logical_plan::Operator>>, child_offset: usize) -> KeyId {
    (expand_oprs_vec
        .iter()
        .map(|expand_oprs| expand_oprs.len())
        .sum::<usize>()
        + child_offset) as KeyId
}

fn set_node_children_at_index(
    match_plan: &mut pb::LogicalPlan, children: Vec<KeyId>, index: usize,
) -> IrPatternResult<()> {
    if let Some(node) = match_plan.nodes.get_mut(index) {
        node.children = children;
        Ok(())
    } else {
        Err(IrPatternError::InvalidExtendPattern(format!("Fail to get plan node at index {}", index)))
    }
}

fn set_last_node_children(match_plan: &mut pb::LogicalPlan, children: Vec<KeyId>) -> IrPatternResult<()> {
    if match_plan.nodes.len() == 0 {
        Err(IrPatternError::InvalidExtendPattern(format!("Fail to get plan node at index {}", 0)))
    } else {
        let last_index = match_plan.nodes.len() - 1;
        set_node_children_at_index(match_plan, children, last_index)
    }
}

/// Generate pattern matching order in a heuristic way:
///  1. vertex has predicates will be extended first; Specifically, predicates of eq compare is in high priority.
///  2. vertex adjacent to more edges with predicates should be extended first; Specifically, predicates of eq compare is in high priority.
///  3. vertex adjacent to more path_expand should be extended later;
///  4. vertex with larger degree will be extended later
impl PatternOrderTrait<PatternId> for Pattern {
    fn compare(&self, v1: &PatternId, v2: &PatternId) -> IrPatternResult<Ordering> {
        let v1_weight = self.get_vertex_weight(*v1)?;
        let v2_weight = self.get_vertex_weight(*v2)?;
        let vertex_order = v1_weight
            .partial_cmp(&v2_weight)
            .unwrap_or(Ordering::Equal);
        if Ordering::Equal == vertex_order {
            let v1_adjacencies_weight = self.get_adjacencies_weight(*v1)?;
            let v2_adjacencies_weight = self.get_adjacencies_weight(*v2)?;
            let adj_order = v1_adjacencies_weight
                .partial_cmp(&v2_adjacencies_weight)
                .unwrap_or(Ordering::Equal);
            if Ordering::Equal == adj_order {
                let v1_path_data_num = self
                    .adjacencies_iter(*v1)
                    .filter(|adj| self.is_pathxpd_data(adj.get_edge_id()))
                    .count();
                let v2_path_data_num = self
                    .adjacencies_iter(*v2)
                    .filter(|adj| self.is_pathxpd_data(adj.get_edge_id()))
                    .count();
                if v1_path_data_num > v2_path_data_num {
                    Ok(Ordering::Less)
                } else if v1_path_data_num < v2_path_data_num {
                    Ok(Ordering::Greater)
                } else {
                    let degree_order = self
                        .get_vertex_degree(*v1)?
                        .cmp(&self.get_vertex_degree(*v2)?);
                    if let Ordering::Equal = degree_order {
                        Ok(self
                            .get_vertex_out_degree(*v1)?
                            .cmp(&self.get_vertex_out_degree(*v2)?))
                    } else {
                        Ok(degree_order)
                    }
                }
            } else {
                Ok(adj_order)
            }
        } else {
            Ok(vertex_order)
        }
    }
}

impl PatternWeightTrait<f64> for Pattern {
    fn get_vertex_weight(&self, vid: PatternId) -> IrPatternResult<f64> {
        // VERTEX_EQ_WEIGHT has the first priority
        const PREDICATE_EQ_WEIGHT: f64 = 10.0;
        const VERTEX_PREDICATE_WEIGHT: f64 = 1.0;
        let mut vertex_weight = VERTEX_PREDICATE_WEIGHT;
        let vertex_params = self.get_vertex_parameters(vid)?;
        if let Some(predicate) = vertex_params.and_then(|params| params.predicate.as_ref()) {
            if has_expr_eq(predicate) {
                vertex_weight = PREDICATE_EQ_WEIGHT;
            }
        }
        Ok(vertex_weight)
    }

    fn get_adjacencies_weight(&self, vid: PatternId) -> IrPatternResult<f64> {
        // PREDICATE_EQ_WEIGHT has the first priority
        // Besides, EdgeExpand with predicates is in prior to PathExpand with predicates,
        // since PathExpand is assumed to expand more intermediate results.
        const PREDICATE_EQ_WEIGHT: f64 = 10.0;
        const EDGE_PREDICATE_WEIGHT: f64 = 1.0;
        const PATH_PREDICATE_WEIGHT: f64 = 0.9;
        self.get_vertex(vid)?;
        Ok(self
            .adjacencies_iter(vid)
            .map(|adj| {
                let edge_data = self.get_edge_data(adj.get_edge_id());
                let mut edge_weight = 0.0;
                if let Ok(edge_data) = edge_data {
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
            .sum())
    }
}

fn has_expr_eq(expr: &common_pb::Expression) -> bool {
    for opr in &expr.operators {
        if opr
            .item
            .eq(&Some(common_pb::expr_opr::Item::Logical(common_pb::Logical::Eq as i32)))
        {
            return true;
        }
    }
    false
}

/// Pick up EdgeExpand Operator from a binder in pb::Pattern's sentences
fn get_edge_expand_from_binder<'a, 'b>(
    binder: &'a pb::pattern::Binder, edge_id: PatternId,
    edge_data_map: &'b mut BTreeMap<PatternId, PbEdgeOrPath>,
) -> IrPatternResult<&'a pb::EdgeExpand> {
    use pb::pattern::binder::Item as BinderItem;
    if let Some(BinderItem::Path(path_expand)) = binder.item.as_ref() {
        let hop_range = path_expand
            .hop_range
            .as_ref()
            .ok_or(ParsePbError::EmptyFieldError("pb::PathExpand::hop_range".to_string()))?;
        if hop_range.lower < 1 {
            // The path with range from 0 cannot be translated to oprs that can be intersected.
            return Err(IrPatternError::Unsupported(format!(
                "PathExpand in Pattern with lower range of {:?}",
                hop_range.lower
            )))?;
        }
        let expand_base = path_expand
            .base
            .as_ref()
            .ok_or(ParsePbError::EmptyFieldError("PathExpand::base in Pattern".to_string()))?;
        let edge_expand = expand_base
            .edge_expand
            .as_ref()
            .ok_or(ParsePbError::EmptyFieldError("PathExpand::base::EdgeExpand in Pattern".to_string()))?;
        edge_data_map.insert(edge_id, PbEdgeOrPath::from(path_expand.clone()));
        Ok(edge_expand)
    } else if let Some(BinderItem::Edge(edge_expand)) = binder.item.as_ref() {
        if edge_expand.expand_opt == pb::edge_expand::ExpandOpt::Degree as i32 {
            Err(IrPatternError::Unsupported(
                "Expand Degree hasn't been supported in pattern match".to_string(),
            ))?
        }
        edge_data_map.insert(edge_id, PbEdgeOrPath::from(edge_expand.clone()));
        Ok(edge_expand)
    } else {
        Err(IrPatternError::InvalidExtendPattern(
            "Only can pick up `EdgeExpand` from BinderItem::Path and BinderItem::Edge".to_string(),
        ))?
    }
}

/// Pick up pb::QueryParams for vertex from a binder in pb::Pattern's sentences
fn get_vertex_parameters_from_binder(binder: &pb::pattern::Binder) -> Option<pb::QueryParams> {
    use pb::pattern::binder::Item as BinderItem;
    if let Some(BinderItem::Path(path_expand)) = binder.item.as_ref() {
        path_expand
            .base
            .as_ref()
            .and_then(|expand_base| expand_base.get_v.as_ref())
            .and_then(|get_v| get_v.params.as_ref())
            .cloned()
    } else if let Some(BinderItem::Select(select)) = binder.item.as_ref() {
        select
            .predicate
            .as_ref()
            .map(|expr| query_params(vec![], vec![], Some(expr.clone())))
    } else if let Some(BinderItem::Vertex(get_v)) = binder.item.as_ref() {
        get_v.params.clone()
    } else {
        None
    }
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
                let mut adjacencies = pattern.adjacencies_iter(vid);
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
pub fn pb_name_or_id_to_id(name_or_id: &common_pb::NameOrId) -> IrPatternResult<TagId> {
    match name_or_id.item {
        Some(common_pb::name_or_id::Item::Id(tag_id)) => Ok(tag_id as TagId),
        _ => {
            Err(ParsePbError::ParseError(format!("tag should be id, while it is {:?}", name_or_id)).into())
        }
    }
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
fn get_edge_expand_labels(edge_expand: &pb::EdgeExpand) -> IrPatternResult<Vec<PatternLabelId>> {
    if let Some(params) = edge_expand.params.as_ref() {
        params
            .tables
            .iter()
            .map(|label| pb_name_or_id_to_id(label).map(|l| l as PatternLabelId))
            .collect::<Result<_, _>>()
    } else {
        Ok(vec![])
    }
}

/// Assign a vertex or edge with the next_id, and add the next_id by one
/// - For a vertex or edge:
/// - - if the vertex or edge has user-given tag, should use its tag id as the pattern id
/// - - otherwise the assigned pattern id by assign_id() function
fn assign_id(next_id: &mut PatternId) -> PatternId {
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
    next_vertex_id: &mut PatternId,
) -> IrPatternResult<PatternId> {
    if is_tail {
        if let Some(v_id) = sentence_end_id {
            Ok(v_id)
        } else {
            Ok(assign_id(next_vertex_id))
        }
    } else if edge_expand.expand_opt == pb::edge_expand::ExpandOpt::Vertex as i32 {
        let dst_vertex_tag: Option<TagId> = edge_expand
            .alias
            .as_ref()
            .map(|tag| pb_name_or_id_to_id(tag))
            .transpose()?;

        // if the dst vertex has tag, just use the tag id as its pattern id
        if let Some(tag) = dst_vertex_tag {
            Ok(tag as PatternId)
        } else {
            Ok(assign_id(next_vertex_id))
        }
    } else {
        Ok(assign_id(next_vertex_id))
    }
}

/// Based on the vertex labels candidates and required src/dst vertex label,
/// assign the src and dst vertex with vertex labels meeting the requirement
fn assign_src_dst_vertex_labels(
    edge_labels: Vec<PatternLabelId>, edge_direction: pb::edge_expand::Direction,
    required_src_labels: Option<BTreeSet<PatternLabelId>>,
    required_dst_labels: Option<BTreeSet<PatternLabelId>>,
) -> IrPatternResult<(BTreeSet<PatternLabelId>, BTreeSet<PatternLabelId>)> {
    // Based on the pattern meta info, find all possible vertex labels candidates:
    // (src vertex label, dst vertex label) with the given edge label and edge direction
    // - if the edge direction is Outgoing:
    //   we use (start vertex label, end vertex label) as (src vertex label, dst vertex label)
    // - if the edge direction is Incoming:
    //   we use (end vertex label, start vertex label) as (src vertex label, dst vertex label)
    // - if the edge direction is Both:
    //   we connect the iterators returned by Outgoing and Incoming together as they all can be the candidates
    let mut candi_src_vertex_labels = BTreeSet::new();
    let mut candi_dst_vertex_labels = BTreeSet::new();

    if let Ok(store_meta) = STORE_META.read() {
        if let Some(schema) = store_meta.schema.as_ref() {
            for edge_label in edge_labels {
                if let Some(bound_labels) = schema.get_bound_labels(edge_label) {
                    for (src_label_meta, dst_label_meta) in bound_labels {
                        let src_label = src_label_meta.get_id();
                        let dst_label = dst_label_meta.get_id();
                        match edge_direction {
                            pb::edge_expand::Direction::Out => {
                                candi_src_vertex_labels.insert(src_label);
                                candi_dst_vertex_labels.insert(dst_label);
                            }
                            pb::edge_expand::Direction::In => {
                                candi_src_vertex_labels.insert(dst_label);
                                candi_dst_vertex_labels.insert(src_label);
                            }
                            pb::edge_expand::Direction::Both => {
                                candi_src_vertex_labels.insert(src_label);
                                candi_src_vertex_labels.insert(dst_label);
                                candi_dst_vertex_labels.insert(src_label);
                                candi_dst_vertex_labels.insert(dst_label);
                            }
                        }
                    }
                }
            }
        }
    }

    // For a chosen candidates:
    // - if the required src label is some, its src vertex label must match the requirement
    // - if the required dst label is some, its dst vertex label must match the requirement
    if required_src_labels.is_some() {
        candi_src_vertex_labels = if candi_src_vertex_labels.is_empty() {
            required_src_labels.unwrap()
        } else {
            candi_src_vertex_labels
                .intersection(&required_src_labels.unwrap())
                .map(|l| *l)
                .collect()
        };
    }
    if required_dst_labels.is_some() {
        candi_dst_vertex_labels = if candi_dst_vertex_labels.is_empty() {
            required_dst_labels.unwrap()
        } else {
            candi_dst_vertex_labels
                .intersection(&required_dst_labels.unwrap())
                .map(|l| *l)
                .collect()
        };
    }

    Ok((candi_src_vertex_labels, candi_dst_vertex_labels))
}

/// If the GetV operator have alias, pick it out and use at as the vertex's id
fn pick_vertex_id_from_get_v(get_v: &pb::GetV) -> Option<PatternId> {
    get_v
        .alias
        .as_ref()
        .and_then(|name_or_id| name_or_id.item.as_ref())
        .and_then(|item| {
            if let common_pb::name_or_id::Item::Id(id) = item {
                Some(*id as PatternId)
            } else {
                None
            }
        })
}

/// When GetV has alias, the vertex is corresponds to may have already been assigned an id
///
/// We use this function to edit the vertex's id during the translation
fn edit_vertex_id_in_translation(
    new_id: PatternId, old_id: PatternId, pattern_edges: &mut Vec<PatternEdge>,
    vertex_labels_map: &mut BTreeMap<PatternId, BTreeSet<PatternLabelId>>,
    vertex_params_map: &mut BTreeMap<PatternId, Vec<pb::QueryParams>>,
) {
    // Edit vertex id in pattern edges
    for pattern_edge in pattern_edges.iter_mut() {
        if pattern_edge.start_vertex.id == old_id {
            pattern_edge.start_vertex.id = new_id
        }
        if pattern_edge.end_vertex.id == old_id {
            pattern_edge.end_vertex.id = new_id
        }
    }
    // Move the labels of the old id to the new id
    if let Some(labels) = vertex_labels_map.remove(&old_id) {
        vertex_labels_map.insert(new_id, labels);
    }
    // Move the params of the old id to the new id
    if let Some(params) = vertex_params_map.remove(&old_id) {
        vertex_params_map.insert(new_id, params);
    }
}

/// Getters of fields of Pattern
impl Pattern {
    /// Get a PatternEdge struct from an edge id
    #[inline]
    pub fn get_edge(&self, edge_id: PatternId) -> IrPatternResult<&PatternEdge> {
        self.edges
            .get(edge_id)
            .ok_or(IrPatternError::MissingPatternEdge(edge_id))
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

    /// Get a PatternEdge's Tag info
    #[inline]
    pub fn get_edge_tag(&self, edge_id: PatternId) -> IrPatternResult<Option<TagId>> {
        self.edges_data
            .get(edge_id)
            .ok_or(IrPatternError::MissingPatternEdge(edge_id))
            .map(|edge_data| edge_data.tag)
    }

    /// Get the data of a PatternEdge
    #[inline]
    pub fn get_edge_data(&self, edge_id: PatternId) -> IrPatternResult<&PbEdgeOrPath> {
        self.edges_data
            .get(edge_id)
            .ok_or(IrPatternError::MissingPatternEdge(edge_id))
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
    pub fn get_vertex(&self, vertex_id: PatternId) -> IrPatternResult<&PatternVertex> {
        self.vertices
            .get(vertex_id)
            .ok_or(IrPatternError::MissingPatternVertex(vertex_id))
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

    /// Get a PatternVertex's Tag info
    #[inline]
    pub fn get_vertex_tag(&self, vertex_id: PatternId) -> IrPatternResult<Option<TagId>> {
        self.vertices_data
            .get(vertex_id)
            .ok_or(IrPatternError::MissingPatternVertex(vertex_id))
            .map(|vertex_data| vertex_data.tag)
    }

    /// Get the parameters of a PatternVertex
    #[inline]
    pub fn get_vertex_parameters(&self, vertex_id: PatternId) -> IrPatternResult<Option<&pb::QueryParams>> {
        self.vertices_data
            .get(vertex_id)
            .ok_or(IrPatternError::MissingPatternVertex(vertex_id))
            .map(|vertex_data| vertex_data.parameters.as_ref())
    }

    /// Count how many outgoing edges connect to this vertex
    #[inline]
    pub fn get_vertex_out_degree(&self, vertex_id: PatternId) -> IrPatternResult<usize> {
        let vertex_data = self
            .vertices_data
            .get(vertex_id)
            .ok_or(IrPatternError::MissingPatternVertex(vertex_id))?;
        Ok(vertex_data
            .adjacencies
            .iter()
            .filter(|adj| adj.direction.eq(&PatternDirection::Out))
            .count())
    }

    /// Count how many edges connect to this vertex
    #[inline]
    pub fn get_vertex_degree(&self, vertex_id: PatternId) -> IrPatternResult<usize> {
        self.vertices_data
            .get(vertex_id)
            .ok_or(IrPatternError::MissingPatternVertex(vertex_id))
            .map(|vertex_data| vertex_data.adjacencies.len())
    }

    pub fn get_max_tag_id(&self) -> TagId {
        self.max_tag_id
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
                .filter(move |edge| edge.get_labels().contains(&edge_label)),
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
                .filter(move |vertex| vertex.get_labels().contains(&vertex_label)),
        )
    }

    /// Iterate all adj edges (including out/in/both) of the given vertex
    pub fn adjacencies_iter(&self, vertex_id: PatternId) -> DynIter<&Adjacency> {
        if let Some(vertex_data) = self.vertices_data.get(vertex_id) {
            Box::new(vertex_data.adjacencies.iter())
        } else {
            Box::new(std::iter::empty())
        }
    }
}

/// Setters of fields of Pattern
impl Pattern {
    /// Set predicate requirement of a PatternEdge
    pub fn set_edge_data(&mut self, edge_id: PatternId, opr: PbEdgeOrPath) -> IrPatternResult<()> {
        let edge_data = self
            .edges_data
            .get_mut(edge_id)
            .ok_or(IrPatternError::MissingPatternEdge(edge_id))?;
        edge_data.data = opr;
        Ok(())
    }

    /// Set max_tag_id, which is the max user-given alias
    pub fn set_max_tag_id(&mut self, max_tag_id: TagId) {
        self.max_tag_id = max_tag_id;
    }

    /// Set predicate requirement of a PatternVertex
    pub fn add_vertex_params(
        &mut self, vertex_id: PatternId, params: pb::QueryParams,
    ) -> IrPatternResult<()> {
        let mut vertex_data = self
            .vertices_data
            .remove(vertex_id)
            .ok_or(IrPatternError::MissingPatternVertex(vertex_id))?;
        if let Some(existed_params) = vertex_data.parameters {
            vertex_data.parameters = Some(combine_query_params(existed_params, params));
        } else {
            vertex_data.parameters = Some(params);
        }
        self.vertices_data
            .insert(vertex_id, vertex_data);
        Ok(())
    }
}

/// Methods for Pattern Extension
impl Pattern {
    /// Remove a vertex with all its adjacent edges in the current pattern
    pub fn remove_vertex(&mut self, vertex_id: PatternId) -> IrPatternResult<()> {
        self.get_vertex(vertex_id)?;
        let adjacencies: Vec<Adjacency> = self
            .adjacencies_iter(vertex_id)
            .cloned()
            .collect();
        // delete target vertex
        // delete in vertices
        self.vertices.remove(vertex_id);
        // delete in vertices data
        self.vertices_data.remove(vertex_id);
        for adjacency in adjacencies {
            let adjacent_vertex_id = adjacency.get_adj_vertex().get_id();
            let adjacent_edge_id = adjacency.get_edge_id();
            // delete adjacent edges
            // delete in edges
            self.edges.remove(adjacent_edge_id);
            // delete in edges data
            self.edges_data.remove(adjacent_edge_id);
            // update adjacent vertices' info
            self.vertices_data
                .get_mut(adjacent_vertex_id)
                .ok_or(IrPatternError::MissingPatternVertex(adjacent_vertex_id))?
                .remove_adjacency(adjacent_edge_id)
        }

        Ok(())
    }
}
