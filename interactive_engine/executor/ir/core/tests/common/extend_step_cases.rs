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

use ir_core::catalogue::extend_step::*;
use ir_core::catalogue::pattern::Pattern;
use ir_core::catalogue::PatternDirection;

/// The extend step looks like:
/// ```text
///         B
///       /   \
///      A     A
/// ```
/// The left A has label id 0 and rankId 0
///
/// The right A also has label id 0 and rankId 0, the two A's are equivalent
///
/// The target vertex is B with label id 1
///
/// The two extend edges are both with edge id 1
///
/// pattern_case1 + extend_step_case1 = pattern_case2
pub fn build_extend_step_case1(pattern: &Pattern) -> ExtendStep {
    let (src_vertex_label, src_vertex_rank) = (0, 0);
    let src_vertices = pattern.get_equivalent_vertices(src_vertex_label, src_vertex_rank);
    let src_vertex1_order = pattern
        .get_vertex_rank(src_vertices[0].get_id())
        .unwrap();
    let src_vertex2_order = pattern
        .get_vertex_rank(src_vertices[1].get_id())
        .unwrap();
    let extend_edge1 = ExtendEdge::new(src_vertex1_order, 1, PatternDirection::Out);
    let extend_edge2 = ExtendEdge::new(src_vertex2_order, 1, PatternDirection::Out);
    ExtendStep::new(1, vec![extend_edge1, extend_edge2])
}

/// The extend step looks like:
/// ```text
///         C
///      /  |   \
///    A(0) A(1) B
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3
/// ```
/// Edge Label Map:
/// ```text
///     A->C: 1, B->C: 2
/// ```
/// The left A has rankId 0 and the middle A has rankId 1
pub fn build_extend_step_case2(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 3;
    let (src_vertex1_label, src_vertex1_rank) = (1, 0);
    let (src_vertex2_label, src_vertex2_rank) = (1, 1);
    let (src_vertex3_label, src_vertex3_rank) = (2, 0);
    let src_vertex1_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex1_label, src_vertex1_rank)[0].get_id())
        .unwrap();
    let src_vertex2_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex2_label, src_vertex2_rank)[0].get_id())
        .unwrap();
    let src_vertex3_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex3_label, src_vertex3_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex1_order, 1, PatternDirection::Out);
    let extend_edge_2 = ExtendEdge::new(src_vertex2_order, 1, PatternDirection::Out);
    let extend_edge_3 = ExtendEdge::new(src_vertex3_order, 2, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1, extend_edge_2, extend_edge_3])
}

/// The extend step looks like:
/// ```text
///     Person -> knows -> Person
/// ```
pub fn build_modern_extend_step_case1(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 0;
    let (src_vertex_label, src_vertex_rank) = (0, 0);
    let src_vertex_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex_label, src_vertex_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex_order, 0, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// ```text
///     Person <- knows <- Person
/// ```
pub fn build_modern_extend_step_case2(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 0;
    let (src_vertex_label, src_vertex_rank) = (0, 0);
    let src_vertex_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex_label, src_vertex_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex_order, 0, PatternDirection::In);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// ```text
///     Person -> create -> Software
/// ```
pub fn build_modern_extend_step_case3(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 1;
    let (src_vertex_label, src_vertex_rank) = (0, 0);
    let src_vertex_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex_label, src_vertex_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex_order, 1, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// ```text
///     Software <- create <- Person
/// ```
pub fn build_modern_extend_step_case4(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 0;
    let (src_vertex_label, src_vertex_rank) = (1, 0);
    let src_vertex_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex_label, src_vertex_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex_order, 1, PatternDirection::In);
    ExtendStep::new(target_v_label, vec![extend_edge_1])
}

/// The extend step looks like:
/// ```text
///            Person
///          /        \
///      create       knows
///      /               \
///   Software          Person
/// ```
pub fn build_modern_extend_step_case5(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 0;
    let (src_vertex1_label, src_vertex1_rank) = (1, 0);
    let (src_vertex2_label, src_vertex2_rank) = (0, 0);
    let src_vertex1_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex1_label, src_vertex1_rank)[0].get_id())
        .unwrap();
    let src_vertex2_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex2_label, src_vertex2_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex1_order, 1, PatternDirection::In);
    let extend_edge_2 = ExtendEdge::new(src_vertex2_order, 0, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1, extend_edge_2])
}

/// The extend step looks like:
/// ```text
///            Software
///          /          \
///      create         create
///      /                 \
///    Person            Person
/// ```
pub fn build_modern_extend_step_case6(pattern: &Pattern) -> ExtendStep {
    let target_v_label = 1;
    let (src_vertex1_label, src_vertex1_rank) = (0, 0);
    let (src_vertex2_label, src_vertex2_rank) = (0, 1);
    let src_vertex1_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex1_label, src_vertex1_rank)[0].get_id())
        .unwrap();
    let src_vertex2_order = pattern
        .get_vertex_rank(pattern.get_equivalent_vertices(src_vertex2_label, src_vertex2_rank)[0].get_id())
        .unwrap();
    let extend_edge_1 = ExtendEdge::new(src_vertex1_order, 1, PatternDirection::Out);
    let extend_edge_2 = ExtendEdge::new(src_vertex2_order, 1, PatternDirection::Out);
    ExtendStep::new(target_v_label, vec![extend_edge_1, extend_edge_2])
}
