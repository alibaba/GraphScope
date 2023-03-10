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

use std::collections::{BTreeSet, HashMap};

use ir_common::expr_parse::str_to_expr_pb;
use ir_common::generated::algebra as pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;
use ir_core::glogue::error::IrPatternResult;
use ir_core::glogue::pattern::*;
use ir_core::glogue::{PatternId, PatternLabelId};
use ir_core::plan::meta::{PlanMeta, TagId};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use crate::common::pattern_meta_cases::*;

pub const TAG_A: KeyId = 0;
pub const TAG_B: KeyId = 1;
pub const TAG_C: KeyId = 2;
pub const TAG_D: KeyId = 3;

fn gen_edge_label_map(edges: Vec<String>) -> HashMap<String, PatternLabelId> {
    let mut rng = StdRng::from_seed([0; 32]);
    let mut values: Vec<PatternLabelId> = (0..=255).collect();
    values.shuffle(&mut rng);
    let mut edge_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut rank = 0;
    for edge in edges {
        if rank >= values.len() {
            panic!("Error in gen_edge_label_map: rank of out of scope");
        }
        edge_label_map.insert(edge, values[rank]);
        rank += 1;
    }

    edge_label_map
}

fn gen_group_ids(max_id: PatternId) -> Vec<PatternId> {
    let mut rng = rand::thread_rng();
    let mut ids: Vec<PatternId> = (0..=max_id).collect();
    ids.shuffle(&mut rng);
    ids
}

pub fn query_params(
    tables: Vec<common_pb::NameOrId>, columns: Vec<common_pb::NameOrId>,
    predicate: Option<common_pb::Expression>,
) -> pb::QueryParams {
    pb::QueryParams {
        tables,
        columns,
        is_all_columns: false,
        limit: None,
        predicate,
        sample_ratio: 1.0,
        extra: HashMap::new(),
    }
}

fn new_pattern_edge(
    id: PatternId, label: PatternLabelId, start_v_id: PatternId, end_v_id: PatternId,
    start_v_label: PatternLabelId, end_v_label: PatternLabelId,
) -> PatternEdge {
    let start_vertex = PatternVertex::with_label(start_v_id, start_v_label);
    let end_vertex = PatternVertex::with_label(end_v_id, end_v_label);
    PatternEdge::new(id, vec![label], start_vertex, end_vertex)
}

fn get_max_tag_id(pattern: &pb::Pattern) -> TagId {
    use pb::pattern::binder::Item as BinderItem;
    let mut tag_id_set = BTreeSet::new();
    for sentence in pattern.sentences.iter() {
        if let Some(start_tag) = sentence.start.as_ref() {
            let start_tag_id = pb_name_or_id_to_id(start_tag).unwrap();
            tag_id_set.insert(start_tag_id);
        }
        if let Some(end_tag) = sentence.end.as_ref() {
            let end_tag_id = pb_name_or_id_to_id(end_tag).unwrap();
            tag_id_set.insert(end_tag_id);
        }
        for binder in sentence.binders.iter() {
            let alias = match binder.item.as_ref() {
                Some(BinderItem::Edge(edge_expand)) => edge_expand.alias.as_ref(),
                Some(BinderItem::Path(path_expand)) => path_expand.alias.as_ref(),
                Some(BinderItem::Vertex(get_v)) => get_v.alias.as_ref(),
                _ => None,
            };
            if let Some(tag) = alias {
                let tag_id = pb_name_or_id_to_id(tag).unwrap();
                tag_id_set.insert(tag_id);
            }
        }
    }
    tag_id_set.len() as TagId
}

fn gen_plan_meta(pattern: &pb::Pattern) -> PlanMeta {
    let mut plan_meta = PlanMeta::default();
    plan_meta.set_max_tag_id(get_max_tag_id(pattern));
    plan_meta
}

/// The pattern looks like:
/// ```text
///      A
/// ```
/// where it is a single vertex pattern without any edges
///
/// A's label's id is 0
///
pub fn build_pattern_case0() -> Pattern {
    let vertex = PatternVertex::with_label(0, 0);
    Pattern::from(vertex)
}

/// The pattern looks like:
/// ```text
///      A <-> A
/// ```
/// where <-> means two edges
///
/// A's label's id is 0
///
/// The edges's labels' id are both 0
///
/// The left A has id 0
///
/// The right A has id 1
pub fn build_pattern_case1() -> Pattern {
    let vertex_1 = PatternVertex::with_label(0, 0);
    let vertex_2 = PatternVertex::with_label(1, 0);
    let edge_1 = PatternEdge::new(0, vec![0], vertex_1.clone(), vertex_2.clone());
    let edge_2 = PatternEdge::new(1, vec![0], vertex_2, vertex_1);
    let pattern_vec = vec![edge_1, edge_2];
    Pattern::from(pattern_vec)
}

/// The pattern looks like:
/// ```text
///         B
///       /   \
///      A <-> A
/// ```
/// where <-> means two edges
///
/// A's label id is 0, B's label id is 1
///
/// The edges between two As have label id 0
///
/// The edges between A and B have label id 1
///
/// The left A has id 0
///
/// The right A has id 1
///
/// B has id 2
pub fn build_pattern_case2() -> Pattern {
    let pattern_vec = vec![
        new_pattern_edge(0, 0, 0, 1, 0, 0),
        new_pattern_edge(1, 0, 1, 0, 0, 0),
        new_pattern_edge(2, 1, 0, 2, 0, 1),
        new_pattern_edge(3, 1, 1, 2, 0, 1),
    ];
    Pattern::from(pattern_vec)
}

/// The pattern looks like:
/// ```text
///    B(2) -> B(3)
///     |       |
///     A(0) -> A(1)
/// ```
/// where <-> means two edges
///
/// Vertex Label Map:
/// ```text
///     A: 0, B: 1,
/// ```
/// Edge Label Map:
/// ```text
///     A-A: 0, A->B: 1, B-B: 2,
/// ```
pub fn build_pattern_case3() -> Pattern {
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(edge_ids[0], 0, 0, 1, 0, 0),
        new_pattern_edge(edge_ids[1], 1, 0, 2, 0, 1),
        new_pattern_edge(edge_ids[2], 1, 1, 3, 0, 1),
        new_pattern_edge(edge_ids[3], 2, 2, 3, 1, 1),
    ];
    Pattern::from(pattern_vec)
}

/// The pattern looks like:
/// ```text
///     B(2)  -> B(3)
///     |        |
///     A(0) <-> A(1)
/// ```
/// where <-> means two edges
///
/// Vertex Label Map:
/// ```text
///     A: 0, B: 1,
/// ```
/// Edge Label Map:
/// ```text
///     A-A: 0, A->B: 1, B-B: 2,
/// ```
pub fn build_pattern_case4() -> Pattern {
    let edge_ids = gen_group_ids(4);
    let pattern_vec = vec![
        new_pattern_edge(edge_ids[0], 0, 0, 1, 0, 0),
        new_pattern_edge(edge_ids[1], 0, 1, 0, 0, 0),
        new_pattern_edge(edge_ids[2], 1, 0, 2, 0, 1),
        new_pattern_edge(edge_ids[3], 1, 1, 3, 0, 1),
        new_pattern_edge(edge_ids[4], 2, 2, 3, 1, 1),
    ];
    Pattern::from(pattern_vec)
}

/// The pattern looks like
/// ```text
///         A(0) -> B(0)    A(1) <-> A(2)
///         |               |
/// C(0) -> B(1) <- A(3) -> B(2) <- C(1) <- D(0)
///         |
///         C(2)
/// ```
/// where <-> means bidirectional edges
///
/// Vertex Label Map
/// ```text
///     A: 3, B: 2, C: 1, D: 0
/// ```
/// Edge Label Map:
/// ```text
///     A-A: 20, A-B: 30, B-C: 15, C-D: 5
/// ```
pub fn build_pattern_case5() -> Pattern {
    let label_a = 3;
    let label_b = 2;
    let label_c = 1;
    let label_d = 0;
    let id_vec_a: Vec<PatternId> = vec![100, 200, 300, 400];
    let id_vec_b: Vec<PatternId> = vec![10, 20, 30];
    let id_vec_c: Vec<PatternId> = vec![1, 2, 3];
    let id_vec_d: Vec<PatternId> = vec![1000];
    let edge_ids = gen_group_ids(10);
    let pattern_vec = vec![
        new_pattern_edge(edge_ids[0], 15, id_vec_c[0], id_vec_b[1], label_c, label_b),
        new_pattern_edge(edge_ids[1], 30, id_vec_a[0], id_vec_b[1], label_a, label_b),
        new_pattern_edge(edge_ids[2], 15, id_vec_c[2], id_vec_b[1], label_c, label_b),
        new_pattern_edge(edge_ids[3], 30, id_vec_a[0], id_vec_b[0], label_a, label_b),
        new_pattern_edge(edge_ids[4], 30, id_vec_a[3], id_vec_b[1], label_a, label_b),
        new_pattern_edge(edge_ids[5], 30, id_vec_a[3], id_vec_b[2], label_a, label_b),
        new_pattern_edge(edge_ids[6], 30, id_vec_a[1], id_vec_b[2], label_a, label_b),
        new_pattern_edge(edge_ids[7], 20, id_vec_a[1], id_vec_a[2], label_a, label_a),
        new_pattern_edge(edge_ids[8], 20, id_vec_a[2], id_vec_a[1], label_a, label_a),
        new_pattern_edge(edge_ids[9], 15, id_vec_c[1], id_vec_b[2], label_c, label_b),
        new_pattern_edge(edge_ids[10], 5, id_vec_d[0], id_vec_c[1], label_d, label_c),
    ];
    Pattern::from(pattern_vec)
}

/// The pattern looks like:
/// ```text
///     B <- A -> C
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3
/// ```
/// Edge Label Map:
/// ```text
///     A->B: 1, A->C: 2
/// ```
pub fn build_pattern_case6() -> Pattern {
    let pattern_edge1 = new_pattern_edge(0, 1, 0, 1, 1, 2);
    let pattern_edge2 = new_pattern_edge(1, 2, 0, 2, 1, 3);
    let pattern_vec = vec![pattern_edge1, pattern_edge2];
    Pattern::from(pattern_vec)
}

/// The pattern looks like:
/// ```text
///         A
///        /|\
///       / D \
///      //  \ \
///     B  ->  C
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3, D: 4
/// ```
/// Edge Label Map:
/// ```text
///     A->B: 0, A->C: 1, B->C: 2, A->D: 3, B->D: 4, D->C: 5
/// ```
pub fn build_pattern_case7() -> Pattern {
    let edge_1 = new_pattern_edge(0, 1, 0, 1, 1, 2);
    let edge_2 = new_pattern_edge(1, 2, 0, 2, 1, 3);
    let edge_3 = new_pattern_edge(2, 3, 1, 2, 2, 3);
    let edge_4 = new_pattern_edge(3, 4, 0, 3, 1, 4);
    let edge_5 = new_pattern_edge(4, 5, 1, 3, 2, 4);
    let edge_6 = new_pattern_edge(5, 6, 3, 2, 4, 3);
    let pattern_edges = vec![edge_1, edge_2, edge_3, edge_4, edge_5, edge_6];
    Pattern::from(pattern_edges)
}

/// The pattern looks like:
/// ```text
///     A -> A -> B
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2
/// ```
/// Edge Label Map:
/// ```text
///     A->A: 0, A->B: 3
/// ```
pub fn build_pattern_case8() -> Pattern {
    let edge_1 = new_pattern_edge(0, 0, 0, 1, 1, 1);
    let edge_2 = new_pattern_edge(1, 3, 1, 2, 1, 2);
    let pattern_edges = vec![edge_1, edge_2];
    Pattern::from(pattern_edges)
}

/// The pattern looks like:
/// ```text
///          C
///       /  |   \
///     A -> A -> B
/// ```
/// Vertex Label Map:
/// ```text
///     A: 1, B: 2, C: 3
/// ```
/// Edge Label Map:
/// ```text
///     A->A: 0, A->C: 1, B->C: 2, A->B: 3
/// ```
pub fn build_pattern_case9() -> Pattern {
    let edge_1 = new_pattern_edge(0, 0, 0, 1, 1, 1);
    let edge_2 = new_pattern_edge(1, 3, 1, 2, 1, 2);
    let edge_3 = new_pattern_edge(2, 1, 0, 3, 1, 3);
    let edge_4 = new_pattern_edge(3, 1, 1, 3, 1, 3);
    let edge_5 = new_pattern_edge(4, 2, 2, 3, 2, 3);
    let pattern_edges = vec![edge_1, edge_2, edge_3, edge_4, edge_5];
    Pattern::from(pattern_edges)
}

/// Pattern from modern schema file
///
/// Person only Pattern
pub fn build_modern_pattern_case1() -> Pattern {
    Pattern::from(PatternVertex::with_label(0, 0))
}

/// Software only Pattern
pub fn build_modern_pattern_case2() -> Pattern {
    Pattern::from(PatternVertex::with_label(0, 1))
}

/// The pattern looks like:
/// ```text
///     Person -> knows -> Person
/// ```
pub fn build_modern_pattern_case3() -> Pattern {
    let pattern_edge = new_pattern_edge(0, 0, 0, 1, 0, 0);
    Pattern::from(vec![pattern_edge])
}

/// The pattern looks like:
/// ```text
///     Person -> created -> Software
/// ```
pub fn build_modern_pattern_case4() -> Pattern {
    let pattern_edge = new_pattern_edge(0, 1, 0, 1, 0, 1);
    Pattern::from(vec![pattern_edge])
}

/// The pattern looks like:
///```text
///           Software
///   create/         \create
///  Person -> knows -> Person
/// ```
/// create and knows are edge labels
///
/// Software and Person are vertex labels
pub fn build_modern_pattern_case5() -> Pattern {
    let pattern_edge1 = new_pattern_edge(0, 0, 0, 1, 0, 0);
    let pattern_edge2 = new_pattern_edge(1, 1, 0, 2, 0, 1);
    let pattern_edge3 = new_pattern_edge(2, 1, 1, 2, 0, 1);
    Pattern::from(vec![pattern_edge1, pattern_edge2, pattern_edge3])
}

/// Pattern from ldbc schema file
/// ```text
///     Person -> knows -> Person
/// ```
pub fn build_ldbc_pattern_case1() -> Pattern {
    let pattern_edge = new_pattern_edge(0, 12, 0, 1, 1, 1);
    Pattern::from(vec![pattern_edge])
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///      Person
/// ```
/// where it is a single vertex pattern without any edges
///
///  Person is the vertex label
///
pub fn build_ldbc_pattern_from_pb_case0() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let vertx_opr = pb::Select { predicate: Some(str_to_expr_pb("@.~label==1".to_string()).unwrap()) };
    let pattern = pb::Pattern {
        sentences: vec![pb::pattern::Sentence {
            start: Some(TAG_A.into()),
            binders: vec![pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Select(vertx_opr)) }],
            end: Some(TAG_A.into()),
            join_kind: 0,
        }],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///           Person
///     knows/      \knows
///     Person  ->  Person
/// ```
/// knows is the edge label
///
/// Person is the vertex label
pub fn build_ldbc_pattern_from_pb_case1() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let expand_opr = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///           University
///     study at/      \study at
///        Person  ->   Person
/// ```
pub fn build_ldbc_pattern_from_pb_case2() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![15.into()], vec![], None)), //STUDYAT
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![15.into()], vec![], None)), //STUDYAT
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr3 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// 4 Persons know each other
pub fn build_ldbc_pattern_from_pb_case3() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let expand_opr = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_C.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
        ],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///             City
///      lives/     \lives
///     Person      Person
///     likes \      / has creator
///           Comment
/// ```
pub fn build_ldbc_pattern_from_pb_case4() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr3 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![13.into()], vec![], None)), //LIKES
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr4 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr1)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2)),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr3)),
                }],
                end: Some(TAG_D.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_D.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr4)),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
        ],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

/// Pattern from ldbc schema file and build from pb::Pattern message
/// ```text
///           Person
///     knows/      \knows
///    knows/       \knows
///   Person knows->knows Person
/// ```
/// knows->knows represents there are two edges with "knows" label between two people
pub fn build_ldbc_pattern_from_pb_case5() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let expand_opr0 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                ],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

pub fn build_ldbc_pattern_from_pb_case6() -> IrPatternResult<Pattern> {
    set_ldbc_graph_schema();
    // define pb pattern message
    let expand_opr0 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr1 = pb::EdgeExpand {
        v_tag: None,
        direction: 1,                                              // in
        params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr2 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr3 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                              // out
        params: Some(query_params(vec![13.into()], vec![], None)), //LIKES
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let expand_opr4 = pb::EdgeExpand {
        v_tag: None,
        direction: 0,                                             // out
        params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
        expand_opt: 0,
        alias: None,
        meta_data: None,
    };
    let pattern = pb::Pattern {
        sentences: vec![
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    },
                ],
                end: Some(TAG_B.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr3.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr4.clone())),
                    },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
            pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                }],
                end: Some(TAG_C.into()),
                join_kind: 0,
            },
        ],
        meta_data: vec![],
    };
    let plan_meta = gen_plan_meta(&pattern);
    Pattern::from_pb_pattern(&pattern, &plan_meta)
}

// Test Cases for Index Ranking
pub fn build_pattern_rank_ranking_case1() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(1);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    let pattern_vec = vec![new_pattern_edge(
        0,
        *edge_label_map.get("A->A").unwrap(),
        *vertex_id_map.get("A0").unwrap(),
        *vertex_id_map.get("A1").unwrap(),
        *vertex_label_map.get("A").unwrap(),
        *vertex_label_map.get("A").unwrap(),
    )];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case2() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(1);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    let edge_ids = gen_group_ids(1);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case3() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    let edge_ids = gen_group_ids(1);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case4() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    let edge_ids = gen_group_ids(2);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case5() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    let edge_ids = gen_group_ids(2);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case6() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->A")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("B->A").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case7() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(2);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case8() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(3);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    let edge_ids = gen_group_ids(3);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case9() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(3);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    let edge_ids = gen_group_ids(4);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case10() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(3);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    let edge_ids = gen_group_ids(5);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case11() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(4);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[4]);
    let edge_ids = gen_group_ids(6);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case12() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->B")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B3"), vertex_ids[5]);
    let edge_ids = gen_group_ids(7);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B3").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case13() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("B->B"),
        String::from("B->A"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[5]);
    let edge_ids = gen_group_ids(7);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("B->A").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case14() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("B->B"),
        String::from("B->C"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    let vertex_ids = gen_group_ids(6);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[2]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B3"), vertex_ids[5]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[6]);
    let edge_ids = gen_group_ids(8);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("B->B").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("B3").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case15() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    let vertex_ids = gen_group_ids(9);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[5]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[6]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[8]);
    vertex_id_map.insert(String::from("C1"), vertex_ids[9]);
    let edge_ids = gen_group_ids(8);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("C1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case16() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("B->C"),
        String::from("C->D"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    vertex_label_map.insert(String::from("D"), 4);
    let vertex_ids = gen_group_ids(9);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[4]);
    vertex_id_map.insert(String::from("B1"), vertex_ids[5]);
    vertex_id_map.insert(String::from("B2"), vertex_ids[6]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[7]);
    vertex_id_map.insert(String::from("C1"), vertex_ids[8]);
    vertex_id_map.insert(String::from("D0"), vertex_ids[9]);
    let edge_ids = gen_group_ids(9);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("B->C").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_id_map.get("C1").unwrap(),
            *vertex_label_map.get("B").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("B2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("B1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("C->D").unwrap(),
            *vertex_id_map.get("C1").unwrap(),
            *vertex_id_map.get("D0").unwrap(),
            *vertex_label_map.get("C").unwrap(),
            *vertex_label_map.get("D").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    let edge_ids = gen_group_ids(4);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_even_num_chain() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(6);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    vertex_id_map.insert(String::from("A6"), vertex_ids[6]);
    let edge_ids = gen_group_ids(5);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_long_chain() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(10);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    vertex_id_map.insert(String::from("A6"), vertex_ids[6]);
    vertex_id_map.insert(String::from("A7"), vertex_ids[7]);
    vertex_id_map.insert(String::from("A8"), vertex_ids[8]);
    vertex_id_map.insert(String::from("A9"), vertex_ids[9]);
    vertex_id_map.insert(String::from("A10"), vertex_ids[10]);
    let edge_ids = gen_group_ids(10);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_id_map.get("A10").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_special_id_situation_1() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_id_map.insert(String::from("A0"), 2);
    vertex_id_map.insert(String::from("A1"), 5);
    vertex_id_map.insert(String::from("A2"), 3);
    vertex_id_map.insert(String::from("A3"), 0);
    vertex_id_map.insert(String::from("A4"), 1);
    vertex_id_map.insert(String::from("A5"), 4);
    let pattern_vec = vec![
        new_pattern_edge(
            2,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            3,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            1,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            4,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            0,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case17_special_id_situation_2() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_id_map.insert(String::from("A0"), 2);
    vertex_id_map.insert(String::from("A1"), 1);
    vertex_id_map.insert(String::from("A2"), 3);
    vertex_id_map.insert(String::from("A3"), 0);
    vertex_id_map.insert(String::from("A4"), 4);
    vertex_id_map.insert(String::from("A5"), 5);
    let pattern_vec = vec![
        new_pattern_edge(
            0,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            1,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            2,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            4,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            3,
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case18() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(5);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    let edge_ids = gen_group_ids(5);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case19() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> = gen_edge_label_map(vec![
        String::from("A->A"),
        String::from("A->B"),
        String::from("A->C"),
        String::from("A->D"),
        String::from("A->E"),
    ]);
    vertex_label_map.insert(String::from("A"), 1);
    vertex_label_map.insert(String::from("B"), 2);
    vertex_label_map.insert(String::from("C"), 3);
    vertex_label_map.insert(String::from("D"), 4);
    vertex_label_map.insert(String::from("E"), 5);
    let vertex_ids = gen_group_ids(13);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    vertex_id_map.insert(String::from("A5"), vertex_ids[5]);
    vertex_id_map.insert(String::from("A6"), vertex_ids[6]);
    vertex_id_map.insert(String::from("A7"), vertex_ids[7]);
    vertex_id_map.insert(String::from("A8"), vertex_ids[8]);
    vertex_id_map.insert(String::from("A9"), vertex_ids[9]);
    vertex_id_map.insert(String::from("B0"), vertex_ids[10]);
    vertex_id_map.insert(String::from("C0"), vertex_ids[11]);
    vertex_id_map.insert(String::from("D0"), vertex_ids[12]);
    vertex_id_map.insert(String::from("E0"), vertex_ids[13]);
    let edge_ids = gen_group_ids(13);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A5").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[10],
            *edge_label_map.get("A->B").unwrap(),
            *vertex_id_map.get("A6").unwrap(),
            *vertex_id_map.get("B0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("B").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[11],
            *edge_label_map.get("A->D").unwrap(),
            *vertex_id_map.get("A7").unwrap(),
            *vertex_id_map.get("D0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("D").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[12],
            *edge_label_map.get("A->C").unwrap(),
            *vertex_id_map.get("A8").unwrap(),
            *vertex_id_map.get("C0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("C").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[13],
            *edge_label_map.get("A->E").unwrap(),
            *vertex_id_map.get("A9").unwrap(),
            *vertex_id_map.get("E0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("E").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}

pub fn build_pattern_rank_ranking_case20() -> (Pattern, HashMap<String, PatternId>) {
    let mut vertex_label_map: HashMap<String, PatternLabelId> = HashMap::new();
    let mut vertex_id_map: HashMap<String, PatternId> = HashMap::new();
    let edge_label_map: HashMap<String, PatternLabelId> =
        gen_edge_label_map(vec![String::from("A->A"), String::from("A->B"), String::from("B->C")]);
    vertex_label_map.insert(String::from("A"), 1);
    let vertex_ids = gen_group_ids(4);
    vertex_id_map.insert(String::from("A0"), vertex_ids[0]);
    vertex_id_map.insert(String::from("A1"), vertex_ids[1]);
    vertex_id_map.insert(String::from("A2"), vertex_ids[2]);
    vertex_id_map.insert(String::from("A3"), vertex_ids[3]);
    vertex_id_map.insert(String::from("A4"), vertex_ids[4]);
    let edge_ids = gen_group_ids(11);
    let pattern_vec = vec![
        new_pattern_edge(
            edge_ids[0],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[1],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[2],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[3],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[4],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[5],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[6],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[7],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A2").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[8],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[9],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A3").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[10],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A0").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
        new_pattern_edge(
            edge_ids[11],
            *edge_label_map.get("A->A").unwrap(),
            *vertex_id_map.get("A4").unwrap(),
            *vertex_id_map.get("A1").unwrap(),
            *vertex_label_map.get("A").unwrap(),
            *vertex_label_map.get("A").unwrap(),
        ),
    ];
    (Pattern::from(pattern_vec), vertex_id_map)
}
