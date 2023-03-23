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
//!
//!

mod common;

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;
    use std::fs::File;
    use std::iter::FromIterator;

    use graph_proxy::apis::GraphElement;
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::DefaultId;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::KeyId;
    use ir_core::glogue::error::IrPatternResult;
    use ir_core::glogue::pattern::Pattern;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::meta::{PlanMeta, STORE_META};
    use ir_core::plan::physical::AsPhysical;
    use ir_core::{plan::meta::Schema, JsonIO};
    use ir_physical_client::physical_builder::JobBuilder;
    use pegasus::result::ResultStream;

    use crate::common::test::*;

    pub fn _set_ldbc_graph_schema() {
        let ldbc_schema_file = File::open("../core/resource/ldbc_schema.json").unwrap();
        let ldbc_schema = Schema::from_json(ldbc_schema_file).unwrap();
        while let Ok(mut store_meta) = STORE_META.write() {
            store_meta.schema = Some(ldbc_schema);
            break;
        }
    }

    pub fn set_modern_graph_schema() {
        let modern_schema_file = File::open("../core/resource/modern_schema.json").unwrap();
        let modern_schema = Schema::from_json(modern_schema_file).unwrap();
        while let Ok(mut store_meta) = STORE_META.write() {
            store_meta.schema = Some(modern_schema);
            break;
        }
    }

    // match(__.as(A).outE().inV().as(B).outE().inV().as(C))
    pub fn build_modern_pattern_case1() -> IrPatternResult<Pattern> {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: Some(TAG_B.into()),
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())) },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())) },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())) },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())) },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).outE().inV().as(B).outE().inV().has("name", "lop").as(C))
    pub fn build_modern_pattern_case1_predicate() -> IrPatternResult<Pattern> {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_b = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: Some(TAG_B.into()),
            meta_data: None,
        };
        let get_v_c = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"lop\"".to_string()).unwrap()),
            )),
            alias: Some(TAG_C.into()),
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())) },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v_b.clone())) },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())) },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v_c.clone())) },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).outE().inV().as(B), __.as(B).outE().inV().as(C))
    pub fn build_modern_pattern_case2() -> IrPatternResult<Pattern> {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).outE().inV().has("age", 32).as(B), __.as(B).outE().inV().as(C))
    pub fn build_modern_pattern_case2_predicate() -> IrPatternResult<Pattern> {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_b = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.age==32".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let get_v_c = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_b.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_c.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(B).inE().outV().as(A).outE().inV().as(C))
    pub fn build_modern_pattern_case3() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_in = pb::EdgeExpand {
            v_tag: None,
            direction: 1, // in
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_start = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::Start as i32,
            params: None,
            alias: Some(TAG_A.into()),
            meta_data: None,
        };
        let get_v_end = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: Some(TAG_A.into()),
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_end.clone())),
                    },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(B).inE().inV().has("name", "marko").as(A).outE().inV().as(C))
    pub fn build_modern_pattern_case3_predicate() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_in = pb::EdgeExpand {
            v_tag: None,
            direction: 1, // in
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_a = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::Start as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"marko\"".to_string()).unwrap()),
            )),
            alias: Some(TAG_A.into()),
            meta_data: None,
        };
        let get_v_c = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: Some(TAG_C.into()),
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_B.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                    },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v_a.clone())) },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                    },
                    pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v_c.clone())) },
                ],
                end: Some(TAG_C.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).outE().inV().as(B), __.as(A).outE().inV().as(C))
    pub fn build_modern_pattern_case4() -> IrPatternResult<Pattern> {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).outE().inV().as(B), __.as(A).outE().inV().has("name", "ripple").as(C))
    pub fn build_modern_pattern_case4_predicate() -> IrPatternResult<Pattern> {
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_b = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let get_v_c = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"ripple\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_b.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_c.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).outE().inV().as(B), __.as(A).outE().inV().as(C), __.as(B).outE().inV().as(C))
    pub fn build_modern_pattern_case5() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).has("age", 29).outE("knows").inV().has("name", "josh").as(B),
    //       __.as(A).has("name", "marko").outE("created").inV().has("name", "lop").as(C),
    //       __.as(B).has("age", 32).outE("created").inV().has("lang", "java").as(C))
    pub fn build_modern_pattern_case5_predicate() -> IrPatternResult<Pattern> {
        let expand_opr_a_b = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: Some(query_params(vec![0.into()], vec![], None)),
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_a_c = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: Some(query_params(vec![1.into()], vec![], None)),
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_b_c = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // outhaod
            params: Some(query_params(vec![1.into()], vec![], None)),
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_b = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"josh\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let get_v_c_1 = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"lop\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let get_v_c_2 = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.lang==\"java\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let select_marko =
            pb::Select { predicate: Some(str_to_expr_pb("@.name == \"marko\"".to_string()).unwrap()) };
        let select_person_age_29 =
            pb::Select { predicate: Some(str_to_expr_pb("@.age == 29".to_string()).unwrap()) };
        let select_person_age_32 =
            pb::Select { predicate: Some(str_to_expr_pb("@.age == 32".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person_age_29.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_a_b.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_b.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_marko.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_a_c.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_c_1.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person_age_32.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_b_c.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_c_2.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).repeat(outE().inV()).times(2).as(B))
    pub fn build_modern_pattern_case6() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let path_expand = pb::PathExpand {
            base: Some(pb::path_expand::ExpandBase {
                edge_expand: Some(expand_opr_out),
                get_v: Some(get_v),
            }),
            start_tag: None,
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            alias: None,
            path_opt: pb::path_expand::PathOpt::Simple as i32,
            result_opt: pb::path_expand::ResultOpt::EndV as i32,
        };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Path(path_expand.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_B as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).repeat(outE().inV()).times(2).as(B), __.as(A).outE().inV().as(B))
    pub fn build_modern_pattern_case7() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let path_expand = pb::PathExpand {
            base: Some(pb::path_expand::ExpandBase {
                edge_expand: Some(expand_opr_out.clone()),
                get_v: Some(get_v.clone()),
            }),
            start_tag: None,
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            alias: None,
            path_opt: pb::path_expand::PathOpt::Simple as i32,
            result_opt: pb::path_expand::ResultOpt::EndV as i32,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Path(path_expand.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr_out)) },
                        pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v)) },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_B as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).repeat(outE().inV().has("name")).times(2).as(B), __.as(A).outE().inV().has("lang", "java").as(B))
    pub fn build_modern_pattern_case7_predicate() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(vec![], vec!["name".into()], None)),
            alias: None,
            meta_data: None,
        };
        let get_v_b = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.lang==\"java\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let path_expand = pb::PathExpand {
            base: Some(pb::path_expand::ExpandBase {
                edge_expand: Some(expand_opr_out.clone()),
                get_v: Some(get_v),
            }),
            start_tag: None,
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            alias: None,
            path_opt: pb::path_expand::PathOpt::Simple as i32,
            result_opt: pb::path_expand::ResultOpt::EndV as i32,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Path(path_expand.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr_out)) },
                        pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Vertex(get_v_b)) },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_B as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).inE().outV().outE().inV().inE().outV().outE().inV().as(B))
    pub fn build_modern_pattern_case8() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_in = pb::EdgeExpand {
            v_tag: None,
            direction: 1, // in
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_end = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let get_v_start = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::Start as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_end.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_end.clone())),
                    },
                ],
                end: Some(TAG_B.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_B as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).has("name", "vadas").inE().outV().outE().inV().inE().outV().outE().inV().has("name", "ripple").as(B))
    pub fn build_modern_pattern_case8_predicate() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_in = pb::EdgeExpand {
            v_tag: None,
            direction: 1, // in
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_end = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let get_v_start = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::Start as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let get_v_ripple = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"ripple\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let select_vadas =
            pb::Select { predicate: Some(str_to_expr_pb("@.name == \"vadas\"".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Select(select_vadas.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_end.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                    },
                    pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Vertex(get_v_ripple.clone())),
                    },
                ],
                end: Some(TAG_B.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_B as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).inE().outV().outE().inV().as(C),
    //       __.as(C).inE().outV().outE().inV().as(B))
    pub fn build_modern_pattern_case9() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_in = pb::EdgeExpand {
            v_tag: None,
            direction: 1, // in
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_end = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let get_v_start = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::Start as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_end.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_end.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // match(__.as(A).has("name", "vadas").inE().outV().outE().inV().has("name", "lop").as(C),
    //       __.as(C).has("lang", "java").inE().outV().outE().inV().has("name", "ripple").as(B))
    pub fn build_modern_pattern_case9_predicate() -> IrPatternResult<Pattern> {
        let expand_opr_out = pb::EdgeExpand {
            v_tag: None,
            direction: 0, // out
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let expand_opr_in = pb::EdgeExpand {
            v_tag: None,
            direction: 1, // in
            params: None,
            expand_opt: pb::edge_expand::ExpandOpt::Edge as i32,
            alias: None,
            meta_data: None,
        };
        let get_v_lop = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"lop\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let get_v_ripple = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::End as i32,
            params: Some(query_params(
                vec![],
                vec![],
                Some(str_to_expr_pb("@.name==\"ripple\"".to_string()).unwrap()),
            )),
            alias: None,
            meta_data: None,
        };
        let get_v_start = pb::GetV {
            tag: None,
            opt: pb::get_v::VOpt::Start as i32,
            params: None,
            alias: None,
            meta_data: None,
        };
        let select_vadas =
            pb::Select { predicate: Some(str_to_expr_pb("@.name == \"vadas\"".to_string()).unwrap()) };
        let select_java =
            pb::Select { predicate: Some(str_to_expr_pb("@.lang == \"java\"".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_vadas.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_lop.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_java.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_in.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_start.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr_out.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Vertex(get_v_ripple.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        let mut plan_meta = PlanMeta::default();
        plan_meta.set_max_tag_id(TAG_C as u32 + 1);
        Pattern::from_pb_pattern(&pattern, &mut plan_meta)
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           Person
    //     knows/      \knows
    //      Person -> Person
    pub fn build_ldbc_pattern_from_pb_case1() -> IrPatternResult<Pattern> {
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };
        let select_person =
            pb::Select { predicate: Some(str_to_expr_pb("@.~label == 1".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Edge(expand_opr.clone())),
                        },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
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
        Pattern::from_pb_pattern(&pattern, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           University
    //     study at/      \study at
    //      Person   ->    Person
    pub fn build_ldbc_pattern_from_pb_case2() -> IrPatternResult<Pattern> {
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
        let select_person =
            pb::Select { predicate: Some(str_to_expr_pb("@.~label == 1".to_string()).unwrap()) };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![
                        pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr1)) },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
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
                    binders: vec![
                        pb::pattern::Binder { item: Some(pb::pattern::binder::Item::Edge(expand_opr3)) },
                        pb::pattern::Binder {
                            item: Some(pb::pattern::binder::Item::Select(select_person.clone())),
                        },
                    ],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        Pattern::from_pb_pattern(&pattern, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    // 4 Persons know each other
    pub fn build_ldbc_pattern_from_pb_case3() -> IrPatternResult<Pattern> {
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
        Pattern::from_pb_pattern(&pattern, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //             City
    //      lives/     \lives
    //     Person      Person
    //     likes \      / has creator
    //           Comment
    pub fn build_ldbc_pattern_from_pb_case4() -> IrPatternResult<Pattern> {
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
        Pattern::from_pb_pattern(&pattern, &mut PlanMeta::default())
    }

    fn build_ldbc_bi11() -> IrPatternResult<Pattern> {
        // define pb pattern message
        let expand_opr0 = pb::EdgeExpand {
            v_tag: None,
            direction: 2,                                              // both
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };
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
            params: Some(query_params(vec![17.into()], vec![], None)), //ISPARTOF
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };
        let pattern = pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr0.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    }],
                    end: Some(TAG_E.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_C.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr1.clone())),
                    }],
                    end: Some(TAG_F.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_D.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_E.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_F.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(expand_opr2.clone())),
                    }],
                    end: Some(TAG_H.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        };
        Pattern::from_pb_pattern(&pattern, &mut PlanMeta::default())
    }

    fn get_sink_of_pattern(pattern: &Pattern) -> pb::Sink {
        let max_tag_id = pattern.get_max_tag_id() as i32;
        let tags = (0..max_tag_id)
            .map(|tag_id| common_pb::NameOrIdKey { key: Some((tag_id as i32).into()) })
            .collect();
        pb::Sink {
            tags,
            sink_target: Some(pb::sink::SinkTarget {
                inner: Some(pb::sink::sink_target::Inner::SinkDefault(pb::SinkDefault {
                    id_name_mappings: vec![],
                })),
            }),
        }
    }

    fn get_patmat_logical_plan(pattern: &Pattern, pb_plan: pb::LogicalPlan) -> LogicalPlan {
        let mut plan = LogicalPlan::default();
        plan.append_plan(pb_plan.into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(get_sink_of_pattern(pattern).into(), vec![plan.get_max_node_id() - 1])
            .unwrap();
        plan
    }

    fn simple_pattern_match(pattern: &Pattern) -> ResultStream<Vec<u8>> {
        initialize();
        let plan = get_patmat_logical_plan(
            &pattern,
            pattern
                .generate_simple_extend_match_plan()
                .unwrap(),
        );
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();
        let request = job_builder.build().unwrap();
        submit_query(request, 2)
    }

    fn get_simple_match_result_ids_set(pattern: &Pattern, tags: Vec<KeyId>) -> BTreeSet<Vec<DefaultId>> {
        let mut results = simple_pattern_match(pattern);
        let mut result_ids_set = BTreeSet::new();
        while let Some(result) = results.next() {
            if let Ok(record_code) = result {
                let mut entry = parse_result(record_code).unwrap();
                assert_eq!(entry.get_columns_mut().len(), tags.len());
                let ids: Vec<DefaultId> = tags
                    .iter()
                    .map(|tag| entry.get(Some(*tag)).unwrap().id() as DefaultId)
                    .collect();
                // avoid repeated vertices
                let ids_set: BTreeSet<DefaultId> = ids.iter().cloned().collect();
                if ids_set.len() == ids.len() {
                    result_ids_set.insert(ids);
                }
            }
        }
        result_ids_set
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case1() {
        let modern_pattern = build_modern_pattern_case1().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v4, v3], vec![v1, v4, v5]]);
        assert_eq!(result_ids_set, expected_result_ids_set);
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case1_predicate() {
        let modern_pattern = build_modern_pattern_case1_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v4, v3]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case2() {
        let modern_pattern = build_modern_pattern_case2().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v4, v3], vec![v1, v4, v5]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case2_predicate() {
        let modern_pattern = build_modern_pattern_case2_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v4, v3], vec![v1, v4, v5]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case3() {
        let modern_pattern = build_modern_pattern_case3().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([
            vec![v1, v2, v3],
            vec![v1, v3, v2],
            vec![v1, v2, v4],
            vec![v1, v4, v2],
            vec![v1, v3, v4],
            vec![v1, v4, v3],
            vec![v4, v3, v5],
            vec![v4, v5, v3],
        ]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case3_predicate() {
        let modern_pattern = build_modern_pattern_case3_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_result_ids_set = BTreeSet::from_iter([
            vec![v1, v2, v3],
            vec![v1, v3, v2],
            vec![v1, v2, v4],
            vec![v1, v4, v2],
            vec![v1, v3, v4],
            vec![v1, v4, v3],
        ]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case4() {
        let modern_pattern = build_modern_pattern_case4().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([
            vec![v1, v2, v3],
            vec![v1, v3, v2],
            vec![v1, v2, v4],
            vec![v1, v4, v2],
            vec![v1, v3, v4],
            vec![v1, v4, v3],
            vec![v4, v3, v5],
            vec![v4, v5, v3],
        ]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case4_predicate() {
        let modern_pattern = build_modern_pattern_case4_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v4, v3, v5]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case5() {
        let modern_pattern = build_modern_pattern_case5().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v4, v3]]);
        assert_eq!(result_ids_set, expected_result_ids_set);
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case5_predicate() {
        let modern_pattern = build_modern_pattern_case5_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v4, v3]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case6() {
        let modern_pattern = build_modern_pattern_case6().unwrap();
        set_modern_graph_schema();
        initialize();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v3], vec![v1, v5]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case7() {
        let modern_pattern = build_modern_pattern_case7().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v3]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case7_predicate() {
        let modern_pattern = build_modern_pattern_case7_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B]);
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v1, v3]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case8() {
        let modern_pattern = build_modern_pattern_case8().unwrap();
        set_modern_graph_schema();
        get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B]);
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case8_predicate() {
        let modern_pattern = build_modern_pattern_case8_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B]);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v2, v5]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case9() {
        let modern_pattern = build_modern_pattern_case9().unwrap();
        set_modern_graph_schema();
        get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
    }

    #[test]
    fn test_generate_simple_matching_plan_for_modern_case9_predicate() {
        let modern_pattern = build_modern_pattern_case9_predicate().unwrap();
        set_modern_graph_schema();
        let result_ids_set = get_simple_match_result_ids_set(&modern_pattern, vec![TAG_A, TAG_B, TAG_C]);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let expected_result_ids_set = BTreeSet::from_iter([vec![v2, v5, v3]]);
        assert_eq!(result_ids_set, expected_result_ids_set)
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case1() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case1().unwrap();
        initialize();
        let plan = get_patmat_logical_plan(
            &ldbc_pattern,
            ldbc_pattern
                .generate_simple_extend_match_plan()
                .unwrap(),
        );
        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();
        let request = job_builder.build().unwrap();
        let mut results = submit_query(request, 2);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case2() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case2().unwrap();
        initialize();
        let mut results = simple_pattern_match(&ldbc_pattern);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
    }

    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case3() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
        initialize();
        let mut results = simple_pattern_match(&ldbc_pattern);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
    }

    // fuzzy pattern test
    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case4() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
        initialize();
        let mut results = simple_pattern_match(&ldbc_pattern);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
    }

    // fuzzy pattern test
    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_bi11() {
        let ldbc_pattern = build_ldbc_bi11().unwrap();
        initialize();
        let mut results = simple_pattern_match(&ldbc_pattern);
        let mut count = 0;
        while let Some(result) = results.next() {
            if let Ok(_) = result {
                count += 1;
            }
        }
        println!("{}", count);
    }
}
