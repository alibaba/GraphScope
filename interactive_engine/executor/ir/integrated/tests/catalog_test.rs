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
    use std::fs::File;

    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_core::catalogue::error::IrPatternResult;
    use ir_core::catalogue::pattern::Pattern;
    use ir_core::catalogue::pattern_meta::PatternMeta;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::meta::PlanMeta;
    use ir_core::plan::physical::AsPhysical;
    use ir_core::{plan::meta::Schema, JsonIO};
    use pegasus_client::builder::JobBuilder;

    use crate::common::test::*;

    pub fn get_ldbc_pattern_meta() -> PatternMeta {
        let ldbc_schema_file = File::open("../core/resource/ldbc_schema.json").unwrap();
        let ldbc_schema = Schema::from_json(ldbc_schema_file).unwrap();
        PatternMeta::from(&ldbc_schema)
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           Person
    //     knows/      \knows
    //      Person -> Person
    pub fn build_ldbc_pattern_from_pb_case1() -> IrPatternResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), // KNOWS
            expand_opt: 0,
            alias: None,
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
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //           University
    //     study at/      \study at
    //      Person   ->    Person
    pub fn build_ldbc_pattern_from_pb_case2() -> IrPatternResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 1,                                              // in
            params: Some(query_params(vec![15.into()], vec![], None)), //STUDYAT
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 1,                                              // in
            params: Some(query_params(vec![15.into()], vec![], None)), //STUDYAT
            expand_opt: 0,
            alias: None,
        };
        let expand_opr3 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
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
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    // 4 Persons know each other
    pub fn build_ldbc_pattern_from_pb_case3() -> IrPatternResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
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
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    // Pattern from ldbc schema file and build from pb::Pattern message
    //             City
    //      lives/     \lives
    //     Person      Person
    //     likes \      / has creator
    //           Comment
    pub fn build_ldbc_pattern_from_pb_case4() -> IrPatternResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
            expand_opt: 0,
            alias: None,
        };
        let expand_opr3 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![13.into()], vec![], None)), //LIKES
            expand_opt: 0,
            alias: None,
        };
        let expand_opr4 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                             // out
            params: Some(query_params(vec![0.into()], vec![], None)), //HASCREATOR
            expand_opt: 0,
            alias: None,
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
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    fn build_ldbc_bi11() -> IrPatternResult<Pattern> {
        let ldbc_pattern_mata = get_ldbc_pattern_meta();
        // define pb pattern message
        let expand_opr0 = pb::EdgeExpand {
            v_tag: None,
            direction: 2,                                              // both
            params: Some(query_params(vec![12.into()], vec![], None)), //KNOWS
            expand_opt: 0,
            alias: None,
        };
        let expand_opr1 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![11.into()], vec![], None)), //ISLOCATEDIN
            expand_opt: 0,
            alias: None,
        };
        let expand_opr2 = pb::EdgeExpand {
            v_tag: None,
            direction: 0,                                              // out
            params: Some(query_params(vec![17.into()], vec![], None)), //ISPARTOF
            expand_opt: 0,
            alias: None,
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
        };
        Pattern::from_pb_pattern(&pattern, &ldbc_pattern_mata, &mut PlanMeta::default())
    }

    fn get_source_of_pattern() -> pb::Scan {
        pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![], None)),
            idx_predicate: None,
        }
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
        plan.append_operator_as_node(get_source_of_pattern().into(), vec![])
            .unwrap();
        plan.append_plan(pb_plan.into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(get_sink_of_pattern(pattern).into(), vec![plan.max_node_id - 1])
            .unwrap();
        plan
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
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case3() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case3().unwrap();
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

    // fuzzy pattern test
    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_pattern_from_pb_case4() {
        let ldbc_pattern = build_ldbc_pattern_from_pb_case4().unwrap();
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

    // fuzzy pattern test
    #[test]
    fn test_generate_simple_matching_plan_for_ldbc_bi11() {
        let ldbc_pattern = build_ldbc_bi11().unwrap();
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
}
