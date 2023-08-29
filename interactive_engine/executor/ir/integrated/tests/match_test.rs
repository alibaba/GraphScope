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

    use graph_proxy::apis::GraphElement;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::KeyId;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::meta::set_schema_from_json;
    use ir_core::plan::physical::AsPhysical;
    use ir_physical_client::physical_builder::{JobBuilder, PlanBuilder};
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::{
        default_sink_target, initialize, parse_result, query_params, submit_query, CREATED_LABEL,
        KNOWS_LABEL, PERSON_LABEL, TAG_A, TAG_B, TAG_C, TAG_D,
    };

    pub fn init_schema() {
        let ldbc_schema_file = File::open("../core/resource/modern_schema.json").unwrap();
        set_schema_from_json(ldbc_schema_file);
    }

    fn build_job_request(plan: LogicalPlan) -> JobRequest {
        let mut plan_builder = PlanBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut plan_builder, &mut plan_meta)
            .unwrap();
        let job_builder = JobBuilder::with_plan(plan_builder);
        job_builder.build().unwrap()
    }

    fn get_person_scan() -> pb::Scan {
        pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            meta_data: None,
        }
    }

    fn get_out_edge() -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: None,
            expand_opt: 0,
            alias: None,
            meta_data: None,
        }
    }

    fn get_in_edge() -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 1,
            params: None,
            expand_opt: 0,
            alias: None,
            meta_data: None,
        }
    }

    fn get_out_knows() -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
        }
    }

    fn get_out_created() -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(
                vec![CREATED_LABEL.into()],
                vec![],
                str_to_expr_pb("@.lange=\"Java\"".to_string()).ok(),
            )),
            expand_opt: 0,
            alias: None,
            meta_data: None,
        }
    }

    fn get_inner_join(tags: Vec<KeyId>) -> pb::Join {
        let keys: Vec<common_pb::Variable> = tags
            .into_iter()
            .map(|tag| common_pb::Variable { tag: Some(tag.into()), property: None, node_type: None })
            .collect();
        pb::Join { left_keys: keys.clone(), right_keys: keys.clone(), kind: 0 }
    }

    fn get_sink(tags: Vec<KeyId>) -> pb::Sink {
        pb::Sink {
            tags: tags
                .into_iter()
                .map(|tag| common_pb::NameOrIdKey { key: Some(tag.into()) })
                .collect(),
            sink_target: default_sink_target(),
        }
    }

    //    __.as('a').has(age, gt(25)).out("created").as('b'),
    //    __.as('a').out('knows').as('c')
    fn get_pattern_case1() -> pb::Pattern {
        let out_knows = get_out_knows();

        let out_created = get_out_created();

        pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_knows)),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_created.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_created.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        }
    }

    //    __.as('a').out("knows").as('b'),
    //    __.as('a').out('knows').as('c'),
    //    __.as('a').out('created').as('d')
    fn get_pattern_case2() -> pb::Pattern {
        let out_knows = get_out_knows();

        let out_created = get_out_created();

        pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_knows.clone())),
                    }],
                    end: Some(TAG_B.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_knows)),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_created.clone())),
                    }],
                    end: Some(TAG_D.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        }
    }

    //    __.as('a').out("created").as('c'),
    //    __.as('b').out('created').as('c'),
    fn get_pattern_case3() -> pb::Pattern {
        let out_created = get_out_created();

        pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_created.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_created.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        }
    }

    //    __.as('a').out("knows").as('b'),
    fn get_pattern_case4() -> pb::Pattern {
        let out_knows = get_out_knows();

        pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(out_knows.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        }
    }

    //    __.as('a').out().as('c'),
    //    __.as('b').out().as('c'),
    fn get_pattern_case5() -> pb::Pattern {
        let out_edge = get_out_edge();

        pb::Pattern {
            sentences: vec![
                pb::pattern::Sentence {
                    start: Some(TAG_A.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_edge.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
                pb::pattern::Sentence {
                    start: Some(TAG_B.into()),
                    binders: vec![pb::pattern::Binder {
                        item: Some(pb::pattern::binder::Item::Edge(out_edge.clone())),
                    }],
                    end: Some(TAG_C.into()),
                    join_kind: 0,
                },
            ],
            meta_data: vec![],
        }
    }

    //    __.as('a').out().as('b'),
    fn get_pattern_case6() -> pb::Pattern {
        let out_edge = get_out_edge();

        pb::Pattern {
            sentences: vec![pb::pattern::Sentence {
                start: Some(TAG_A.into()),
                binders: vec![pb::pattern::Binder {
                    item: Some(pb::pattern::binder::Item::Edge(out_edge.clone())),
                }],
                end: Some(TAG_B.into()),
                join_kind: 0,
            }],
            meta_data: vec![],
        }
    }

    // g.V().match(
    //    __.as('a').has(age, gt(25)).out("created").as('b'),
    //    __.as('a').out('knows').as('c'),
    // )
    // if define source, g.V().hasLabel('person').match(...)
    fn init_match_case1_request(define_source: bool) -> JobRequest {
        init_schema();
        let source = if define_source { get_person_scan() } else { pb::Scan::default() };

        let pattern = get_pattern_case1();

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let srouce_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();
        let id = plan
            .append_operator_as_node(pattern.into(), vec![srouce_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().match(
    //    __.as('a').out("knows").as('b'),
    //    __.as('a').out('knows').as('c'),
    //    __.as('a').out('created').as('d')
    // )
    // if define source, g.V().hasLabel('person').match(...)
    fn init_match_case2_request(define_source: bool) -> JobRequest {
        init_schema();
        let source = if define_source { get_person_scan() } else { pb::Scan::default() };

        let pattern = get_pattern_case2();

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C, TAG_D]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();
        let id = plan
            .append_operator_as_node(pattern.into(), vec![source_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().match(
    //    __.as('a').out("created").as('c'),
    //    __.as('b').out('created').as('c'),
    // ).join(
    //    match(__.as('a').out("knows").as('b'))
    // )
    fn init_match_case3_request() -> JobRequest {
        init_schema();

        let source = pb::Scan::default();

        let pattern_0 = get_pattern_case3();

        let pattern_1 = get_pattern_case4();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![source_id])
            .unwrap();
        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![source_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().in().match(
    //    __.as('a').out("created").as('c'),
    //    __.as('b').out('created').as('c'),
    // ).join(
    //    match(__.as('a').out("knows").as('b'))
    // )
    fn init_match_case4_request() -> JobRequest {
        init_schema();

        let source = pb::Scan::default();

        let in_edge = get_in_edge();

        let pattern_0 = get_pattern_case3();

        let pattern_1 = get_pattern_case4();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let in_edge_id = plan
            .append_operator_as_node(in_edge.into(), vec![source_id])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![in_edge_id])
            .unwrap();

        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![in_edge_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().in().match(
    //    __.as('a').out("created").as('c'),
    //    __.as('b').out('created').as('c'),
    // ).join(
    //    match(__.as('a').out("knows").as('b'))
    // )
    // first match follows the "in()" step
    // second match follows the "V()"(source operator) step
    fn init_match_case5_request() -> JobRequest {
        init_schema();

        let source = pb::Scan::default();

        let in_edge = get_in_edge();

        let pattern_0 = get_pattern_case3();

        let pattern_1 = get_pattern_case4();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let in_edge_id = plan
            .append_operator_as_node(in_edge.into(), vec![source_id])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![in_edge_id])
            .unwrap();

        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![source_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().match(
    //    __.as('a').out().as('c'),
    //    __.as('b').out().as('c'),
    // ).join(
    //    match(__.as('a').out().as('b'))
    // )
    fn init_match_case6_request() -> JobRequest {
        init_schema();

        let source = pb::Scan::default();

        let pattern_0 = get_pattern_case5();

        let pattern_1 = get_pattern_case6();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![source_id])
            .unwrap();
        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![source_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().hasLabel('person').match(
    //    __.as('a').out().as('c'),
    //    __.as('b').out().as('c'),
    // ).join(
    //    match(__.as('a').out().as('b'))
    // )
    fn init_match_case7_request() -> JobRequest {
        init_schema();

        let source = get_person_scan();

        let pattern_0 = get_pattern_case5();

        let pattern_1 = get_pattern_case6();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![source_id])
            .unwrap();
        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![source_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().hasLabel('person').match(
    //    __.as('a').out("created").as('c'),
    //    __.as('b').out("created").as('c'),
    // ).join(
    //    match(__.as('a').out().as('b'))
    // )
    fn init_match_case8_request() -> JobRequest {
        init_schema();

        let source = get_person_scan();

        let pattern_0 = get_pattern_case3();

        let pattern_1 = get_pattern_case6();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![source_id])
            .unwrap();
        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![source_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // g.V().hasLabel('person').match(
    //    __.as('a').out().as('c'),
    //    __.as('b').out().as('c'),
    // ).join(
    //    match(__.as('a').out("knows").as('b'))
    // )
    fn init_match_case9_request() -> JobRequest {
        init_schema();

        let source = get_person_scan();

        let pattern_0 = get_pattern_case5();

        let pattern_1 = get_pattern_case4();

        let join = get_inner_join(vec![TAG_A, TAG_B]);

        let sink = get_sink(vec![TAG_A, TAG_B, TAG_C]);

        let mut plan = LogicalPlan::with_root();
        let source_id = plan
            .append_operator_as_node(source.into(), vec![0])
            .unwrap();

        let pattern_0_id = plan
            .append_operator_as_node(pattern_0.into(), vec![source_id])
            .unwrap();
        let pattern_1_id = plan
            .append_operator_as_node(pattern_1.into(), vec![source_id])
            .unwrap();

        let join_id = plan
            .append_operator_as_node(join.into(), vec![pattern_0_id, pattern_1_id])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    fn match_case1(define_source: bool) {
        initialize();
        let request = init_match_case1_request(define_source);
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    #[test]
    fn naive_match_case1() {
        match_case1(true)
    }

    #[test]
    fn extend_match_case1() {
        match_case1(false)
    }

    fn match_case2(define_source: bool) {
        initialize();
        let request = init_match_case2_request(define_source);
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![
            (1, 2, 2, 1 << 56 | 3),
            (1, 2, 4, 1 << 56 | 3),
            (1, 4, 2, 1 << 56 | 3),
            (1, 4, 4, 1 << 56 | 3),
        ];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    let d = entry.get(Some(TAG_D)).unwrap().as_vertex();
                    result_collection.push((
                        a.unwrap().id(),
                        b.unwrap().id(),
                        c.unwrap().id(),
                        d.unwrap().id(),
                    ));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    #[test]
    fn naive_match_case2() {
        match_case2(true)
    }

    #[test]
    fn extend_match_case2() {
        match_case2(false)
    }

    #[test]
    fn match_case3() {
        initialize();
        let request = init_match_case3_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    #[test]
    fn match_case4() {
        initialize();
        let request = init_match_case4_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3); 9];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(expected_result_ids, result_collection);
    }

    #[test]
    fn match_case5() {
        initialize();
        let request = init_match_case5_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3); 3];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(expected_result_ids, result_collection);
    }

    #[test]
    fn match_case6() {
        initialize();
        let request = init_match_case6_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    #[test]
    fn match_case7() {
        initialize();
        let request = init_match_case7_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    #[test]
    fn match_case8() {
        initialize();
        let request = init_match_case8_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    #[test]
    fn match_case9() {
        initialize();
        let request = init_match_case9_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry.get(Some(TAG_A)).unwrap().as_vertex();
                    let b = entry.get(Some(TAG_B)).unwrap().as_vertex();
                    let c = entry.get(Some(TAG_C)).unwrap().as_vertex();
                    result_collection.push((a.unwrap().id(), b.unwrap().id(), c.unwrap().id()));
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }
}
