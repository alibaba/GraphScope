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
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::physical::AsPhysical;
    use pegasus_server::JobRequest;
    use runtime::builder::JobBuilder;
    use runtime::graph::element::GraphElement;

    use crate::common::test::{initialize, parse_result, query_params, submit_query, TAG_A, TAG_B, TAG_C};

    // g.V().hasLabel("person").match(
    //    __.as('a').has(age, gt(25)).out("created").as('b'),
    //    __.as('a').out('knows').as('c'),
    // )
    fn init_match_case1_request() -> JobRequest {
        let source = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![], None)),
            idx_predicate: None,
        };

        let out_knows = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec!["knows".into()], vec![], None)),
            is_edge: false,
            alias: None,
        };

        let out_created = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(
                vec!["created".into()],
                vec![],
                str_to_expr_pb("@.lange=\"Java\"".to_string()).ok(),
            )),
            is_edge: false,
            alias: None,
        };

        let pattern = pb::Pattern {
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
        };

        let sink = pb::Sink {
            tags: vec![
                common_pb::NameOrIdKey { key: Some(TAG_A.into()) },
                common_pb::NameOrIdKey { key: Some(TAG_B.into()) },
                common_pb::NameOrIdKey { key: Some(TAG_C.into()) },
            ],
            id_name_mappings: vec![],
        };

        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(source.into(), vec![])
            .unwrap();
        let id = plan
            .append_operator_as_node(pattern.into(), vec![0])
            .unwrap();
        plan.append_operator_as_node(sink.into(), vec![id])
            .unwrap();

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta)
            .unwrap();

        job_builder.build().unwrap()
    }

    #[test]
    fn match_case1() {
        initialize();
        let request = init_match_case1_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_ids = vec![(1, 4, 1 << 56 | 3)];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    let a = entry
                        .get(Some(&TAG_A.into()))
                        .unwrap()
                        .as_graph_vertex();
                    let b = entry
                        .get(Some(&TAG_B.into()))
                        .unwrap()
                        .as_graph_vertex();
                    let c = entry
                        .get(Some(&TAG_C.into()))
                        .unwrap()
                        .as_graph_vertex();
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
