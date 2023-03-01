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
    use dyn_type::Object;
    use graph_proxy::apis::GraphElement;
    use graph_store::common::DefaultId;
    use graph_store::ldbc::LDBCVertexParser;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::KeyId;
    use ir_physical_client::physical_builder::*;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;

    fn init_apply_request(join_kind: i32) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            op_meta: None,
        };

        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], vec![], None)),
            expand_opt: 0,
            alias: None,
            op_meta: None,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.apply_func(
            unsafe { ::std::mem::transmute(join_kind) },
            move |plan| {
                plan.shuffle(None);
                plan.edge_expand(expand_opr.clone().into());
            },
            None,
        );
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    fn apply_semi_join(worker_num: u32) {
        initialize();
        // join_kind: SemiJoin
        let request = init_apply_request(4);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let expected_result_ids = vec![v1, v4, v6];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    // g.V().where(out())
    #[test]
    fn apply_semi_join_test() {
        apply_semi_join(1)
    }

    // g.V().where(out())
    #[test]
    fn apply_semi_join_w2_test() {
        apply_semi_join(2)
    }

    fn apply_anti_join(worker_num: u32) {
        initialize();
        // join_kind: AntiJoin
        let request = init_apply_request(5);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let expected_result_ids = vec![v2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    // g.V().where(not(out()))
    #[test]
    fn apply_anti_join_test() {
        apply_anti_join(1)
    }

    // g.V().where(not(out()))
    #[test]
    fn apply_anti_join_w2_test() {
        apply_anti_join(2)
    }

    // apply with the result of subtask is a single value (count())
    fn init_apply_count_request(join_kind: i32, alias: KeyId) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            op_meta: None,
        };

        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], vec![], None)),
            expand_opt: 0,
            alias: None,
            op_meta: None,
        };

        let fold_opr = pb::GroupBy {
            mappings: vec![],
            functions: vec![pb::group_by::AggFunc {
                vars: vec![common_pb::Variable::from("@".to_string())],
                aggregate: 3, // count
                alias: None,
            }],
            op_meta: vec![],
        };

        let sink_opr = pb::Sink {
            tags: vec![
                common_pb::NameOrIdKey { key: None },
                common_pb::NameOrIdKey { key: Some(alias.into()) },
            ],
            sink_target: default_sink_target(),
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.apply_func(
            unsafe { ::std::mem::transmute(join_kind) },
            move |plan| {
                plan.shuffle(None)
                    .edge_expand(expand_opr.clone().into())
                    .group(fold_opr.clone().into());
            },
            Some(alias.into()),
        );
        job_builder.sink(sink_opr);

        job_builder.build().unwrap()
    }

    fn apply_inner_join(worker_num: u32) {
        initialize();
        // join_kind: InnerJoin
        let request = init_apply_count_request(0, TAG_A);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        // this may not be a good test case.
        // for v2, since it doesn't have any out neighbors, the expected_results seems not include v2.
        // but here, count() will lead to a result of 0, thus v2 is included in expected_results.
        let mut expected_results = vec![(v1, 3), (v2, 0), (v4, 2), (v6, 1)];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        let cnt = record
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_object()
                            .unwrap();

                        result_collection.push((vertex.id() as DefaultId, cnt.as_u64().unwrap()));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_results.sort_by(|v1, v2| v1.0.cmp(&v2.0));
        result_collection.sort_by(|v1, v2| v1.0.cmp(&v2.0));
        assert_eq!(result_collection, expected_results)
    }

    // g.V().apply(out().count(), inner_join)
    #[test]
    fn apply_inner_join_test() {
        apply_inner_join(1)
    }

    // g.V().apply(out().count(), inner_join)
    #[test]
    fn apply_inner_join_w2_test() {
        apply_inner_join(2)
    }

    fn apply_left_out_join(worker_num: u32) {
        initialize();
        // join_kind: LeftOuterJoin
        let request = init_apply_count_request(1, TAG_A);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_result_ids = vec![(v1, Some(3)), (v2, Some(0)), (v4, Some(2)), (v6, Some(1))];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        let object = record
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_object()
                            .unwrap();
                        if object.eq(&Object::None) {
                            result_collection.push((vertex.id() as DefaultId, None));
                        } else {
                            result_collection
                                .push((vertex.id() as DefaultId, Some(object.as_u64().unwrap())));
                        }
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort_by(|v1, v2| v1.0.cmp(&v2.0));
        result_collection.sort_by(|v1, v2| v1.0.cmp(&v2.0));
        assert_eq!(result_collection, expected_result_ids)
    }

    // g.V().apply(out().count(), left_outer_join)
    #[test]
    fn apply_left_out_join_test() {
        apply_left_out_join(1)
    }

    // g.V().apply(out().count(), left_outer_join)
    #[test]
    fn apply_left_out_join_w2_test() {
        apply_left_out_join(2)
    }
}
