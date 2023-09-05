//
//! Copyright 2023 Alibaba Group Holding Limited.
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
    use graph_proxy::apis::GraphElement;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::KeyId;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::physical::AsPhysical;
    use ir_physical_client::physical_builder::{JobBuilder, PlanBuilder};
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::{
        default_sink_target, initialize, parse_result, query_params, submit_query, CREATED_LABEL,
        KNOWS_LABEL, PERSON_LABEL, SOFTWARE_LABEL, TAG_A,
    };

    fn build_job_request(plan: LogicalPlan) -> JobRequest {
        let mut plan_builder = PlanBuilder::default();
        let mut plan_meta = plan.get_meta().clone();
        plan.add_job_builder(&mut plan_builder, &mut plan_meta)
            .unwrap();

        let job_builder = JobBuilder::with_plan(plan_builder);
        job_builder.build().unwrap()
    }

    fn get_person_scan(alias: Option<common_pb::NameOrId>) -> pb::Scan {
        pb::Scan {
            scan_opt: 0,
            alias,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            meta_data: None,
        }
    }

    fn get_software_scan(alias: Option<common_pb::NameOrId>) -> pb::Scan {
        pb::Scan {
            scan_opt: 0,
            alias,
            params: Some(query_params(vec![SOFTWARE_LABEL.into()], vec![], None)),
            idx_predicate: None,
            meta_data: None,
        }
    }

    fn get_out_edge(alias: Option<common_pb::NameOrId>) -> pb::EdgeExpand {
        pb::EdgeExpand { v_tag: None, direction: 0, params: None, expand_opt: 0, alias, meta_data: None }
    }

    fn get_in_edge(alias: Option<common_pb::NameOrId>) -> pb::EdgeExpand {
        pb::EdgeExpand { v_tag: None, direction: 1, params: None, expand_opt: 0, alias, meta_data: None }
    }

    fn get_out_knows(alias: Option<common_pb::NameOrId>) -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias,
            meta_data: None,
        }
    }

    fn get_out_created(alias: Option<common_pb::NameOrId>) -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias,
            meta_data: None,
        }
    }

    fn get_in_knows(alias: Option<common_pb::NameOrId>) -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 1,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias,
            meta_data: None,
        }
    }

    fn get_in_created(alias: Option<common_pb::NameOrId>) -> pb::EdgeExpand {
        pb::EdgeExpand {
            v_tag: None,
            direction: 1,
            params: Some(query_params(vec![CREATED_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias,
            meta_data: None,
        }
    }

    fn get_select_marko() -> pb::Select {
        pb::Select { predicate: Some(str_to_expr_pb("@.name == \"marko\"".to_string()).unwrap()) }
    }

    fn get_select_vadas() -> pb::Select {
        pb::Select { predicate: Some(str_to_expr_pb("@.name == \"vadas\"".to_string()).unwrap()) }
    }

    fn get_select_josh() -> pb::Select {
        pb::Select { predicate: Some(str_to_expr_pb("@.name == \"josh\"".to_string()).unwrap()) }
    }

    fn get_as(alias: Option<common_pb::NameOrId>) -> pb::As {
        pb::As { alias }
    }

    fn get_union(parents: Vec<i32>) -> pb::Union {
        pb::Union { parents }
    }

    fn get_inner_join(tags: Vec<KeyId>) -> pb::Join {
        let keys: Vec<common_pb::Variable> = tags
            .into_iter()
            .map(|tag| common_pb::Variable { tag: Some(tag.into()), property: None, node_type: None })
            .collect();
        pb::Join { left_keys: keys.clone(), right_keys: keys.clone(), kind: 0 }
    }

    fn get_sink(tags: Vec<KeyId>) -> pb::Sink {
        if tags.is_empty() {
            pb::Sink {
                tags: vec![common_pb::NameOrIdKey { key: None }],
                sink_target: default_sink_target(),
            }
        } else {
            pb::Sink {
                tags: tags
                    .into_iter()
                    .map(|tag| common_pb::NameOrIdKey { key: Some(tag.into()) })
                    .collect(),
                sink_target: default_sink_target(),
            }
        }
    }

    // The plan looks like:
    //       root(0)
    //       / \
    //      1    2
    //     / \   |
    //    3   4  |
    //    \   /  |
    //      5    |
    //       \  /
    //         6
    //         |
    //         7
    // 0: dummy root
    // 1: scan person
    // 2: scan software
    // 3: select marko
    // 4: select vadas
    // 5: union
    // 6: union
    // 7: sink
    fn build_nested_branch_plan1_request() -> JobRequest {
        let mut plan = LogicalPlan::with_root();

        let person_scan = get_person_scan(None);

        let software_scan = get_software_scan(None);

        let select_marko = get_select_marko();

        let select_vadas = get_select_vadas();

        let person_scan_id = plan
            .append_operator_as_node(person_scan.into(), vec![0])
            .unwrap();
        let software_scan_id = plan
            .append_operator_as_node(software_scan.into(), vec![0])
            .unwrap();

        let select_marko_id = plan
            .append_operator_as_node(select_marko.into(), vec![person_scan_id])
            .unwrap();

        let select_vadas_id = plan
            .append_operator_as_node(select_vadas.into(), vec![person_scan_id])
            .unwrap();

        let union1 = get_union(vec![select_marko_id as i32, select_vadas_id as i32]);

        let union1_id = plan
            .append_operator_as_node(union1.into(), vec![select_marko_id, select_vadas_id])
            .unwrap();

        let union2 = get_union(vec![union1_id as i32, software_scan_id as i32]);

        let union2_id = plan
            .append_operator_as_node(union2.into(), vec![union1_id, software_scan_id])
            .unwrap();

        let sink = get_sink(vec![]);

        plan.append_operator_as_node(sink.into(), vec![union2_id])
            .unwrap();

        build_job_request(plan)
    }

    // The plan looks like:
    //         root(1)
    //      /   |   \
    //     2    3    4
    //     \   /    /
    //       5     /
    //        \   /
    //          6
    // 1: scan all vertices
    // 2: in created, alias TAG_A
    // 3: in knows, alias TAG_A
    // 4: out knows, alias TAG_A
    // 5: join in TAG_A
    // 6: union
    fn build_nested_branch_plan2_request() -> JobRequest {
        let mut plan = LogicalPlan::with_root();

        let scan = pb::Scan::default();

        let in_created = get_in_created(Some(TAG_A.into()));

        let in_knows = get_in_knows(Some(TAG_A.into()));

        let out_knows = get_out_knows(Some(TAG_A.into()));

        let scan_id = plan
            .append_operator_as_node(scan.into(), vec![0])
            .unwrap();

        let in_created_id = plan
            .append_operator_as_node(in_created.into(), vec![scan_id])
            .unwrap();

        let in_knows_id = plan
            .append_operator_as_node(in_knows.into(), vec![scan_id])
            .unwrap();

        let out_knows_id = plan
            .append_operator_as_node(out_knows.into(), vec![scan_id])
            .unwrap();

        let join = get_inner_join(vec![TAG_A]);

        let join_id = plan
            .append_operator_as_node(join.into(), vec![in_created_id, in_knows_id])
            .unwrap();

        let union_opr = get_union(vec![join_id as i32, out_knows_id as i32]);

        let union_id = plan
            .append_operator_as_node(union_opr.into(), vec![join_id, out_knows_id])
            .unwrap();

        let sink = get_sink(vec![TAG_A]);

        plan.append_operator_as_node(sink.into(), vec![union_id])
            .unwrap();

        build_job_request(plan)
    }

    // The plan looks like:
    //        root(1)
    //      /   |   \
    //     2    3    4
    //     \   / \   /
    //       5     6
    //        \   /
    //          7
    // 1: Scan all vertices
    // 2: in knows, alias as TAG_A
    // 3: in created, alias as TAG_A
    // 4. out knows, alias as TAG_A
    // 5. join1 in TAG_A
    // 6. join2 in TAG_A
    // 7. union
    fn build_nested_branch_plan3_request() -> JobRequest {
        let mut plan = LogicalPlan::with_root();

        let scan = pb::Scan::default();

        let in_knows = get_in_knows(Some(TAG_A.into()));

        let in_created = get_in_created(Some(TAG_A.into()));

        let out_knows = get_out_knows(Some(TAG_A.into()));

        let scan_id = plan
            .append_operator_as_node(scan.into(), vec![0])
            .unwrap();

        let in_knows_id = plan
            .append_operator_as_node(in_knows.into(), vec![scan_id])
            .unwrap();

        let in_created_id = plan
            .append_operator_as_node(in_created.into(), vec![scan_id])
            .unwrap();

        let out_knows_id = plan
            .append_operator_as_node(out_knows.into(), vec![scan_id])
            .unwrap();

        let join1 = get_inner_join(vec![TAG_A]);

        let join2 = get_inner_join(vec![TAG_A]);

        let join1_id = plan
            .append_operator_as_node(join1.into(), vec![in_knows_id, in_created_id])
            .unwrap();

        let join2_id = plan
            .append_operator_as_node(join2.into(), vec![in_created_id, out_knows_id])
            .unwrap();

        let union_opr = get_union(vec![join1_id as i32, join2_id as i32]);

        let union_id = plan
            .append_operator_as_node(union_opr.into(), vec![join1_id, join2_id])
            .unwrap();

        let sink = get_sink(vec![TAG_A]);

        plan.append_operator_as_node(sink.into(), vec![union_id])
            .unwrap();

        build_job_request(plan)
    }

    // The plan looks like:
    //               root(1)
    //             /   |    \
    //           2     3     4
    //            \   /    /   \
    //              5      6   7
    //              \      \ /
    //               8      9
    //                \   /
    //                 10
    // 1: Scan all vertices
    // 2: select marko
    // 3: select vandas
    // 4: select josh
    // 5: union
    // 6: out created, alias as TAG_A
    // 7: in_knows, alias as TAG_A
    // 8: alias as TAG_A
    // 9: union
    // 10: join in TAG_A
    fn build_nested_branch_plan4_request() -> JobRequest {
        let mut plan = LogicalPlan::with_root();

        let scan = pb::Scan::default();

        let select_marko = get_select_marko();

        let select_vadas = get_select_vadas();

        let select_josh = get_select_josh();

        let out_created = get_out_created(Some(TAG_A.into()));

        let in_knows = get_in_knows(Some(TAG_A.into()));

        let scan_id = plan
            .append_operator_as_node(scan.into(), vec![0])
            .unwrap();

        let select_marko_id = plan
            .append_operator_as_node(select_marko.into(), vec![scan_id])
            .unwrap();

        let select_vadas_id = plan
            .append_operator_as_node(select_vadas.into(), vec![scan_id])
            .unwrap();

        let select_josh_id = plan
            .append_operator_as_node(select_josh.into(), vec![scan_id])
            .unwrap();

        let out_created_id = plan
            .append_operator_as_node(out_created.into(), vec![select_josh_id])
            .unwrap();

        let in_knows_id = plan
            .append_operator_as_node(in_knows.into(), vec![select_josh_id])
            .unwrap();

        let union1 = get_union(vec![select_marko_id as i32, select_vadas_id as i32]);

        let union1_id = plan
            .append_operator_as_node(union1.into(), vec![select_marko_id, select_vadas_id])
            .unwrap();

        let as_opr = get_as(Some(TAG_A.into()));

        let as_id = plan
            .append_operator_as_node(as_opr.into(), vec![union1_id])
            .unwrap();

        let union2 = get_union(vec![out_created_id as i32, in_knows_id as i32]);

        let union2_id = plan
            .append_operator_as_node(union2.into(), vec![out_created_id, in_knows_id])
            .unwrap();

        let join = get_inner_join(vec![TAG_A.into()]);

        let join_id = plan
            .append_operator_as_node(join.into(), vec![as_id, union2_id])
            .unwrap();

        let sink = get_sink(vec![TAG_A]);

        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // The plan looks like:
    //                root(1)
    //               /      \
    //              2        3
    //            /   \    /   \
    //           4    5   6    7
    //            \   /    \  /
    //              8       9
    //                \   /
    //                  10
    // 1: scan all vertices
    // 2: select marko
    // 3: select josh
    // 4: out created, alias as TAG_A
    // 5: out knows, alias as TAG_A
    // 6: out, alias as TAG_A
    // 7: in, alias as TAG_A
    // 8: union
    // 9: union
    // 10: join on TAG_A
    fn build_nested_branch_plan5_request() -> JobRequest {
        let mut plan = LogicalPlan::with_root();

        let scan = pb::Scan::default();

        let select_marko = get_select_marko();

        let select_josh = get_select_josh();

        let out_created = get_out_created(Some(TAG_A.into()));

        let out_knows = get_out_knows(Some(TAG_A.into()));

        let out_edge = get_out_edge(Some(TAG_A.into()));

        let in_edge = get_in_edge(Some(TAG_A.into()));

        let scan_id = plan
            .append_operator_as_node(scan.into(), vec![0])
            .unwrap();

        let select_marko_id = plan
            .append_operator_as_node(select_marko.into(), vec![scan_id])
            .unwrap();

        let select_josh_id = plan
            .append_operator_as_node(select_josh.into(), vec![scan_id])
            .unwrap();

        let out_created_id = plan
            .append_operator_as_node(out_created.into(), vec![select_marko_id])
            .unwrap();

        let out_knows_id = plan
            .append_operator_as_node(out_knows.into(), vec![select_marko_id])
            .unwrap();

        let out_id = plan
            .append_operator_as_node(out_edge.into(), vec![select_josh_id])
            .unwrap();

        let in_id = plan
            .append_operator_as_node(in_edge.into(), vec![select_josh_id])
            .unwrap();

        let union1 = get_union(vec![out_created_id as i32, out_knows_id as i32]);

        let union1_id = plan
            .append_operator_as_node(union1.into(), vec![out_created_id, out_knows_id])
            .unwrap();

        let union2 = get_union(vec![out_id as i32, in_id as i32]);

        let union2_id = plan
            .append_operator_as_node(union2.into(), vec![out_id, in_id])
            .unwrap();

        let join = get_inner_join(vec![TAG_A]);

        let join_id = plan
            .append_operator_as_node(join.into(), vec![union1_id, union2_id])
            .unwrap();

        let sink = get_sink(vec![TAG_A]);

        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    // The plan looks like:
    //                  root(1)
    //                  /   \
    //                 2     3
    //                  \  /   \
    //                   4       5
    //                    \   /    \
    //                      6       7
    //                       \     /
    //                        8   /
    //                         \ /
    //                          9
    // 1: scan all vertices
    // 2: select marko
    // 3: select josh
    // 4: union
    // 5: out created
    // 6: union
    // 7: in edge, alias as TAG_A
    // 8: alias as TAG_A
    // 9: join in TAG_A
    fn build_nested_branch_plan6_request() -> JobRequest {
        let mut plan = LogicalPlan::with_root();

        let scan = pb::Scan::default();

        let select_marko = get_select_marko();

        let select_josh = get_select_josh();

        let out_created = get_out_created(None);

        let in_edge = get_in_edge(Some(TAG_A.into()));

        let scan_id = plan
            .append_operator_as_node(scan.into(), vec![0])
            .unwrap();

        let select_marko_id = plan
            .append_operator_as_node(select_marko.into(), vec![scan_id])
            .unwrap();

        let select_josh_id = plan
            .append_operator_as_node(select_josh.into(), vec![scan_id])
            .unwrap();

        let union1 = get_union(vec![select_josh_id as i32, select_josh_id as i32]);

        let union1_id = plan
            .append_operator_as_node(union1.into(), vec![select_josh_id, select_marko_id])
            .unwrap();

        let out_created_id = plan
            .append_operator_as_node(out_created.into(), vec![select_josh_id])
            .unwrap();

        let union2 = get_union(vec![union1_id as i32, out_created_id as i32]);

        let union2_id = plan
            .append_operator_as_node(union2.into(), vec![union1_id, out_created_id])
            .unwrap();

        let in_edge_id = plan
            .append_operator_as_node(in_edge.into(), vec![out_created_id])
            .unwrap();

        let as_opr = get_as(Some(TAG_A.into()));

        let as_id = plan
            .append_operator_as_node(as_opr.into(), vec![union2_id])
            .unwrap();

        let join = get_inner_join(vec![TAG_A]);

        let join_id = plan
            .append_operator_as_node(join.into(), vec![as_id, in_edge_id])
            .unwrap();

        let sink = get_sink(vec![TAG_A]);

        plan.append_operator_as_node(sink.into(), vec![join_id])
            .unwrap();

        build_job_request(plan)
    }

    #[test]
    fn test_nested_branch_plan1() {
        initialize();
        let request = build_nested_branch_plan1_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_collection = vec![1, 2, 1 << 56 | 3, 1 << 56 | 5];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();

                    result_collection.push(
                        entry
                            .get(None)
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .id(),
                    );
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    #[test]
    fn test_nested_branch_plan2() {
        initialize();
        let request = build_nested_branch_plan2_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_collection = vec![1, 1, 2, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();

                    result_collection.push(
                        entry
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .id(),
                    );
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    #[test]
    fn test_nested_branch_plan3() {
        initialize();
        let request = build_nested_branch_plan3_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_collection = vec![1, 1, 4, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();

                    result_collection.push(
                        entry
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .id(),
                    );
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    #[test]
    fn test_nested_branch_plan4() {
        initialize();
        let request = build_nested_branch_plan4_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_collection = vec![1];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();

                    result_collection.push(
                        entry
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .id(),
                    );
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    #[test]
    fn test_nested_branch_plan5() {
        initialize();
        let request = build_nested_branch_plan5_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_collection = vec![1 << 56 | 3];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();

                    result_collection.push(
                        entry
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .id(),
                    );
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    #[test]
    fn test_nested_branch_plan6() {
        initialize();
        let request = build_nested_branch_plan6_request();
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        let expected_result_collection = vec![1, 4, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();

                    result_collection.push(
                        entry
                            .get(Some(TAG_A))
                            .unwrap()
                            .as_vertex()
                            .unwrap()
                            .id(),
                    );
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }
}
