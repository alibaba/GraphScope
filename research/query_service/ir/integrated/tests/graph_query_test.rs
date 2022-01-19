//
//! Copyright 2021 Alibaba Group Holding Limited.
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

#[macro_use]
extern crate dyn_type;

#[cfg(test)]
mod test {
    use dyn_type::Object;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_core::plan::logical::LogicalPlan;
    use ir_core::plan::physical::AsPhysical;
    use pegasus_client::builder::*;
    use pegasus_server::JobRequest;
    use runtime::graph::element::GraphElement;
    use runtime::process::record::{Entry, ObjectElement, RecordElement};

    use crate::common::test::*;

    fn init_poc_request() -> JobRequest {
        // g.V().hasLabel("person").has("id", 1).out("knows").limit(10)
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(
            pb::Scan {
                scan_opt: 0,
                alias: None,
                params: Some(pb::QueryParams {
                    table_names: vec![common_pb::NameOrId::from("person".to_string())],
                    columns: vec![],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
                idx_predicate: None,
            }
            .into(),
            vec![],
        ).unwrap();

        plan.append_operator_as_node(
            pb::Select { predicate: Some(str_to_expr_pb("@.id == 1".to_string()).unwrap()) }.into(),
            vec![0],
        ).unwrap();

        plan.append_operator_as_node(
            pb::EdgeExpand {
                base: Some(pb::ExpandBase {
                    v_tag: None,
                    direction: 0,
                    params: Some(pb::QueryParams {
                        table_names: vec![common_pb::NameOrId::from("knows".to_string())],
                        columns: vec![],
                        limit: None,
                        predicate: None,
                        requirements: vec![],
                    }),
                }),
                is_edge: false,
                alias: None,
            }
            .into(),
            vec![1],
        ).unwrap();

        plan.append_operator_as_node(
            pb::Sink { tags: vec![], sink_current: true, id_name_mappings: vec![] }.into(),
            vec![2],
        ).unwrap();

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta).unwrap();

        job_builder.build().unwrap()
    }

    fn init_select_request() -> JobRequest {
        // g.V().as('a').out().as('b').select('a', 'b')
        let mut plan = LogicalPlan::default();
        plan.append_operator_as_node(
            pb::Scan {
                scan_opt: 0,
                alias: Some("a".into()),
                params: Some(pb::QueryParams {
                    table_names: vec![],
                    columns: vec![],
                    limit: None,
                    predicate: None,
                    requirements: vec![],
                }),
                idx_predicate: None,
            }
            .into(),
            vec![],
        ).unwrap();

        plan.append_operator_as_node(
            pb::EdgeExpand {
                base: Some(pb::ExpandBase {
                    v_tag: None,
                    direction: 0,
                    params: Some(pb::QueryParams {
                        table_names: vec![],
                        columns: vec![],
                        limit: None,
                        predicate: None,
                        requirements: vec![],
                    }),
                }),
                is_edge: false,
                alias: Some("b".into()),
            }
            .into(),
            vec![0],
        ).unwrap();

        plan.append_operator_as_node(
            pb::Project {
                mappings: vec![pb::project::ExprAlias {
                    expr: str_to_expr_pb("{@a, @b}".to_string()).ok(),
                    alias: None,
                }],
                is_append: true,
            }
            .into(),
            vec![1],
        ).unwrap();

        plan.append_operator_as_node(
            pb::Sink { tags: vec![], sink_current: true, id_name_mappings: vec![] }.into(),
            vec![2],
        ).unwrap();

        let mut job_builder = JobBuilder::default();
        let mut plan_meta = plan.meta.clone();
        plan.add_job_builder(&mut job_builder, &mut plan_meta).unwrap();

        job_builder.build().unwrap()
    }

    #[test]
    fn test_poc_query() {
        initialize();
        let request = init_poc_request();
        let mut results = submit_query(request, 1);
        let mut result_collection = vec![];
        let expected_result_ids = vec![2, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(vertex) = entry.get(None).unwrap().as_graph_element() {
                        result_collection.push(vertex.id());
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

    #[test]
    fn test_select_query() {
        initialize();
        let request = init_select_request();
        let mut results = submit_query(request, 1);
        let mut computed_results = vec![];
        let mut expected_results = vec![
            vec![
                (object!(vec![object!("a"), object!("")]), object!(1_u64)),
                (object!(vec![object!("b"), object!("")]), object!(4_u64)),
            ],
            vec![
                (object!(vec![object!("a"), object!("")]), object!(1_u64)),
                (object!(vec![object!("b"), object!("")]), object!(72057594037927939_u64)),
            ],
            vec![
                (object!(vec![object!("a"), object!("")]), object!(1_u64)),
                (object!(vec![object!("b"), object!("")]), object!(2_u64)),
            ],
            vec![
                (object!(vec![object!("a"), object!("")]), object!(4_u64)),
                (object!(vec![object!("b"), object!("")]), object!(72057594037927941_u64)),
            ],
            vec![
                (object!(vec![object!("a"), object!("")]), object!(4_u64)),
                (object!(vec![object!("b"), object!("")]), object!(72057594037927939_u64)),
            ],
            vec![
                (object!(vec![object!("a"), object!("")]), object!(6_u64)),
                (object!(vec![object!("b"), object!("")]), object!(72057594037927939_u64)),
            ],
        ];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(item) = entry.get(None) {
                        match item.as_ref() {
                            Entry::Element(RecordElement::OffGraph(obj)) => match obj {
                                ObjectElement::Prop(val) => match val {
                                    Object::KV(kv) => {
                                        let mut tuple = vec![];
                                        for (k, v) in kv {
                                            tuple.push((k.clone(), v.clone()));
                                        }
                                        computed_results.push(tuple);
                                    }
                                    _ => panic!("unexpected result: {:?}", item),
                                },
                                _ => panic!("unexpected result: {:?}", item),
                            },
                            _ => panic!("unexpected result: {:?}", item),
                        }
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        computed_results.sort();
        expected_results.sort();
        assert_eq!(computed_results, expected_results);
    }
}
