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

#[cfg(test)]
mod test {
    use dyn_type::object;
    use dyn_type::Object;
    use graph_proxy::apis::GraphElement;
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_physical_client::physical_builder::*;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;

    fn init_poc_request() -> JobRequest {
        // g.V().hasLabel("person").has("id", 1).out("knows").limit(10)
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec!["id".into()], None)),
            idx_predicate: None,
        };
        let select_opr = pb::Select { predicate: Some(str_to_expr_pb("@.id == 1".to_string()).unwrap()) };
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.select(select_opr);
        job_builder.shuffle(None);
        job_builder.edge_expand(expand_opr);
        job_builder.limit(pb::Limit { range: Some(pb::Range { lower: 1, upper: 11 }) });
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    fn poc_query(worker_num: u32) {
        initialize();
        let request = init_poc_request();
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let expected_result_ids = vec![2, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(vertex) = entry.get(None).unwrap().as_vertex() {
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
    fn poc_query_test() {
        poc_query(1)
    }

    #[test]
    fn poc_query_w2_test() {
        poc_query(2)
    }

    // g.V().hasLabel("person").valueMap()
    fn init_get_property_request() -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params_all_columns(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
        };

        let project_opr = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.~all".to_string()).unwrap()),
                alias: None,
            }],
            is_append: true,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.project(project_opr);
        job_builder.sink(default_sink_pb());
        job_builder.build().unwrap()
    }

    // g.V().as("a").out('knows').select("a").valueMap()
    fn init_get_property_after_shuffle_request() -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: Some(TAG_A.into()),
            params: Some(query_params_all_columns(vec![], vec![], None)),
            idx_predicate: None,
        };

        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let project_opr = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(to_expr_var_all_prop_pb(Some(TAG_A.into()))),
                alias: None,
            }],
            is_append: true,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.edge_expand(expand_opr);
        job_builder.project(project_opr);
        job_builder.sink(default_sink_pb());
        job_builder.build().unwrap()
    }

    fn get_all_properties(worker_num: u32) {
        initialize();
        let request = init_get_property_request();
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];

        let prop1: Object = vec![
            (object!("id"), object!(1)),
            (object!("name"), object!("marko")),
            (object!("age"), object!(29)),
        ]
        .into();

        let prop2: Object = vec![
            (object!("id"), object!(2)),
            (object!("name"), object!("vadas")),
            (object!("age"), object!(27)),
        ]
        .into();

        let prop4: Object = vec![
            (object!("id"), object!(4)),
            (object!("name"), object!("josh")),
            (object!("age"), object!(32)),
        ]
        .into();

        let prop6: Object = vec![
            (object!("id"), object!(6)),
            (object!("name"), object!("peter")),
            (object!("age"), object!(35)),
        ]
        .into();

        let mut expected_result_props =
            vec![prop1.to_string(), prop2.to_string(), prop4.to_string(), prop6.to_string()];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    let obj = record.get(None).unwrap().as_object().unwrap();
                    result_collection.push(obj.to_string())
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        expected_result_props.sort();
        assert_eq!(result_collection, expected_result_props)
    }

    fn get_all_properties_after_shuffle(worker_num: u32) {
        initialize();
        let request = init_get_property_after_shuffle_request();
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];

        let prop1: Object = vec![
            (object!("id"), object!(1)),
            (object!("name"), object!("marko")),
            (object!("age"), object!(29)),
        ]
        .into();

        let mut expected_result_props = vec![prop1.to_string(), prop1.to_string()];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    let obj = record.get(None).unwrap().as_object().unwrap();
                    result_collection.push(obj.to_string())
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        expected_result_props.sort();
        assert_eq!(result_collection, expected_result_props)
    }

    // g.V().valueMap()
    #[test]
    fn get_all_property_test() {
        get_all_properties(1)
    }

    // g.V().valueMap()
    #[test]
    fn get_all_property_w2_test() {
        get_all_properties(2)
    }

    // g.V().as("a").out('knows').select("a").valueMap()
    #[test]
    fn get_all_property_after_shuffle_test() {
        get_all_properties_after_shuffle(1)
    }

    // g.V().as("a").out('knows').select("a").valueMap()
    #[test]
    fn get_all_property_after_shuffle_w2_test() {
        get_all_properties_after_shuffle(2)
    }
}
