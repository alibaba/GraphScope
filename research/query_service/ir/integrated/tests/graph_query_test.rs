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
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus_client::builder::*;
    use pegasus_server::JobRequest;
    use prost::Message;
    use runtime::graph::element::GraphElement;

    use crate::common::test::*;

    fn init_poc_request() -> JobRequest {
        // g.V().hasLabel("person").has("id", 1).out("knows").limit(10)
        let source_opr = pb::Scan {
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
        };
        let select_opr = pb::Select { predicate: Some(str_to_expr_pb("@.id == 1".to_string()).unwrap()) };
        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(pb::QueryParams {
                table_names: vec![common_pb::NameOrId::from("knows".to_string())],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge: false,
            alias: None,
        };
        let source_opr_bytes = pb::logical_plan::Operator::from(source_opr).encode_to_vec();
        let select_opr_bytes = pb::logical_plan::Operator::from(select_opr).encode_to_vec();
        let expand_opr_bytes = pb::logical_plan::Operator::from(expand_opr).encode_to_vec();
        let sink_opr_bytes = pb::logical_plan::Operator::from(pb::Sink {
            tags: vec![],
            sink_current: true,
            id_name_mappings: vec![],
        })
        .encode_to_vec();

        let mut job_builder = JobBuilder::default();
        job_builder.add_source(source_opr_bytes.clone());
        job_builder.filter(select_opr_bytes);
        job_builder.flat_map(expand_opr_bytes.clone());
        job_builder.limit(10);
        job_builder.sink(sink_opr_bytes);

        job_builder.build().unwrap()
    }

    // g.V().both("1..3", "knows")
    fn init_path_expand_request() -> JobRequest {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
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
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(pb::QueryParams {
                table_names: vec![common_pb::NameOrId::from("knows".to_string())],
                columns: vec![],
                limit: None,
                predicate: None,
                requirements: vec![],
            }),
            is_edge: false,
            alias: None,
        };

        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, is_whole_path: false });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });
        let sink_opr_bytes = pb::logical_plan::Operator::from(pb::Sink {
            tags: vec![],
            sink_current: true,
            id_name_mappings: vec![],
        })
        .encode_to_vec();

        let mut job_builder = JobBuilder::default();
        job_builder.add_source(source_opr.encode_to_vec());
        job_builder.map(path_start_opr.encode_to_vec());
        job_builder.iterate_emit(2, |plan| {
            plan.flat_map(expand_opr.clone().encode_to_vec());
        });
        job_builder.map(path_end_opr.encode_to_vec());
        job_builder.sink(sink_opr_bytes);

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
                    if let Some(vertex) = entry.get(None).unwrap().as_graph_vertex() {
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
    fn test_path_expand_query() {
        initialize();
        let request = init_path_expand_request();
        let mut results = submit_query(request, 1);
        let mut result_collection = vec![];
        // result paths:
        //     vec![1, 2],
        //     vec![1, 4],
        //     vec![2, 1],
        //     vec![4, 1],
        //     vec![1, 2, 1],
        //     vec![1, 4, 1],
        //     vec![2, 1, 2],
        //     vec![2, 1, 4],
        //     vec![4, 1, 2],
        //     vec![4, 1, 4],
        let expected_result_path_ends = vec![1, 1, 1, 1, 2, 2, 2, 4, 4, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(path.get_path_end().unwrap().id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_path_ends)
    }
}
