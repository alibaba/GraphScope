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
    use graph_proxy::apis::GraphElement;
    use graph_store::common::DefaultId;
    use graph_store::ldbc::LDBCVertexParser;
    use ir_common::generated::algebra as algebra_pb;
    use ir_common::generated::common as common_pb;
    use ir_common::generated::physical as pb;
    use ir_physical_client::physical_builder::*;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;

    // the join implementation of the following match query:
    // g.V().match( __.as("a").hasLabel("person").has("name","marko").out("knows").as("b"), __.as("b").out("created").as("c"))
    fn init_join_request(join_kind: i32) -> JobRequest {
        // all vertices
        let source_opr_1 = pb::Scan {
            scan_opt: 0,
            alias: Some(TAG_A),
            params: Some(query_params(vec![], vec![], None)),
            idx_predicate: None,
        };

        // person vertices
        let source_opr_2 = pb::Scan {
            scan_opt: 0,
            alias: Some(TAG_A),
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_dummy_source();
        job_builder.join_func(
            unsafe { std::mem::transmute(join_kind) },
            move |left| {
                left.add_scan_source(source_opr_1.clone());
            },
            move |right| {
                right.add_scan_source(source_opr_2.clone());
            },
            vec![common_pb::Variable::from("@0.~id".to_string())],
            vec![common_pb::Variable::from("@0.~id".to_string())],
        );
        job_builder.sink(algebra_pb::Sink {
            tags: vec![
                common_pb::NameOrIdKey { key: Some(TAG_A.into()) },
                common_pb::NameOrIdKey { key: None },
            ],
            sink_target: default_sink_target(),
        });

        job_builder.build().unwrap()
    }

    fn inner_join(worker_num: u32) {
        initialize();
        let request = init_join_request(0); // INNER
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_result_ids = vec![v1, v2, v4, v6];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    println!("result {:?}", record);
                    if let Some(vertex) = record.get(Some(TAG_A)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    #[test]
    fn inner_join_test() {
        inner_join(1)
    }

    #[test]
    fn inner_join_w2_test() {
        inner_join(2)
    }

    fn left_join(worker_num: u32) {
        initialize();
        let request = init_join_request(1); // LEFT
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_result_ids = vec![v1, v2, v3, v4, v5, v6];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    println!("result {:?}", record);
                    if let Some(vertex) = record.get(Some(TAG_A)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    #[test]
    fn left_join_test() {
        left_join(1)
    }

    #[test]
    fn left_join_w2_test() {
        left_join(2)
    }

    fn right_join(worker_num: u32) {
        initialize();
        let request = init_join_request(2); // RIGHT
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_result_ids = vec![v1, v2, v4, v6];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    println!("result {:?}", record);
                    if let Some(vertex) = record.get(Some(TAG_A)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    #[test]
    fn right_join_test() {
        right_join(1)
    }

    #[test]
    fn right_join_w2_test() {
        right_join(2)
    }

    fn full_join(worker_num: u32) {
        initialize();
        let request = init_join_request(3); // FULL
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_result_ids = vec![v1, v2, v3, v4, v5, v6];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    println!("result {:?}", record);
                    if let Some(vertex) = record.get(Some(TAG_A)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    #[test]
    fn full_join_test() {
        full_join(1)
    }

    #[test]
    fn full_join_w2_test() {
        full_join(2)
    }

    fn semi_join(worker_num: u32) {
        initialize();
        let request = init_join_request(4); // SEMI
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
        let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);
        let mut expected_result_ids = vec![v1, v2, v4, v6];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    println!("result {:?}", record);
                    if let Some(vertex) = record.get(Some(TAG_A)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    #[test]
    fn semi_join_test() {
        semi_join(1)
    }

    #[test]
    fn semi_join_w2_test() {
        semi_join(2)
    }

    fn anti_join(worker_num: u32) {
        initialize();
        let request = init_join_request(5); // ANTI
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
        let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
        let mut expected_result_ids = vec![v3, v5];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    println!("result {:?}", record);
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as DefaultId);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids)
    }

    #[test]
    fn anti_join_test() {
        anti_join(1)
    }

    #[test]
    fn anti_join_w2_test() {
        anti_join(2)
    }
}
