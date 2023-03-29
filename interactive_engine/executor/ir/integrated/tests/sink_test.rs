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

    use graph_proxy::apis::GraphElement;
    use graph_store::common::DefaultId;
    use graph_store::ldbc::LDBCVertexParser;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_common::KeyId;
    use ir_physical_client::physical_builder::JobBuilder;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;

    fn init_sink_request(
        source_alias: Option<KeyId>, sink_keys: Vec<common_pb::NameOrIdKey>,
    ) -> JobRequest {
        // g.V()
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: source_alias.map(|tag| tag.into()),
            params: Some(query_params(vec![], vec![], None)),
            idx_predicate: None,
            meta_data: None,
        };

        let sink_opr = pb::Sink { tags: sink_keys, sink_target: default_sink_target() };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.sink(sink_opr);

        job_builder.build().unwrap()
    }

    // g.V().as(0) + Sink(0)
    #[test]
    fn sink_with_alias() {
        initialize();
        let request = init_sink_request(Some(0), vec![common_pb::NameOrIdKey { key: Some(0.into()) }]);
        let mut results = submit_query(request, 1);
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
                    let entry = parse_result(res).unwrap();
                    // head is not sinked
                    assert!(entry.get(None).is_none());
                    if let Some(vertex) = entry.get(Some(0)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as usize);
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

    // g.V() + Sink(None)
    #[test]
    fn sink_with_head() {
        initialize();
        let request = init_sink_request(None, vec![common_pb::NameOrIdKey { key: None }]);
        let mut results = submit_query(request, 1);
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
                    let entry = parse_result(res).unwrap();
                    if let Some(vertex) = entry.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as usize);
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

    // g.V().as(0) + Sink(0, None)
    #[test]
    fn sink_with_head_and_alias() {
        initialize();
        let request = init_sink_request(
            Some(0.into()),
            vec![common_pb::NameOrIdKey { key: None }, common_pb::NameOrIdKey { key: Some(0.into()) }],
        );
        let mut results = submit_query(request, 1);
        let mut result_collection = vec![];
        let mut result_head_collection = vec![];
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
                    let entry = parse_result(res).unwrap();
                    if let Some(vertex) = entry.get(None).unwrap().as_vertex() {
                        result_head_collection.push(vertex.id() as usize);
                    }
                    if let Some(vertex) = entry.get(Some(0)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as usize);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        result_head_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
        assert_eq!(result_head_collection, expected_result_ids);
    }

    // g.V().as(0) + Sink empty tags (i.e., here is Sink(0) by default)
    #[test]
    fn sink_default_for_aliased_records() {
        initialize();
        let request = init_sink_request(Some(0.into()), vec![]);
        let mut results = submit_query(request, 1);
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
                    let entry = parse_result(res).unwrap();
                    // head is not sinked by default.
                    assert!(entry.get(None).is_none());
                    if let Some(vertex) = entry.get(Some(0)).unwrap().as_vertex() {
                        result_collection.push(vertex.id() as usize);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_ids.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_ids);
    }

    // g.V() + Sink empty tags (i.e., here nothing is sinked by default as no records is tagged.)
    #[test]
    fn sink_default_for_unaliased_records() {
        initialize();
        let request = init_sink_request(None, vec![]);
        let mut results = submit_query(request, 1);
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    // head is not sinked
                    assert!(entry.get(None).is_none());
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
    }
}
