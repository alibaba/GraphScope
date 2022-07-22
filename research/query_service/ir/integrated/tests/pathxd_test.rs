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
    use graph_proxy::apis::{GraphElement, ID};
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use pegasus_client::builder::*;
    use pegasus_server::job_pb as server_pb;
    use pegasus_server::JobRequest;
    use prost::Message;

    use crate::common::test::*;

    // g.V().hasLabel("person").both("1..3", "knows")
    fn init_path_expand_request(is_whole_path: bool) -> JobRequest {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![], None)),
            idx_predicate: None,
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec!["knows".into()], vec![], None)),
            is_edge: false,
            alias: None,
        };

        let shuffle_opr = common_pb::NameOrIdKey { key: None };
        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, is_whole_path });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });
        let sink_opr_bytes = pb::logical_plan::Operator::from(default_sink_pb()).encode_to_vec();

        let mut job_builder = JobBuilder::default();
        job_builder.add_source(source_opr.encode_to_vec());
        job_builder.filter_map(path_start_opr.encode_to_vec());
        job_builder.repartition(shuffle_opr.clone().encode_to_vec());
        job_builder.flat_map(expand_opr.encode_to_vec());
        job_builder.iterate_emit(server_pb::iteration_emit::EmitKind::EmitBefore, 1, |plan| {
            plan.repartition(shuffle_opr.clone().encode_to_vec());
            plan.flat_map(expand_opr.clone().encode_to_vec());
        });
        job_builder.map(path_end_opr.encode_to_vec());
        job_builder.sink(sink_opr_bytes);

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("2..3", "knows")
    fn init_path_expand_exactly_request(is_whole_path: bool) -> JobRequest {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![], None)),
            idx_predicate: None,
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec!["knows".into()], vec![], None)),
            is_edge: false,
            alias: None,
        };

        let shuffle_opr = common_pb::NameOrIdKey { key: None };
        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, is_whole_path });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });
        let sink_opr_bytes = pb::logical_plan::Operator::from(default_sink_pb()).encode_to_vec();

        let mut job_builder = JobBuilder::default();
        job_builder.add_source(source_opr.encode_to_vec());
        job_builder.filter_map(path_start_opr.encode_to_vec());
        job_builder.repartition(shuffle_opr.clone().encode_to_vec());
        job_builder.flat_map(expand_opr.clone().encode_to_vec());
        job_builder.repartition(shuffle_opr.clone().encode_to_vec());
        job_builder.flat_map(expand_opr.clone().encode_to_vec());
        job_builder.map(path_end_opr.encode_to_vec());
        job_builder.sink(sink_opr_bytes);

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("0..3", "knows")
    fn init_path_expand_range_from_zero_request(is_whole_path: bool) -> JobRequest {
        let source_opr = pb::logical_plan::Operator::from(pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec!["person".into()], vec![], None)),
            idx_predicate: None,
        });

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec!["knows".into()], vec![], None)),
            is_edge: false,
            alias: None,
        };

        let shuffle_opr = common_pb::NameOrIdKey { key: None };
        let expand_opr = pb::logical_plan::Operator::from(edge_expand.clone());
        let path_start_opr =
            pb::logical_plan::Operator::from(pb::PathStart { start_tag: None, is_whole_path });
        let path_end_opr = pb::logical_plan::Operator::from(pb::PathEnd { alias: None });
        let sink_opr_bytes = pb::logical_plan::Operator::from(default_sink_pb()).encode_to_vec();

        let mut job_builder = JobBuilder::default();
        job_builder.add_source(source_opr.encode_to_vec());
        job_builder.filter_map(path_start_opr.encode_to_vec());
        job_builder.iterate_emit(server_pb::iteration_emit::EmitKind::EmitBefore, 2, |plan| {
            plan.repartition(shuffle_opr.clone().encode_to_vec());
            plan.flat_map(expand_opr.clone().encode_to_vec());
        });
        job_builder.map(path_end_opr.encode_to_vec());
        job_builder.sink(sink_opr_bytes);

        job_builder.build().unwrap()
    }

    fn path_expand_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(true);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut expected_result_paths = vec![
            vec![1, 2],
            vec![1, 4],
            vec![2, 1],
            vec![4, 1],
            vec![1, 2, 1],
            vec![1, 4, 1],
            vec![2, 1, 2],
            vec![2, 1, 4],
            vec![4, 1, 2],
            vec![4, 1, 4],
        ];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(
                            path.clone()
                                .take_path()
                                .unwrap()
                                .into_iter()
                                .map(|v| v.id())
                                .collect(),
                        );
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths)
    }

    #[test]
    fn path_expand_whole_query_test() {
        path_expand_whole_query(1)
    }

    #[test]
    fn path_expand_whole_query_w2_test() {
        path_expand_whole_query(2)
    }

    fn path_expand_end_query(num_workers: u32) {
        initialize();
        let request = init_path_expand_request(false);
        let mut results = submit_query(request, num_workers);
        let mut result_collection = vec![];
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

    #[test]
    fn path_expand_end_query_test() {
        path_expand_end_query(1)
    }

    #[test]
    fn path_expand_end_query_w2_test() {
        path_expand_end_query(2)
    }

    fn path_expand_exactly_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_exactly_request(true);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut expected_result_paths =
            vec![vec![1, 2, 1], vec![1, 4, 1], vec![2, 1, 2], vec![2, 1, 4], vec![4, 1, 2], vec![4, 1, 4]];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(
                            path.clone()
                                .take_path()
                                .unwrap()
                                .into_iter()
                                .map(|v| v.id())
                                .collect(),
                        );
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths)
    }

    #[test]
    fn path_expand_exactly_whole_query_test() {
        path_expand_exactly_whole_query(1)
    }

    #[test]
    fn path_expand_exactly_whole_query_w2_test() {
        path_expand_exactly_whole_query(1)
    }

    fn path_expand_exactly_end_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_exactly_request(false);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let expected_result_path_ends = vec![1, 1, 2, 2, 4, 4];
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

    #[test]
    fn path_expand_exactly_end_query_test() {
        path_expand_exactly_end_query(1)
    }

    #[test]
    fn path_expand_exactly_end_query_w2_test() {
        path_expand_exactly_end_query(2)
    }

    fn path_expand_range_from_zero_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_range_from_zero_request(true);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut expected_result_paths = vec![
            vec![1],
            vec![2],
            vec![4],
            vec![6],
            vec![1, 2],
            vec![1, 4],
            vec![2, 1],
            vec![4, 1],
            vec![1, 2, 1],
            vec![1, 4, 1],
            vec![2, 1, 2],
            vec![2, 1, 4],
            vec![4, 1, 2],
            vec![4, 1, 4],
        ];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(
                            path.clone()
                                .take_path()
                                .unwrap()
                                .into_iter()
                                .map(|v| v.id())
                                .collect(),
                        );
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths)
    }

    #[test]
    fn path_expand_range_from_zero_query_test() {
        path_expand_range_from_zero_whole_query(1)
    }

    #[test]
    fn path_expand_range_from_zero_query_w2_test() {
        path_expand_range_from_zero_whole_query(2)
    }
}
