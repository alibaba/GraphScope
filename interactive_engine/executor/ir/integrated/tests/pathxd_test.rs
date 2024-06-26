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
    use dyn_type::{object, Object};
    use graph_proxy::apis::{Element, GraphElement, ID};
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_common::generated::common as common_pb;
    use ir_physical_client::physical_builder::*;
    use pegasus_common::downcast::AsAny;
    use pegasus_server::JobRequest;
    use runtime::process::entry::{CollectionEntry, Entry};

    use crate::common::test::*;

    // g.V().hasLabel("person").both("lower..upper", "knows")
    // result_opt: 0: EndV, 1: AllV, 2: AllVE;  path_opt: 0: Arbitrary, 1: Simple
    fn init_path_expand_request(range: pb::Range, result_opt: i32, path_opt: i32) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(range),
            path_opt,
            result_opt,
            condition: None,
            is_optional: false,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("2..3", "knows")
    // result_opt: 0: EndV, 1: AllV, 2: AllVE;
    fn init_path_expand_exactly_request(result_opt: i32) -> JobRequest {
        init_path_expand_request(pb::Range { lower: 2, upper: 3 }, result_opt, 0)
    }

    // g.V().hasLabel("person").both("0..3", "knows")
    fn init_path_expand_range_from_zero_request(result_opt: i32) -> JobRequest {
        init_path_expand_request(pb::Range { lower: 0, upper: 3 }, result_opt, 0)
    }

    // g.V().hasLabel("person").both("lower..upper", "knows").with("UNTIL", "@.name == \"marko\"")
    fn init_path_expand_with_until_request(range: pb::Range) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(range),
            path_opt: 0,
            result_opt: 1,
            condition: str_to_expr_pb("@.name == \"marko\"".to_string()).ok(),
            is_optional: false,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("1..3", "knows") with vertex's filter "@.age>28"
    // Notice that, this is not equivalent to g.V().hasLabel("person").both("1..3", "knows").has("@.age>28").
    fn init_path_expand_with_filter_request(is_whole_path: bool) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let getv = pb::GetV {
            tag: None,
            opt: 4,
            params: Some(query_params(vec![], vec![], str_to_expr_pb("@.age >28".to_string()).ok())),
            alias: None,
            meta_data: None,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some((edge_expand, getv).into()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 1, upper: 3 }),
            path_opt: 0,
            result_opt: if is_whole_path { 1 } else { 0 },
            condition: None,
            is_optional: false,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // both(1..3)
    fn path_expand_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(pb::Range { lower: 1, upper: 3 }, 1, 0);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
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
        let exptected_result_path_lengths = vec![1, 1, 1, 1, 2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, exptected_result_path_lengths);
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
        let request = init_path_expand_request(pb::Range { lower: 1, upper: 3 }, 0, 0);
        let mut results = submit_query(request, num_workers);
        let mut result_collection = vec![];
        let expected_result_path_ends = vec![1, 1, 1, 1, 2, 2, 2, 4, 4, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(path.get_path_end().id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_path_ends);
    }

    #[test]
    fn path_expand_end_query_test() {
        path_expand_end_query(1)
    }

    #[test]
    fn path_expand_end_query_w2_test() {
        path_expand_end_query(2)
    }

    // both(2..3)
    fn path_expand_exactly_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_exactly_request(1);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths =
            vec![vec![1, 2, 1], vec![1, 4, 1], vec![2, 1, 2], vec![2, 1, 4], vec![4, 1, 2], vec![4, 1, 4]];
        let expected_result_path_lens = vec![2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_exactly_whole_query_test() {
        path_expand_exactly_whole_query(1)
    }

    #[test]
    fn path_expand_exactly_whole_query_w2_test() {
        path_expand_exactly_whole_query(2)
    }

    fn path_expand_exactly_end_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_exactly_request(0);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        let expected_result_path_ends = vec![1, 1, 2, 2, 4, 4];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(path.get_path_end().id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        result_collection.sort();
        assert_eq!(result_collection, expected_result_path_ends);
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
        let request = init_path_expand_range_from_zero_request(1);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
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
        let expected_result_path_lens = vec![0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_range_from_zero_query_test() {
        path_expand_range_from_zero_whole_query(1)
    }

    #[test]
    fn path_expand_range_from_zero_query_w2_test() {
        path_expand_range_from_zero_whole_query(2)
    }

    fn simple_path_expand_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(pb::Range { lower: 1, upper: 3 }, 1, 1);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths =
            vec![vec![1, 2], vec![1, 4], vec![2, 1], vec![4, 1], vec![2, 1, 4], vec![4, 1, 2]];
        let expected_result_path_lens = vec![1, 1, 1, 1, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn simple_path_expand_whole_query_test() {
        simple_path_expand_whole_query(1)
    }

    #[test]
    fn simple_path_expand_whole_query_w2_test() {
        simple_path_expand_whole_query(2)
    }

    fn simple_path_expand_end_query(num_workers: u32) {
        initialize();
        let request = init_path_expand_request(pb::Range { lower: 1, upper: 3 }, 0, 1);
        let mut results = submit_query(request, num_workers);
        let mut result_collection = vec![];
        let mut expected_result_path_ends = vec![2, 4, 1, 1, 4, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_collection.push(path.get_path_end().id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_path_ends.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_path_ends);
    }

    #[test]
    fn simple_path_expand_end_query_test() {
        simple_path_expand_end_query(1)
    }

    #[test]
    fn simple_path_expand_end_query_w2_test() {
        simple_path_expand_end_query(2)
    }

    // both(1..3).until()
    fn path_expand_whole_with_until_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_with_until_request(pb::Range { lower: 1, upper: 3 });
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths = vec![vec![2, 1], vec![4, 1], vec![1, 2, 1], vec![1, 4, 1]];
        let expected_result_path_lens = vec![1, 1, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_whole_with_until_query_test() {
        path_expand_whole_with_until_query(1)
    }

    #[test]
    fn path_expand_whole_with_until_query_w2_test() {
        path_expand_whole_with_until_query(2)
    }

    // both(2..3).until()
    fn path_expand_exactly_whole_with_until_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_with_until_request(pb::Range { lower: 2, upper: 3 });
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths =
            vec![vec![1, 2, 1], vec![1, 4, 1], vec![2, 1, 2], vec![2, 1, 4], vec![4, 1, 2], vec![4, 1, 4]];
        let expected_result_path_lens = vec![2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_exactly_whole_with_until_query_test() {
        path_expand_exactly_whole_with_until_query(1)
    }

    #[test]
    fn path_expand_exactly_whole_with_until_query_w2_test() {
        path_expand_exactly_whole_with_until_query(2)
    }

    // both(0..3).until()
    fn path_expand_range_from_zero_with_until_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_with_until_request(pb::Range { lower: 0, upper: 3 });
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths = vec![vec![1], vec![2, 1], vec![4, 1]];
        let expected_result_path_lens = vec![0, 1, 1];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_range_from_zero_with_until_query_test() {
        path_expand_range_from_zero_with_until_query(1)
    }

    #[test]
    fn path_expand_range_from_zero_with_until_query_w2_test() {
        path_expand_range_from_zero_with_until_query(2)
    }

    fn path_expand_with_filter_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_with_filter_request(true);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths =
            vec![vec![1, 4], vec![2, 1], vec![4, 1], vec![1, 4, 1], vec![2, 1, 4], vec![4, 1, 4]];
        let expected_result_path_lens = vec![1, 1, 1, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
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
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_with_filter_query_test() {
        path_expand_with_filter_query(1)
    }

    #[test]
    fn path_expand_with_filter_query_w2_test() {
        path_expand_with_filter_query(2)
    }

    // both(1..3) with vertices and edges preserved in the path
    fn path_expand_whole_v_e_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(pb::Range { lower: 1, upper: 3 }, 2, 0);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<String>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths: Vec<Vec<String>> = vec![
            vec!["v1", "e[1->2]", "v2"],
            vec!["v1", "e[1->4]", "v4"],
            vec!["v2", "e[1->2]", "v1"],
            vec!["v4", "e[1->4]", "v1"],
            vec!["v1", "e[1->2]", "v2", "e[1->2]", "v1"],
            vec!["v1", "e[1->4]", "v4", "e[1->4]", "v1"],
            vec!["v2", "e[1->2]", "v1", "e[1->2]", "v2"],
            vec!["v2", "e[1->2]", "v1", "e[1->4]", "v4"],
            vec!["v4", "e[1->4]", "v1", "e[1->2]", "v2"],
            vec!["v4", "e[1->4]", "v1", "e[1->4]", "v4"],
        ]
        .into_iter()
        .map(|ids| {
            ids.into_iter()
                .map(|id| id.to_string())
                .collect()
        })
        .collect();
        let expected_result_path_lens = vec![1, 1, 1, 1, 2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
                        let path_collect = path.clone().take_path().unwrap();
                        let mut path_ids = vec![];
                        for v_or_e in path_collect {
                            match v_or_e {
                                graph_proxy::apis::VertexOrEdge::V(v) => {
                                    path_ids.push(format!("v{}", v.id()))
                                }
                                graph_proxy::apis::VertexOrEdge::E(e) => {
                                    path_ids.push(format!("e[{}->{}]", e.src_id, e.dst_id));
                                }
                            }
                        }
                        result_collection.push(path_ids);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_whole_v_e_test() {
        path_expand_whole_v_e_query(1)
    }

    #[test]
    fn path_expand_whole_v_e_w2_test() {
        path_expand_whole_v_e_query(2)
    }

    // both(0..3) with vertices and edges preserved in the path
    fn path_expand_range_from_zero_whole_v_e_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_range_from_zero_request(2);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<String>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths: Vec<Vec<String>> = vec![
            vec!["v1"],
            vec!["v2"],
            vec!["v4"],
            vec!["v6"],
            vec!["v1", "e[1->2]", "v2"],
            vec!["v1", "e[1->4]", "v4"],
            vec!["v2", "e[1->2]", "v1"],
            vec!["v4", "e[1->4]", "v1"],
            vec!["v1", "e[1->2]", "v2", "e[1->2]", "v1"],
            vec!["v1", "e[1->4]", "v4", "e[1->4]", "v1"],
            vec!["v2", "e[1->2]", "v1", "e[1->2]", "v2"],
            vec!["v2", "e[1->2]", "v1", "e[1->4]", "v4"],
            vec!["v4", "e[1->4]", "v1", "e[1->2]", "v2"],
            vec!["v4", "e[1->4]", "v1", "e[1->4]", "v4"],
        ]
        .into_iter()
        .map(|ids| {
            ids.into_iter()
                .map(|id| id.to_string())
                .collect()
        })
        .collect();
        let expected_result_path_lens = vec![0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
                        let path_collect = path.clone().take_path().unwrap();
                        let mut path_ids = vec![];
                        for v_or_e in path_collect {
                            match v_or_e {
                                graph_proxy::apis::VertexOrEdge::V(v) => {
                                    path_ids.push(format!("v{}", v.id()));
                                }
                                graph_proxy::apis::VertexOrEdge::E(e) => {
                                    path_ids.push(format!("e[{}->{}]", e.src_id, e.dst_id));
                                }
                            }
                        }
                        result_collection.push(path_ids);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        result_length_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_range_from_zero_whole_v_e_test() {
        path_expand_range_from_zero_whole_v_e_query(1)
    }

    #[test]
    fn path_expand_range_from_zero_whole_v_e_w2_test() {
        path_expand_range_from_zero_whole_v_e_query(2)
    }

    // both(2..3) with vertices and edges preserved in the path
    fn path_expand_exactly_whole_v_e_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_exactly_request(2);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<String>> = vec![];
        let mut result_length_collection = vec![];
        let mut expected_result_paths: Vec<Vec<String>> = vec![
            vec!["v1", "e[1->2]", "v2", "e[1->2]", "v1"],
            vec!["v1", "e[1->4]", "v4", "e[1->4]", "v1"],
            vec!["v2", "e[1->2]", "v1", "e[1->2]", "v2"],
            vec!["v2", "e[1->2]", "v1", "e[1->4]", "v4"],
            vec!["v4", "e[1->4]", "v1", "e[1->2]", "v2"],
            vec!["v4", "e[1->4]", "v1", "e[1->4]", "v4"],
        ]
        .into_iter()
        .map(|ids| {
            ids.into_iter()
                .map(|id| id.to_string())
                .collect()
        })
        .collect();
        let expected_result_path_lens = vec![2, 2, 2, 2, 2, 2];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
                        result_length_collection.push(path.len());
                        let path_collect = path.clone().take_path().unwrap();
                        let mut path_ids = vec![];
                        for v_or_e in path_collect {
                            match v_or_e {
                                graph_proxy::apis::VertexOrEdge::V(v) => {
                                    path_ids.push(format!("v{}", v.id()));
                                }
                                graph_proxy::apis::VertexOrEdge::E(e) => {
                                    path_ids.push(format!("e[{}->{}]", e.src_id, e.dst_id));
                                }
                            }
                        }
                        result_collection.push(path_ids);
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
        assert_eq!(result_length_collection, expected_result_path_lens);
    }

    #[test]
    fn path_expand_exactly_whole_v_e_test() {
        path_expand_exactly_whole_v_e_query(1)
    }

    #[test]
    fn path_expand_exactly_whole_v_e_w2_test() {
        path_expand_exactly_whole_v_e_query(2)
    }

    // g.V().hasLabel("person").both("2..3", "knows").values("name")
    fn init_path_expand_project_request(result_opt: i32) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            path_opt: 0, // Arbitrary
            result_opt,
            condition: None,
            is_optional: false,
        };

        let project_opr = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                alias: None,
            }],
            is_append: true,
            meta_data: vec![],
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.project(project_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    #[test]
    fn path_expand_allv_project_test() {
        initialize();
        let request = init_path_expand_project_request(1); // all v
        let mut results = submit_query(request, 2);
        let mut result_collection: Vec<Object> = vec![];
        let mut expected_result_paths: Vec<Object> = vec![
            Object::Vector(vec![object!("marko"), object!("vadas"), object!("marko")]),
            Object::Vector(vec![object!("marko"), object!("josh"), object!("marko")]),
            Object::Vector(vec![object!("vadas"), object!("marko"), object!("vadas")]),
            Object::Vector(vec![object!("vadas"), object!("marko"), object!("josh")]),
            Object::Vector(vec![object!("josh"), object!("marko"), object!("vadas")]),
            Object::Vector(vec![object!("josh"), object!("marko"), object!("josh")]),
        ];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(e) = entry.get(None) {
                        let collection = e
                            .as_any_ref()
                            .downcast_ref::<CollectionEntry>()
                            .unwrap();
                        let mut path_values_collection = vec![];
                        for v in collection.inner.iter() {
                            path_values_collection.push(v.as_object().unwrap().clone());
                        }
                        result_collection.push(Object::Vector(path_values_collection));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
    }

    // g.V().hasLabel("person").both("2..3", "knows").unfold()
    fn init_path_expand_unfold_request(result_opt: i32) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            path_opt: 0, // Arbitrary
            result_opt,
            condition: None,
            is_optional: false,
        };

        let unfold_opr = pb::Unfold { tag: None, alias: None, meta_data: None };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.unfold(unfold_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    #[test]
    fn path_expand_allv_unfold_test() {
        initialize();
        let request = init_path_expand_unfold_request(1); // all v
        let mut results = submit_query(request, 2);

        let mut expected_result_collection: Vec<String> = vec![
            vec!["v1", "v2", "v1"],
            vec!["v1", "v4", "v1"],
            vec!["v2", "v1", "v2"],
            vec!["v2", "v1", "v4"],
            vec!["v4", "v1", "v2"],
            vec!["v4", "v1", "v4"],
        ]
        .into_iter()
        .flat_map(|ids| ids.into_iter().map(|id| id.to_string()))
        .collect();
        let mut result_collection = vec![];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(v) = entry.get(None).unwrap().as_vertex() {
                        result_collection.push(format!("v{}", v.id()));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_collection.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    #[test]
    fn path_expand_allve_unfold_test() {
        initialize();
        let request = init_path_expand_unfold_request(2); // all ve
        let mut results = submit_query(request, 2);

        let mut expected_result_collection: Vec<String> = vec![
            vec!["v1", "e[1->2]", "v2", "e[1->2]", "v1"],
            vec!["v1", "e[1->4]", "v4", "e[1->4]", "v1"],
            vec!["v2", "e[1->2]", "v1", "e[1->2]", "v2"],
            vec!["v2", "e[1->2]", "v1", "e[1->4]", "v4"],
            vec!["v4", "e[1->4]", "v1", "e[1->2]", "v2"],
            vec!["v4", "e[1->4]", "v1", "e[1->4]", "v4"],
        ]
        .into_iter()
        .flat_map(|ids| ids.into_iter().map(|id| id.to_string()))
        .collect();

        let mut result_collection = vec![];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(v) = entry.get(None).unwrap().as_vertex() {
                        result_collection.push(format!("v{}", v.id()));
                    } else if let Some(e) = entry.get(None).unwrap().as_edge() {
                        result_collection.push(format!("e[{}->{}]", e.src_id, e.dst_id));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_collection.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    // g.V().hasLabel("person").both("3..4").with('RESULT_OPT', 'ALL_V_E').with('PATH_OPT', 'SIMPLE').endV().values("name")
    #[test]
    fn path_expand_endv_project_test() {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 3, upper: 4 }),
            path_opt: 1,   // Simple
            result_opt: 2, // AllVE
            condition: None,
            is_optional: false,
        };

        let path_end = pb::GetV {
            tag: None,
            opt: 1, // endV
            params: None,
            alias: None,
            meta_data: None,
        };

        let project_opr = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(str_to_expr_pb("@.name".to_string()).unwrap()),
                alias: None,
            }],
            is_append: true,
            meta_data: vec![],
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.get_v(path_end);
        job_builder.project(project_opr);
        job_builder.sink(default_sink_pb());

        let request = job_builder.build().unwrap();

        initialize();
        let mut results = submit_query(request, 2);

        let mut expected_result_collection: Vec<String> = vec![
            "marko", "josh", "josh", "peter", "peter", "peter", "vadas", "vadas", "lop", "ripple",
            "ripple", "ripple",
        ]
        .into_iter()
        .map(|name| name.to_string())
        .collect();

        let mut result_collection = vec![];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(v) = entry.get(None).unwrap().as_object() {
                        result_collection.push(v.to_string());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_collection.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_collection);
    }

    // g.V().hasLabel("person").both("2..3", "knows").values("xx"), where values("xx") would be computed via PathFunc
    fn init_path_expand_project_path_func_request(
        result_opt: i32, path_key: common_pb::path_function::PathKey,
    ) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
            is_optional: false,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            path_opt: 0, // Arbitrary
            result_opt,
            condition: None,
            is_optional: false,
        };

        // to project path.name
        let project_opr = pb::Project {
            mappings: vec![pb::project::ExprAlias {
                expr: Some(common_pb::Expression {
                    operators: vec![common_pb::ExprOpr {
                        node_type: None,
                        item: Some(common_pb::expr_opr::Item::PathFunc(common_pb::PathFunction {
                            tag: None,
                            opt: 0, // opt is not supported for now
                            node_type: None,
                            path_key: Some(path_key),
                        })),
                    }],
                }),
                alias: Some(TAG_A.into()),
            }],
            is_append: false,
            meta_data: vec![],
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.project(project_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("2..3", "knows").with("RESULT_OPT", "ALL_V").values("name"),
    #[test]
    fn path_expand_allv_project_path_func_test() {
        initialize();
        let path_key = common_pb::path_function::PathKey::Property(common_pb::Property {
            item: Some(common_pb::property::Item::Key("name".into())),
        });
        let request = init_path_expand_project_path_func_request(1, path_key); // all v
        let mut results = submit_query(request, 2);
        let mut result_collection: Vec<Object> = vec![];
        let mut expected_result_paths: Vec<Object> = vec![
            Object::Vector(vec![object!("marko"), object!("vadas"), object!("marko")]),
            Object::Vector(vec![object!("marko"), object!("josh"), object!("marko")]),
            Object::Vector(vec![object!("vadas"), object!("marko"), object!("vadas")]),
            Object::Vector(vec![object!("vadas"), object!("marko"), object!("josh")]),
            Object::Vector(vec![object!("josh"), object!("marko"), object!("vadas")]),
            Object::Vector(vec![object!("josh"), object!("marko"), object!("josh")]),
        ];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(e) = entry.get(None) {
                        let collection = e
                            .as_any_ref()
                            .downcast_ref::<CollectionEntry>()
                            .unwrap();
                        let mut path_values_collection = vec![];
                        for v in collection.inner.iter() {
                            path_values_collection.push(v.as_object().unwrap().clone());
                        }
                        result_collection.push(Object::Vector(path_values_collection));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
    }

    // g.V().hasLabel("person").both("2..3", "knows").with("RESULT_OPT", "END_V").values("name"),
    #[test]
    fn path_expand_endv_project_path_func_test() {
        initialize();
        let path_key = common_pb::path_function::PathKey::Property(common_pb::Property {
            item: Some(common_pb::property::Item::Key("name".into())),
        });
        let request = init_path_expand_project_path_func_request(0, path_key); // endv
        let mut results = submit_query(request, 2);
        let mut result_collection: Vec<Object> = vec![];
        let mut expected_result_paths: Vec<Object> = vec![
            Object::Vector(vec![object!("marko")]),
            Object::Vector(vec![object!("marko")]),
            Object::Vector(vec![object!("vadas")]),
            Object::Vector(vec![object!("josh")]),
            Object::Vector(vec![object!("vadas")]),
            Object::Vector(vec![object!("josh")]),
        ];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(e) = entry.get(None) {
                        let collection = e
                            .as_any_ref()
                            .downcast_ref::<CollectionEntry>()
                            .unwrap();
                        let mut path_values_collection = vec![];
                        for v in collection.inner.iter() {
                            path_values_collection.push(v.as_object().unwrap().clone());
                        }
                        result_collection.push(Object::Vector(path_values_collection));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
    }

    // g.V().hasLabel("person").both("2..3", "knows").with("RESULT_OPT", "ALL_VE").values("name", "weight"),
    #[test]
    fn path_expand_allve_project_path_func_test() {
        initialize();
        let path_key = common_pb::path_function::PathKey::Vars(common_pb::path_function::PathElementKeys {
            keys: vec![
                common_pb::Property { item: Some(common_pb::property::Item::Key("name".into())) },
                common_pb::Property { item: Some(common_pb::property::Item::Key("weight".into())) },
            ],
        });
        let request = init_path_expand_project_path_func_request(2, path_key); // all ve
        let mut results = submit_query(request, 2);
        let mut result_collection: Vec<Object> = vec![];
        let not_exist = object!("");
        let mut expected_result_paths: Vec<Object> = vec![
            // marko - 0.5 - vadas - 0.5 - marko
            Object::Vector(
                vec![
                    // e.g., for the vertex element, name: marko, weight, None
                    vec![object!("marko"), not_exist.clone()],
                    // e.g., for the edge element, name: None, weight: 0.5
                    vec![not_exist.clone(), object!("0.5")],
                    vec![object!("vadas"), not_exist.clone()],
                    vec![not_exist.clone(), object!("0.5")],
                    vec![object!("marko"), not_exist.clone()],
                ]
                .into_iter()
                .map(|vec_obj| Object::Vector(vec_obj))
                .collect(),
            ),
            // marko - 1 - josh - 1 - marko
            Object::Vector(
                vec![
                    vec![object!("marko"), not_exist.clone()],
                    vec![not_exist.clone(), object!("1")],
                    vec![object!("josh"), not_exist.clone()],
                    vec![not_exist.clone(), object!("1")],
                    vec![object!("marko"), not_exist.clone()],
                ]
                .into_iter()
                .map(|vec_obj| Object::Vector(vec_obj))
                .collect(),
            ),
            // vadas - 0.5 - marko - 0.5 - vadas
            Object::Vector(
                vec![
                    vec![object!("vadas"), not_exist.clone()],
                    vec![not_exist.clone(), object!("0.5")],
                    vec![object!("marko"), not_exist.clone()],
                    vec![not_exist.clone(), object!("0.5")],
                    vec![object!("vadas"), not_exist.clone()],
                ]
                .into_iter()
                .map(|vec_obj| Object::Vector(vec_obj))
                .collect(),
            ),
            // vadas - 0.5 - marko - 1 - josh
            Object::Vector(
                vec![
                    // e.g., for the vertex element, name: marko, weight, None
                    vec![object!("vadas"), not_exist.clone()],
                    // e.g., for the edge element, name: None, weight: 0.5
                    vec![not_exist.clone(), object!("0.5")],
                    vec![object!("marko"), not_exist.clone()],
                    vec![not_exist.clone(), object!("1")],
                    vec![object!("josh"), not_exist.clone()],
                ]
                .into_iter()
                .map(|vec_obj| Object::Vector(vec_obj))
                .collect(),
            ),
            // josh - 1 - marko - 0.5 - vadas
            Object::Vector(
                vec![
                    vec![object!("josh"), not_exist.clone()],
                    vec![not_exist.clone(), object!("1")],
                    vec![object!("marko"), not_exist.clone()],
                    vec![not_exist.clone(), object!("0.5")],
                    vec![object!("vadas"), not_exist.clone()],
                ]
                .into_iter()
                .map(|vec_obj| Object::Vector(vec_obj))
                .collect(),
            ),
            // josh - 1 - marko - 1 - josh
            Object::Vector(
                vec![
                    vec![object!("josh"), not_exist.clone()],
                    vec![not_exist.clone(), object!("1")],
                    vec![object!("marko"), not_exist.clone()],
                    vec![not_exist.clone(), object!("1")],
                    vec![object!("josh"), not_exist.clone()],
                ]
                .into_iter()
                .map(|vec_obj| Object::Vector(vec_obj))
                .collect(),
            ),
        ];

        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(e) = entry.get(None) {
                        println!("entry {:?}", e);
                        let collection = e
                            .as_any_ref()
                            .downcast_ref::<CollectionEntry>()
                            .unwrap();
                        let mut path_values_collection = vec![];
                        for v in collection.inner.iter() {
                            path_values_collection.push(v.as_object().unwrap().clone());
                        }
                        result_collection.push(Object::Vector(path_values_collection));
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        expected_result_paths.sort();
        result_collection.sort();
        assert_eq!(result_collection, expected_result_paths);
    }
}
