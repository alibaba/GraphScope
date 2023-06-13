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
    use ir_common::expr_parse::str_to_expr_pb;
    use ir_common::generated::algebra as pb;
    use ir_physical_client::physical_builder::*;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;

    // g.V().hasLabel("person").both("lower..upper", "knows")
    // result_opt: 0: EndV, 1: AllV, 2: AllVE;  path_opt: 0: Arbitrary, 1: Simple
    fn init_path_expand_request(range: pb::Range, result_opt: i32, path_opt: i32) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(range),
            path_opt,
            result_opt,
            condition: None,
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
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some(edge_expand.into()),
            start_tag: None,
            alias: None,
            hop_range: Some(range),
            path_opt: 0,
            result_opt: 1,
            condition: str_to_expr_pb("@.name == \"marko\"".to_string()).ok(),
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
            meta_data: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
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

    // both(2..3)
    fn path_expand_exactly_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_exactly_request(1);
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
        let request = init_path_expand_range_from_zero_request(1);
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

    fn simple_path_expand_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(pb::Range { lower: 1, upper: 3 }, 1, 1);
        let mut results = submit_query(request, worker_num);
        let mut result_collection: Vec<Vec<ID>> = vec![];
        let mut expected_result_paths =
            vec![vec![1, 2], vec![1, 4], vec![2, 1], vec![4, 1], vec![2, 1, 4], vec![4, 1, 2]];
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
        assert_eq!(result_collection, expected_result_path_ends)
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
        let mut expected_result_paths = vec![vec![2, 1], vec![4, 1], vec![1, 2, 1], vec![1, 4, 1]];
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
        let mut expected_result_paths = vec![vec![1], vec![2, 1], vec![4, 1]];
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
        let mut expected_result_paths =
            vec![vec![1, 4], vec![2, 1], vec![4, 1], vec![1, 4, 1], vec![2, 1, 4], vec![4, 1, 4]];
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
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
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
        assert_eq!(result_collection, expected_result_paths)
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
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
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
        assert_eq!(result_collection, expected_result_paths)
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
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let entry = parse_result(res).unwrap();
                    if let Some(path) = entry.get(None).unwrap().as_graph_path() {
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
        assert_eq!(result_collection, expected_result_paths)
    }

    #[test]
    fn path_expand_exactly_whole_v_e_test() {
        path_expand_exactly_whole_v_e_query(1)
    }

    #[test]
    fn path_expand_exactly_whole_v_e_w2_test() {
        path_expand_exactly_whole_v_e_query(2)
    }
}
