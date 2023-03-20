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
    use ir_physical_client::physical_builder::*;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;
    use ir_common::expr_parse::str_to_expr_pb;

    // g.V().hasLabel("person").both("1..3", "knows")
    // result_opt: 0: EndV, 1: AllV;  path_opt: 0: Arbitrary, 1: Simple
    fn init_path_expand_request(result_opt: i32, path_opt: i32) -> JobRequest {
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
            hop_range: Some(pb::Range { lower: 1, upper: 3 }),
            path_opt,
            result_opt,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("2..3", "knows")
    fn init_path_expand_exactly_request(is_whole_path: bool) -> JobRequest {
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
            hop_range: Some(pb::Range { lower: 2, upper: 3 }),
            path_opt: 0,
            result_opt: if is_whole_path { 1 } else { 0 },
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("0..3", "knows")
    fn init_path_expand_range_from_zero_request(is_whole_path: bool) -> JobRequest {
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
            hop_range: Some(pb::Range { lower: 0, upper: 3 }),
            path_opt: 0,
            result_opt: if is_whole_path { 1 } else { 0 },
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().hasLabel("person").both("1..3", "knows") with vertex's filter "@.age>28"
    fn init_path_expand_with_filter_request(is_whole_path: bool) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![PERSON_LABEL.into()], vec![], None)),
            idx_predicate: None,
        };

        let edge_expand = pb::EdgeExpand {
            v_tag: None,
            direction: 2,
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0,
            alias: None,
        };

        let getv = pb::GetV {
            tag: None,
            opt: 4,
            params: Some(query_params(vec![], vec![], str_to_expr_pb("@.age >28".to_string()).ok())),
            alias: None,
        };

        let path_expand_opr = pb::PathExpand {
            base: Some((edge_expand, getv).into()),
            start_tag: None,
            alias: None,
            hop_range: Some(pb::Range { lower: 1, upper: 3 }),
            path_opt: 0,
            result_opt: if is_whole_path { 1 } else { 0 },
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.shuffle(None);
        job_builder.path_expand(path_expand_opr);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    fn path_expand_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(1, 0);
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
        let request = init_path_expand_request(0, 0);
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

    fn simple_path_expand_whole_query(worker_num: u32) {
        initialize();
        let request = init_path_expand_request(1, 1);
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
        let request = init_path_expand_request(0, 1);
        let mut results = submit_query(request, num_workers);
        let mut result_collection = vec![];
        let mut expected_result_path_ends = vec![2, 4, 1, 1, 4, 2];
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
}
