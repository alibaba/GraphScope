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
    use ir_common::generated::algebra as pb;
    use ir_physical_client::physical_builder::*;
    use pegasus_server::JobRequest;
    use runtime::process::entry::Entry;

    use crate::common::test::*;

    fn gen_sample_by_num_opr(sample_num: i32, seed: Option<i32>) -> pb::Sample {
        let sample_by_num = pb::sample::SampleByNum { num: sample_num };
        pb::Sample {
            sample_type: Some(pb::sample::SampleType {
                inner: Some(pb::sample::sample_type::Inner::SampleByNum(sample_by_num)),
            }),
            seed,
            sample_weight: None,
        }
    }

    fn gen_sample_by_ratio_opr(sample_ratio: f64, seed: Option<i32>) -> pb::Sample {
        let sample_by_ratio = pb::sample::SampleByRatio { ratio: sample_ratio };
        pb::Sample {
            sample_type: Some(pb::sample::SampleType {
                inner: Some(pb::sample::sample_type::Inner::SampleByRatio(sample_by_ratio)),
            }),
            seed,
            sample_weight: None,
        }
    }

    // g.V().sample()
    fn init_scan_sample_request(sample: pb::Sample) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.sample(sample);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    // g.V().out().sample()
    fn init_scan_out_sample_request(sample: pb::Sample) -> JobRequest {
        let source_opr = pb::Scan {
            scan_opt: 0,
            alias: None,
            params: Some(query_params(vec![], vec![], None)),
            idx_predicate: None,
            is_count_only: false,
            meta_data: None,
        };

        let expand_opr = pb::EdgeExpand {
            v_tag: None,
            direction: 0,
            params: Some(query_params(vec![], vec![], None)),
            expand_opt: 0,
            alias: None,
            meta_data: None,
        };

        let mut job_builder = JobBuilder::default();
        job_builder.add_scan_source(source_opr);
        job_builder.edge_expand(expand_opr);
        job_builder.sample(sample);
        job_builder.sink(default_sink_pb());

        job_builder.build().unwrap()
    }

    fn scan_sample_by_num(worker_num: u32, sample_num: i32) -> usize {
        initialize();
        let sample_by_num = gen_sample_by_num_opr(sample_num, None);
        let request = init_scan_sample_request(sample_by_num);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        print!("{:?}", result_collection);
        result_collection.len()
    }

    fn scan_out_sample_by_num(sample_num: i32) -> usize {
        initialize();
        let sample_by_num = gen_sample_by_num_opr(sample_num, None);
        let request = init_scan_out_sample_request(sample_by_num);
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        print!("{:?}", result_collection);
        result_collection.len()
    }

    fn scan_sample_by_num_with_seed(sample_num: i32, seed: i32) -> Vec<i64> {
        initialize();
        let sample_by_num = gen_sample_by_num_opr(sample_num, Some(seed));
        let request = init_scan_sample_request(sample_by_num);
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        print!("{:?}", result_collection);
        result_collection
    }

    fn scan_sample_by_ratio(worker_num: u32, sample_ratio: f64) -> usize {
        initialize();
        let sample_by_ratio = gen_sample_by_ratio_opr(sample_ratio, None);
        let request = init_scan_sample_request(sample_by_ratio);
        let mut results = submit_query(request, worker_num);
        let mut result_collection = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        print!("{:?}", result_collection);
        result_collection.len()
    }

    fn scan_out_sample_by_ratio(sample_ratio: f64) -> usize {
        initialize();
        let sample_by_ratio = gen_sample_by_ratio_opr(sample_ratio, None);
        let request = init_scan_out_sample_request(sample_by_ratio);
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        print!("{:?}", result_collection);
        result_collection.len()
    }

    fn scan_sample_by_ratio_with_seed(sample_ratio: f64, seed: i32) -> Vec<i64> {
        initialize();
        let sample_by_ratio = gen_sample_by_ratio_opr(sample_ratio, Some(seed));
        let request = init_scan_sample_request(sample_by_ratio);
        let mut results = submit_query(request, 2);
        let mut result_collection = vec![];
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    let record = parse_result(res).unwrap();
                    if let Some(vertex) = record.get(None).unwrap().as_vertex() {
                        result_collection.push(vertex.id());
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        print!("{:?}", result_collection);
        result_collection
    }

    // g.V().sample(2) with worker_num = 1
    #[test]
    fn scan_sample_by_num_test() {
        let res_num = scan_sample_by_num(1, 2);
        assert_eq!(res_num, 2);
    }

    // g.V().sample(2) with worker_num = 2
    #[test]
    fn scan_sample_by_num_w2_test() {
        let res_num = scan_sample_by_num(2, 2);
        assert_eq!(res_num, 2);
    }

    // g.V().sample(10) with worker_num = 1
    #[test]
    fn scan_sample_by_num_test_02() {
        let res_num = scan_sample_by_num(1, 10);
        // no upsampling
        assert_eq!(res_num, 6);
    }

    // g.V().sample(10) with worker_num = 2
    #[test]
    fn scan_sample_by_num_w2_test_02() {
        let res_num = scan_sample_by_num(2, 10);
        // no upsampling
        assert_eq!(res_num, 6);
    }

    // g.V().out().sample(2)
    #[test]
    fn scan_out_sample_by_num_test() {
        let res_num = scan_out_sample_by_num(2);
        assert_eq!(res_num, 2);
    }

    // g.V().out().sample(10)
    #[test]
    fn scan_out_sample_by_num_test_02() {
        let res_num = scan_out_sample_by_num(10);
        // no upsampling
        assert_eq!(res_num, 6);
    }

    // g.V().sample(2).with("REPEATABLE", 97)
    #[test]
    fn scan_sample_by_num_with_seed_test() {
        let mut first_sample = scan_sample_by_num_with_seed(2, 97);
        first_sample.sort();
        for _i in 0..3 {
            let mut try_sample = scan_sample_by_num_with_seed(2, 97);
            try_sample.sort();
            assert_eq!(first_sample, try_sample);
        }
    }

    // g.V().coin(1.0) with worker_num = 1
    #[test]
    fn scan_sample_by_coin_test() {
        let res_num = scan_sample_by_ratio(1, 1.0);
        assert_eq!(res_num, 6);
    }

    // g.V().coin(1.0) with worker_num = 2
    #[test]
    fn scan_sample_by_coin_w2_test() {
        let res_num = scan_sample_by_ratio(2, 1.0);
        assert_eq!(res_num, 6);
    }

    // g.V().coin(0.1) with worker_num = 1
    #[test]
    fn scan_sample_by_coin_01_test() {
        let res_num = scan_sample_by_ratio(1, 0.1);
        assert!(res_num < 6);
    }

    // g.V().coin(0.1) with worker_num = 2
    #[test]
    fn scan_sample_by_coin_01_w2_test() {
        let res_num = scan_sample_by_ratio(2, 0.1);
        assert!(res_num < 6);
    }

    // g.V().out().coin(1.0)
    #[test]
    fn scan_out_sample_by_coin_test() {
        let res_num = scan_out_sample_by_ratio(1.0);
        assert_eq!(res_num, 6);
    }

    // g.V().out().coin(0.1)
    #[test]
    fn scan_out_sample_by_coin_01_test() {
        let res_num = scan_out_sample_by_ratio(0.1);
        assert!(res_num < 6);
    }

    // g.V().coin(0.8).with("REPEATABLE", 97)
    #[test]
    fn scan_sample_by_coin_with_seed_test() {
        let mut first_sample = scan_sample_by_ratio_with_seed(0.8, 97);
        first_sample.sort();
        for _i in 0..3 {
            let mut try_sample = scan_sample_by_ratio_with_seed(0.8, 97);
            try_sample.sort();
            assert_eq!(first_sample, try_sample);
        }
    }
}
