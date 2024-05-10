//
//! Copyright 2023 Alibaba Group Holding Limited.
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

#![feature(test)]

mod common;

extern crate test;

use ir_common::generated::algebra as algebra_pb;
use ir_common::generated::common as common_pb;
use ir_common::KeyId;
use ir_physical_client::physical_builder::*;
use pegasus_server::JobRequest;
use test::Bencher;

use crate::common::benchmark::{
    default_sink_target, incr_request_job_id, initialize, parse_result, query_params, submit_query,
    KNOWS_LABEL, TAG_A, TAG_B, TAG_C, TAG_D,
};

// (A) -knows-> (B);  (A) -knows-> (C); (B) <-knows (C)
fn init_intersect_job_request(edge_tag_1: Option<KeyId>, edge_tag_2: Option<KeyId>) -> JobRequest {
    //  (A)
    let source_opr = algebra_pb::Scan {
        scan_opt: 0,
        alias: Some(TAG_A.into()),
        params: None,
        idx_predicate: None,
        is_count_only: false,
        meta_data: None,
    };

    // (A) -> (B);
    let expand_opr1 = algebra_pb::EdgeExpand {
        v_tag: Some(TAG_A.into()),
        direction: 0, // out
        params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
        expand_opt: 0,
        alias: Some(TAG_B.into()),
        meta_data: None,
        is_optional: false,
    };

    let expand_opr2;
    let mut get_v_opr1 = None;

    if let Some(edge_tag_1) = edge_tag_1 {
        // a seperate expande + getv
        // (A) -> (C); with edge tag edge_tag_1
        expand_opr2 = algebra_pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 1, // expand edge
            alias: Some(edge_tag_1.into()),
            meta_data: None,
            is_optional: false,
        };

        get_v_opr1 = Some(algebra_pb::GetV {
            tag: None,
            opt: 1, // EndV
            params: Some(query_params(vec![], vec![], None)),
            alias: Some(TAG_C.into()),
            meta_data: None,
        });
    } else {
        // a fused expandv
        expand_opr2 = algebra_pb::EdgeExpand {
            v_tag: Some(TAG_A.into()),
            direction: 0, // out
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0, // expand vertex
            alias: Some(TAG_C.into()),
            meta_data: None,
            is_optional: false,
        };
    }

    let expand_opr3;
    let mut get_v_opr2 = None;

    if let Some(edge_tag_2) = edge_tag_2 {
        // a seperate expande + getv
        // (B) <- (C); with edge tag edge_tag_2
        expand_opr3 = algebra_pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 1, // in
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 1, // expand edge
            alias: Some(edge_tag_2.into()),
            meta_data: None,
            is_optional: false,
        };

        get_v_opr2 = Some(algebra_pb::GetV {
            tag: None,
            opt: 0, // StartV
            params: Some(query_params(vec![], vec![], None)),
            alias: Some(TAG_C.into()),
            meta_data: None,
        });
    } else {
        // a fused expandv
        expand_opr3 = algebra_pb::EdgeExpand {
            v_tag: Some(TAG_B.into()),
            direction: 1, // in
            params: Some(query_params(vec![KNOWS_LABEL.into()], vec![], None)),
            expand_opt: 0, // expand vertex
            alias: Some(TAG_C.into()),
            meta_data: None,
            is_optional: false,
        };
    }

    let mut sink_tags: Vec<_> = vec![TAG_A, TAG_B, TAG_C]
        .into_iter()
        .map(|i| common_pb::NameOrIdKey { key: Some(i.into()) })
        .collect();
    if let Some(edge_tag_1) = edge_tag_1 {
        sink_tags.push(common_pb::NameOrIdKey { key: Some(edge_tag_1.into()) });
    }
    if let Some(edge_tag_2) = edge_tag_2 {
        sink_tags.push(common_pb::NameOrIdKey { key: Some(edge_tag_2.into()) });
    }
    let sink_pb = algebra_pb::Sink { tags: sink_tags, sink_target: default_sink_target() };

    let mut job_builder = JobBuilder::default();
    job_builder.add_scan_source(source_opr.clone());
    job_builder.shuffle(None);
    job_builder.edge_expand(expand_opr1.clone());
    let mut plan_builder_1 = PlanBuilder::new(1);
    plan_builder_1.shuffle(None);
    plan_builder_1.edge_expand(expand_opr2);
    if let Some(get_v_opr1) = get_v_opr1 {
        plan_builder_1.get_v(get_v_opr1);
    }
    let mut plan_builder_2 = PlanBuilder::new(2);
    plan_builder_2.shuffle(None);
    plan_builder_2.edge_expand(expand_opr3.clone());
    if let Some(get_v_opr2) = get_v_opr2 {
        plan_builder_2.get_v(get_v_opr2);
    }
    job_builder.intersect(vec![plan_builder_1, plan_builder_2], TAG_C.into());

    job_builder.sink(sink_pb);
    job_builder.build().unwrap()
}

fn bench_request(b: &mut Bencher, pb_request: JobRequest, print_flag: bool) {
    initialize();
    b.iter(|| {
        let mut job_req = pb_request.clone();
        let job_id = incr_request_job_id(&mut job_req);
        let mut results = submit_query(job_req, 32);
        let mut res_count: i32 = 0;
        while let Some(result) = results.next() {
            match result {
                Ok(res) => {
                    res_count += 1;
                    if print_flag && job_id == 1 {
                        if let Some(result) = parse_result(res) {
                            println!("result {:?}", result)
                        }
                    }
                }
                Err(e) => {
                    panic!("err result {:?}", e);
                }
            }
        }
        println!("result count: {:?}", res_count)
    })
}

#[bench]
fn bench_intersect_01(b: &mut Bencher) {
    // no edge tags, i.e., the optimized intersection
    let request = init_intersect_job_request(None, None);
    bench_request(b, request, true)
}

#[bench]
fn bench_intersect_02(b: &mut Bencher) {
    let request = init_intersect_job_request(Some(TAG_D), None);
    bench_request(b, request, true)
}

#[bench]
fn bench_intersect_03(b: &mut Bencher) {
    let request = init_intersect_job_request(None, Some(TAG_D));
    bench_request(b, request, true)
}

#[bench]
fn bench_intersect_04(b: &mut Bencher) {
    let request = init_intersect_job_request(Some(TAG_D), Some(TAG_D));
    bench_request(b, request, true)
}
