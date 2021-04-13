//
//! Copyright 2020 Alibaba Group Holding Limited.
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

#![feature(test)]

mod common;

extern crate test;
#[macro_use]
extern crate pegasus_config;
extern crate tempdir;
use crate::common::benchmark::{
    initialize, read_pb_request, start_bench_service, submit_query, BenchJobFactory,
    BENCHMARK_PARAM_PATH, BENCHMARK_PLAN_PATH, ID,
};
use gremlin_core::process::traversal::traverser::Requirement;
use pegasus_server::JobRequest;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use test::Bencher;

static JOB_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Copy, Clone, Debug)]
pub enum WhichQuery {
    ThreeHop,
    FourHop,
    CR1,
    CR2,
    CR3,
    CR5,
    CR6,
    CR7,
    CR8,
    CR9,
    CR11,
    CR12,
}

impl WhichQuery {
    fn to_string(&self) -> String {
        match self {
            WhichQuery::ThreeHop => "three_hop_query".to_string(),
            WhichQuery::FourHop => "four_hop_query".to_string(),
            WhichQuery::CR1 => "ldbc_query_1".to_string(),
            WhichQuery::CR2 => "ldbc_query_2".to_string(),
            WhichQuery::CR3 => "ldbc_query_3".to_string(),
            WhichQuery::CR5 => "ldbc_query_5".to_string(),
            WhichQuery::CR6 => "ldbc_query_6".to_string(),
            WhichQuery::CR7 => "ldbc_query_7".to_string(),
            WhichQuery::CR8 => "ldbc_query_8".to_string(),
            WhichQuery::CR9 => "ldbc_query_9".to_string(),
            WhichQuery::CR11 => "ldbc_query_11".to_string(),
            WhichQuery::CR12 => "ldbc_query_12".to_string(),
        }
    }
}

fn incr_request_job_id(job_req: &mut JobRequest) {
    let mut conf = job_req.conf.take().expect("no job_conf");
    conf.job_id = JOB_ID.fetch_add(1, Ordering::SeqCst) as u64;
    job_req.conf = Some(conf);
}

/// Read src ids from given file, if it fails, use default id
fn prepare_src_ids(which: WhichQuery) -> Vec<i64> {
    let param_path = gen_bench_path(which, &*BENCHMARK_PARAM_PATH);
    if let Some(src_ids) = read_src_ids(param_path) {
        src_ids
    } else {
        let src_ids = default_src_ids(which);
        println!("Use default src ids {:?}", src_ids);
        src_ids
    }
}

/// read src ids from file, the src ids should be sperated by '\n'
fn read_src_ids<P: AsRef<Path>>(file_name: P) -> Option<Vec<i64>> {
    if let Ok(string_src_ids) = std::fs::read_to_string(&file_name) {
        let split: Vec<_> = string_src_ids.split('\n').collect();
        let src_ids: Vec<i64> = split.into_iter().map(|s| FromStr::from_str(s).unwrap()).collect();
        Some(src_ids)
    } else {
        None
    }
}

/// default src_ids for queries
fn default_src_ids(which: WhichQuery) -> Vec<i64> {
    match which {
        WhichQuery::ThreeHop => vec![72088380363511554, 72075186223972746],
        WhichQuery::FourHop => vec![72088380363511554, 72075186223972746],
        WhichQuery::CR1 => vec![72088380363511554],
        WhichQuery::CR2 => vec![72075186223972746],
        WhichQuery::CR3 => vec![72075186223983055],
        WhichQuery::CR5 => vec![72079584270488238],
        WhichQuery::CR6 => vec![72088380363511554],
        WhichQuery::CR7 => vec![72075186223981073],
        WhichQuery::CR8 => vec![72075186223972746],
        WhichQuery::CR9 => vec![72070788177470770],
        WhichQuery::CR11 => vec![72088380363511554],
        WhichQuery::CR12 => vec![72075186223972746],
    }
}

/// We prepare query plans by script firstly(which prepares all benchmark queries for only once), and then
/// Read query plans from the up-to-date binary file
fn prepare_pb_request(which: WhichQuery) -> Option<JobRequest> {
    let plan_path = gen_bench_path(which, &*BENCHMARK_PLAN_PATH);
    read_pb_request(plan_path)
}

fn gen_bench_path<P: AsRef<Path>>(which: WhichQuery, dir: P) -> PathBuf {
    dir.as_ref().join(which.to_string())
}

fn bench_queries(b: &mut Bencher, which: WhichQuery, requirement: Requirement) {
    initialize();
    let bench_job_factory = BenchJobFactory::new(
        prepare_src_ids(which).iter().map(|id| *id as ID).collect(),
        requirement,
    );
    let service = start_bench_service(bench_job_factory);
    let pb_request = prepare_pb_request(which).expect("read pb failed");
    b.iter(|| {
        let mut job_req = pb_request.clone();
        incr_request_job_id(&mut job_req);
        submit_query(&service, job_req)
    })
}

#[bench]
//g.V().out("PERSON_KNOWS_PERSON").out("PERSON_KNOWS_PERSON").out("PERSON_KNOWS_PERSON")
fn bench_three_hop(b: &mut Bencher) {
    bench_queries(b, WhichQuery::ThreeHop, Requirement::PATH);
}

#[bench]
fn bench_three_hop_without_path(b: &mut Bencher) {
    bench_queries(b, WhichQuery::ThreeHop, Requirement::OBJECT);
}

#[bench]
//g.V().out("PERSON_KNOWS_PERSON").out("PERSON_KNOWS_PERSON").out("PERSON_KNOWS_PERSON").out("PERSON_KNOWS_PERSON")
fn bench_four_hop(b: &mut Bencher) {
    bench_queries(b, WhichQuery::FourHop, Requirement::PATH);
}

#[bench]
fn bench_four_hop_without_path(b: &mut Bencher) {
    bench_queries(b, WhichQuery::FourHop, Requirement::OBJECT);
}

#[bench]
fn bench_ldbc_1(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR1, Requirement::PATH);
}

#[bench]
fn bench_ldbc_2(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR2, Requirement::LABELED_PATH);
}

#[bench]
#[ignore]
fn bench_ldbc_3(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR3, Requirement::LABELED_PATH);
}

#[bench]
#[ignore]
fn bench_ldbc_5(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR5, Requirement::LABELED_PATH);
}

#[bench]
#[ignore]
fn bench_ldbc_6(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR6, Requirement::LABELED_PATH);
}

#[bench]
fn bench_ldbc_7(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR7, Requirement::LABELED_PATH);
}

#[bench]
fn bench_ldbc_8(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR8, Requirement::LABELED_PATH);
}

#[bench]
fn bench_ldbc_9(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR9, Requirement::LABELED_PATH);
}

#[bench]
fn bench_ldbc_11(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR11, Requirement::LABELED_PATH);
}

#[bench]
fn bench_ldbc_12(b: &mut Bencher) {
    bench_queries(b, WhichQuery::CR12, Requirement::LABELED_PATH);
}
