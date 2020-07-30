//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use super::*;
use crate::operator::advanced::map::Map;
use crate::operator::advanced::inspect::Inspect;
use crate::operator::advanced::aggregate::Count;


fn run_simple(workers: usize, threads: usize) {
    run_local(workers, threads, |worker| {
        worker.dataflow("test_executor_1", |builder| {
            (0..4).into_stream(builder)
                .flat_map(|record| Some(vec![record;2]))
                .inspect(|record| info!("get record {} ", record))
                .count(Pipeline)
                .inspect(|count| {
                    info!("TestExecutor_1: Count {}", count);
                    assert_eq!(8, *count);
                });
            Ok(())
        }).unwrap();
    }).unwrap();

}

#[test]
fn test_executor_1_2() {
    try_init_logger().ok();
    run_simple(1, 2);
}

#[test]
fn test_executor_2_2() {
    try_init_logger().ok();
    run_simple(2, 2);
}

#[test]
fn test_executor_1_3() {
    try_init_logger().ok();
    run_simple(1, 3);
}

#[test]
fn test_executor_2_3() {
    try_init_logger().ok();
    run_simple(2, 3);
}
