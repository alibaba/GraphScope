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
use crate::operator::advanced::exchange::Exchange;
use crate::operator::advanced::aggregate::Count;
use crate::operator::advanced::inspect::Inspect;
use crate::stream::DataflowBuilder;

#[test]
fn test_inter_process_exchange_1() {
    try_init_logger().ok();
    run_local(2, 0, |worker| {
        worker.dataflow("test_exchange_1", |builder| {
            (0..10000u64).into_stream(builder)
                .exchange(|i| *i)
                .count(Pipeline)
                .inspect(|i| {
                    assert_eq!(*i, 10000);
                });
            Ok(())
        }).expect("build failure");
    }).unwrap();
}

#[test]
fn test_inter_process_exchange_2() {
    try_init_logger().ok();

    run_local(3, 0, |worker| {
        worker.dataflow("test_exchange_2", |builder| {
            let idx = builder.worker_id().1 as u64;
            let mut count = 0u64 ;
            (0..6000u64).into_stream(builder)
                .exchange(move |_i| {
                    count += 1;
                    if count <= 3000 { 0 }
                    else if count > 3000 && count <= 5000 { 1 }
                    else { 2 }
                })
                .count(Pipeline)
                .inspect(move |i| {
                    let expect = 3000 * (3 - idx) as usize;
                    assert_eq!(*i, expect);
                });
            Ok(())
        }).expect("build failure");
    }).unwrap();
}
