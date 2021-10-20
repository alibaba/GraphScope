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
use crate::operator::advanced::iterate::Iteration;
use crate::operator::advanced::map::Map;
use crate::operator::advanced::inspect::Inspect;
use crate::operator::advanced::aggregate::Count;
use crate::operator::advanced::exchange::Exchange;
use crate::communication::Aggregate;
use crate::worker::DefaultStrategy;

#[test]
fn test_simple_loop_1() {
    try_init_logger().ok();
    run_local(1, 0, |worker| {
        worker.dataflow("test_simple_loop_1", |builder| {
            (1..2).into_stream(builder)
                .iterate(3, |start| {
                    start.flat_map(|item| Some(item..item + 2) )
                })
                .count(Pipeline)
                .inspect(|count| {
                   assert_eq!(*count, 8)
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_simple_loop_2() {
    try_init_logger().ok();
    run_local(2, 0, |worker| {
        worker.dataflow("test_simple_loop_2", |builder| {
            (1..2).into_stream(builder)
                .iterate(3, |start| {
                    start.flat_map(|item| Some(item..item + 2))
                        .exchange(|r| *r)
                })
                .count(Pipeline)
                .inspect(|count| {
                    assert_eq!(*count, 8)
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_simple_loop_3() {
    try_init_logger().ok();
    run_local(3, 0, |worker| {
        worker.dataflow("test_simple_loop_3", |builder| {
            let converge = |r: &u64| *r >= 5;
            (1..2).into_stream(builder)
                .iterate_until(65536, converge,  |start| {
                    start.flat_map(|item| Some(item + 1..item + 3))
                        .exchange(|r| *r)
                })
                .inspect(|r| {
                    println!("get record {}", r);
                    debug_assert!(*r >= 5);
                    debug_assert!(*r <= 6);
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_simple_loop_4() {
    try_init_logger().ok();
    run_local(2, 0, |worker| {
        worker.dataflow("test_simple_loop_4", |builder| {
            (1..2).into_stream(builder)
                .iterate(65536, |start| {
                    start.flat_map(|item| {
                        if item >= 5 {
                            None
                        } else {
                            Some(vec![item + 1;2])
                        }
                    }).exchange(|r| *r)
                })
                .count(Pipeline)
                .inspect(|count| {
                    assert_eq!(*count, 0)
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_simple_loop_5() {
    try_init_logger().ok();
    run_local(3, 0, |worker| {
        let index = worker.id.1;
        worker.dataflow("test_simple_loop_5", move |builder| {
            let source = if index == 0 {
                Some(1)
            } else { None };
            source.into_stream(builder)
                .iterate(4, |start| {
                    start.flat_map(|item| Some(item..item + 2))
                        .exchange(|r| *r)
                })
                .count(Aggregate::new(0))
                .inspect(move |count| {
                    if index == 0 {
                        info!("get count {}", count);
                        assert_eq!(*count, 16)
                    }
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_simple_loop_6() {
    try_init_logger().ok();
    run_local(2, 0, |worker| {
        let index = worker.id.1;
        worker.set_schedule_strategy(DefaultStrategy::with_capacity(256));
        worker.dataflow_opt("test_simple_loop_6", 128, true, move |builder| {
            let source = if index == 0 {
                Some(1)
            } else { None };
            source.into_stream(builder)
                .iterate(3, |start| {
                    start.flat_map(|item| Some(item..item + 16))
                        .exchange(|r| *r)
                })
                .count(Aggregate::new(0))
                .inspect(move |count| {
                    if index == 0 {
                        info!("get count {}", count);
                        assert_eq!(*count, 16 * 16 * 16)
                    }
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}
