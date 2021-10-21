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

use std::collections::HashSet;
use super::*;
use crate::operator::lazy::unary::LazyUnary;
use crate::operator::lazy::binary::LazyBinary;
use crate::operator::unary::Unary;
use crate::operator::branch::Branch;
use crate::channel::output::OutputHandle;

#[test]
fn test_lazy_unary() {
    run_local(1, 0, |worker| {
        worker.dataflow("test_lazy_unary", |builder| {
            (0..3u32).into_stream(builder)
                .lazy_unary("unary", Pipeline, |_info| {
                    |data: u32| { Some(vec![data;16]) }
                })
                .lazy_unary_state("count", Pipeline, |_info| {
                    (
                        |data: u32, count: &mut usize|  {
                            // println!("get data {:?}", data);
                            *count += 1;
                            Some(::std::iter::once(data))
                        },
                        |count: usize| {
                            println!("### [Test Lazy Unary] final count: {}", count);
                            assert_eq!(count, 48);
                            vec![0]
                        }
                    )
                });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_lazy_binary() {
    run_local(1, 0 , |worker| {
        worker.dataflow("test_lazy_binary", |builder| {
            let stream_1 = (0..32u32).into_stream(builder);
            let stream_2 = (32..64u32).into_stream(builder);
            stream_1.lazy_binary("merge", &stream_2, Pipeline, Pipeline, |_| {
                (
                    |item1: u32| { Some(vec![item1; 1024].into_iter()) },
                    |item2: u32| { Some(vec![item2; 1024].into_iter()) }
                )
            }).lazy_unary_state("dedup", Pipeline, |_| {
                (
                    |data: u32, state: &mut (HashSet<u32>, usize)| {
                        {
                            let count = &mut state.1;
                            *count += 1;
                        }
                        if (&mut state.0).insert(data) {
                            let output = data as usize;
                            Some(vec![output])
                        } else { None }
                    },
                    |(_, count): (HashSet<u32>, usize)| {
                        assert_eq!(count, 64 * 1024);
                        vec![]
                    }
                )
            }).unary_state("count", Pipeline, |_| {
                (
                    |input, _: &mut OutputHandle<()>, states| {
                        input.for_each_batch(|dataset| {
                            let (t, d) = dataset.take();
                            let count = states.entry(t).or_insert(0usize);
                            *count += d.len();
                            Ok(false)
                        })?;
                        Ok(())
                    },
                    |_: &mut OutputHandle<()>, states| {
                        for n in states {
                            assert_eq!(n.1, 64);
                        }
                        Ok(())
                    }
                )
            });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

#[test]
fn test_lazy_binary_state() {
    try_init_logger().ok();
    run_local(1, 0 , |worker| {
        worker.dataflow("test_lazy_binary_state", |builder| {
            let condition = |r: &u32| *r < 10;
            let (s1, s2) = (0..32u32).into_stream(builder)
                .branch("branch", condition);
            s1.lazy_binary_state("lazy_merge", &s2, Pipeline, Pipeline, |_| {
                (
                    |r: u32, s: &mut usize| {
                       *s += 1;
                        Some(vec![r; 2048])
                    },
                    |r: u32, s: &mut usize| {
                        *s += 1;
                        Some(vec![r; 128])
                    },
                    |s: usize| {
                        assert_eq!(s, 32);
                        vec![s as u32]
                    }
                )
            });
            Ok(())
        }).expect("build plan failure");
    }).unwrap();
}

