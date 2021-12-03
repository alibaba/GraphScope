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

use pegasus::api::{Sink, Unary};
use pegasus::JobConf;

/// Test unary pipeline;
#[test]
fn unary_test_01() {
    let conf = JobConf::new("unary_test_01");
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(vec![0; 1024])?
                .unary("unary", |_info| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            for item in dataset.drain() {
                                session.give(item + 1)?;
                            }
                            Ok(())
                        })
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut count = 0;
    while let Some(Ok(data)) = result.next() {
        assert_eq!(data, 1);
        count += 1;
    }
    assert_eq!(1024, count);
}

/// test unary with shuffle between two worker;
#[test]
fn unary_test_02() {
    let mut conf = JobConf::new("unary_test_02");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..1024u32)?
                .repartition(|x: &u32| Ok(*x as u64))
                .unary("unary", |_info| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            for i in dataset.drain() {
                                session.give(i + 1)?;
                            }
                            Ok(())
                        })
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut count = 0;
    while let Some(Ok(_)) = result.next() {
        count += 1;
    }
    assert_eq!(2048, count);
}

/// test unary with shuffle between more workers;
#[test]
fn unary_test_03() {
    let mut conf = JobConf::new("unary_test_03");
    conf.set_workers(8);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            input
                .input_from(0..1024u32)?
                .repartition(|x: &u32| Ok(*x as u64))
                .unary("unary", |_info| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            let mut session = output.new_session(&dataset.tag)?;
                            for i in dataset.drain() {
                                session.give(i + 1)?;
                            }
                            Ok(())
                        })
                    }
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure:");

    let mut count = vec![0; 1024];
    while let Some(Ok(d)) = result.next() {
        let i = (d - 1) as usize;
        count[i] += 1;
    }
    assert_eq!(count, vec![8; 1024]);
}
