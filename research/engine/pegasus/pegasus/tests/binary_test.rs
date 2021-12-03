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

use pegasus::api::{Binary, Map, Sink};
use pegasus::JobConf;

/// test binary merge pipeline;
#[test]
fn binary_test_01() {
    let conf = JobConf::new("binary_test_01");
    let mut result = pegasus::run(conf, || {
        |input, output| {
            let stream = input.input_from(vec![1u32; 1000])?;
            let (left, right) = stream.copied()?;
            let left = left.map(|item| Ok(item + 1))?;
            let right = right.map(|item| Ok(item + 2))?;
            left.binary("merge", right, |_info| {
                |left, right, output| {
                    left.for_each_batch(|dataset| {
                        let mut session = output.new_session(&dataset.tag)?;
                        for item in dataset.drain() {
                            session.give(item)?;
                        }
                        Ok(())
                    })?;

                    right.for_each_batch(|dataset| {
                        let mut session = output.new_session(&dataset.tag)?;
                        for item in dataset.drain() {
                            session.give(item)?;
                        }
                        Ok(())
                    })
                }
            })?
            .sink_into(output)
        }
    })
    .expect("submit job failure;");

    let mut count = 0;
    while let Some(Ok(data)) = result.next() {
        assert!(data == 2 || data == 3);
        count += 1;
    }

    assert_eq!(2000, count);
}

/// test binary merge with shuffle between two workers;
#[test]
fn binary_test_02() {
    let mut conf = JobConf::new("binary_test_02");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        |input, output| {
            let stream = input.input_from(0..1000)?;
            let (left, right) = stream.copied()?;
            let left = left.map(|item| Ok(item + 1))?;
            let right = right
                .map(|item| Ok(item + 2))?
                .repartition(|item: &u32| Ok(*item as u64));
            left.binary("merge", right, |_info| {
                |left, right, output| {
                    left.for_each_batch(|dataset| {
                        let mut session = output.new_session(&dataset.tag)?;
                        for item in dataset.drain() {
                            session.give(item)?;
                        }
                        Ok(())
                    })?;

                    right.for_each_batch(|dataset| {
                        let mut session = output.new_session(&dataset.tag)?;
                        for item in dataset.drain() {
                            session.give(item)?;
                        }
                        Ok(())
                    })
                }
            })?
            .sink_into(output)
        }
    })
    .expect("submit job failure;");

    let mut count = 0;
    while let Some(Ok(data)) = result.next() {
        assert!(data > 0 && data < 1002);
        count += 1;
    }

    assert_eq!(4000, count);
}
