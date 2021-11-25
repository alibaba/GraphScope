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

use pegasus::api::{Binary, Branch, Sink};
use pegasus::JobConf;

#[test]
fn branch_test() {
    let mut conf = JobConf::new("branch_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let stream =
                if index == 0 { input.input_from(0..1000u32) } else { input.input_from(1000..2000u32) }?;

            let (left, right) = stream
                .repartition(|item: &u32| Ok(*item as u64))
                .branch("split", |_info| {
                    |input, left, right| {
                        input.for_each_batch(|dataset| {
                            let mut left_sess = left.new_session(&dataset.tag)?;
                            let mut right_sess = right.new_session(&dataset.tag)?;
                            for item in dataset.drain() {
                                if item >= 555 {
                                    left_sess.give(item)?;
                                } else {
                                    right_sess.give(item)?;
                                }
                            }
                            Ok(())
                        })
                    }
                })?;

            left.binary("union", right, |_info| {
                |left, right, output| {
                    left.for_each_batch(|dataset| {
                        let mut session = output.new_session(&dataset.tag)?;
                        for d in dataset.drain() {
                            session.give((0, d))?;
                        }
                        Ok(())
                    })?;

                    right.for_each_batch(|dataset| {
                        let mut session = output.new_session(&dataset.tag)?;
                        for d in dataset.drain() {
                            session.give((1, d))?;
                        }
                        Ok(())
                    })
                }
            })?
            .sink_into(output)
        }
    })
    .expect("submit job failure;");

    let mut count = vec![0, 0];
    while let Some(Ok((flag, data))) = result.next() {
        if flag == 0 {
            assert!(data >= 555);
            count[0] += 1;
        } else {
            assert!(data < 555);
            count[1] += 1;
        }
    }

    assert_eq!(count[0], 2000 - 555);
    assert_eq!(count[1], 555);
}
