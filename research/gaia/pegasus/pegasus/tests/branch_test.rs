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

use pegasus::api::{Branch, Exchange, IntoBranch, ResultSet, Sink};
use pegasus::{Configuration, JobConf};

#[test]
fn branch_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("branch_test");
    conf.set_workers(2);

    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let index = worker.id.index;
        let tx = tx.clone();
        worker.dataflow(move |builder| {
            let source = if index == 0 {
                builder.input_from_iter(0..1000u32)
            } else {
                builder.input_from_iter(1000..2000u32)
            }?;

            let (left, right) = source.exchange_with_fn(|item: &u32| *item as u64)?.branch(
                "split",
                |item: &u32| {
                    if *item >= 555 {
                        Branch::Left
                    } else {
                        Branch::Right
                    }
                },
            )?;

            let tx_left = tx.clone();
            left.sink_by(|_| {
                move |_, result| match result {
                    ResultSet::Data(data) => {
                        tx_left.send((0, data)).unwrap();
                    }
                    _ => (),
                }
            })?;

            right.sink_by(|_| {
                move |_, result| match result {
                    ResultSet::Data(data) => {
                        tx.send((1, data)).unwrap();
                    }
                    _ => (),
                }
            })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);

    let mut count = vec![0, 0];
    while let Ok((flag, data)) = rx.recv() {
        if flag == 0 {
            data.iter().for_each(|item| assert!(*item >= 555));
            count[0] += data.len();
        } else {
            data.iter().for_each(|item| assert!(*item < 555));
            count[1] += data.len();
        }
    }

    assert_eq!(count[0], 2000 - 555);
    assert_eq!(count[1], 555);
    pegasus::shutdown_all();
}
