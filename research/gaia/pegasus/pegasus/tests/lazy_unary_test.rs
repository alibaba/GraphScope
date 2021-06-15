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

use pegasus::api::function::*;
use pegasus::api::{Exchange, LazyUnary, ResultSet, Sink};
use pegasus::communication::Pipeline;
use pegasus::flat_map;
use pegasus::{Configuration, JobConf};

#[test]
fn lazy_test_01() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let conf = JobConf::new("lazy_unary_01");
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            builder
                .input_from_iter(0..1000u32)?
                .lazy_unary("flatmap", Pipeline, |_| {
                    flat_map!(|item: u32| {
                        let mut result = vec![];
                        if item != 0 {
                            let size = item as usize;
                            result = vec![item; size];
                        }
                        let iter = result.into_iter().map(|i| Ok(i));
                        Ok(iter)
                    })
                })?
                .sink_by(|_| {
                    move |_, result| match result {
                        ResultSet::Data(data) => tx.send(data).unwrap(),
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    ::std::mem::drop(tx);
    let mut count = 0;
    while let Ok(data) = rx.recv() {
        count += data.len();
    }

    println!("total {} results", count);
    let excepted = 1000 * 998 / 2 + 500usize;
    assert_eq!(count, excepted);
    pegasus::shutdown_all();
}

#[test]
fn lazy_test_02() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("lazy_unary_01");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let index = worker.id.index;
        let tx = tx.clone();
        worker.dataflow(move |builder| {
            if index == 0 {
                builder.input_from_iter(0..500u32)
            } else {
                builder.input_from_iter(500..1000u32)
            }?
            .exchange_with_fn(|item: &u32| *item as u64)?
            .lazy_unary("flatmap", Pipeline, |_meta| {
                flat_map!(|item: u32| {
                    let mut result = vec![];
                    if item != 0 {
                        let size = item as usize;
                        result = vec![item; size];
                    }
                    Ok(result.into_iter().map(|item| Ok(item)))
                })
            })?
            .sink_by(|_| {
                move |_, result| match result {
                    ResultSet::Data(data) => tx.send(data).unwrap(),
                    _ => (),
                }
            })?;
            Ok(())
        })
    })
    .expect("submit job failure");

    std::mem::drop(tx);
    let mut count = 0;
    while let Ok(data) = rx.recv() {
        count += data.len();
    }

    println!("total {} results", count);
    let excepted = 1000 * 998 / 2 + 500usize;
    assert_eq!(count, excepted);
    pegasus::shutdown_all();
}
