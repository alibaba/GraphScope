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
use pegasus::api::{
    complete, Exchange, Filter, Iteration, Limit, LoopCondition, Map, Multiplexing,
    NonBlockReceiver, Range, ResultSet, Sink,
};
use pegasus::communication::Pipeline;
use pegasus::filter;
use pegasus::{Configuration, JobConf};

#[test]
fn ping_pong_test_01() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("ping_pong_test_01");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        let index = worker.id.index;
        worker.dataflow(move |builder| {
            let source = if index == 0 {
                builder.input_from_iter(0..500u32)
            } else {
                builder.input_from_iter(500..1000u32)
            }?;
            source
                .iterate(10, |start| {
                    start
                        .exchange_with_fn(|item: &u32| *item as u64)?
                        .map_with_fn(Pipeline, |item| Ok(item + 1))
                })?
                .sink_by(|_| {
                    move |_, result| {
                        if let ResultSet::Data(data) = result {
                            tx.send(data).unwrap();
                        }
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure");

    std::mem::drop(tx);
    let mut count = 0;
    let mut sum = 0;
    while let Ok(data) = rx.recv() {
        count += data.len();
        for x in data {
            sum += x;
        }
    }
    guard.unwrap().join().expect("run job failure;");
    assert_eq!(count, 1000);
    assert_eq!(sum, 999 * 500 + 1000 * 10);
    pegasus::shutdown_all();
}

#[test]
fn ping_pong_test_02() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("ping_pong_test_02");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        let index = worker.id.index;
        worker.dataflow(move |builder| {
            let source = if index == 0 {
                builder.input_from_iter(0..500u32)
            } else {
                builder.input_from_iter(500..1000u32)
            }?
            .map_with_fn(Pipeline, |item| Ok((0u32, item)))?;
            let mut condition = LoopCondition::new();
            condition.until(Box::new(filter!(|item: &(u32, u32)| Ok(item.0 >= 10))));
            source
                .iterate_until(condition, |start| {
                    start
                        .exchange_with_fn(|item: &(u32, u32)| item.1 as u64)?
                        .map_with_fn(Pipeline, |(index, item)| Ok((index + 1, item + 1)))
                })?
                .sink_by(|_| {
                    move |_, result| {
                        if let ResultSet::Data(data) = result {
                            tx.send(data).unwrap();
                        }
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure");

    std::mem::drop(tx);
    let mut count = 0;
    let mut sum = 0;
    while let Ok(data) = rx.recv() {
        count += data.len();
        for (x, y) in data {
            sum += y;
            assert_eq!(x, 10);
        }
    }
    assert_eq!(count, 1000);
    assert_eq!(sum, 999 * 500 + 1000 * 10);
    pegasus::shutdown_all();
}

#[test]
fn ping_pong_test_03() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("ping_pong_test_03");
    conf.set_workers(2);
    let (input_tx, input_rx) = crossbeam_channel::unbounded();
    let (output_tx, output_rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let index = worker.id.index;
        let rx = input_rx.clone();
        let tx = output_tx.clone();
        worker.dataflow(move |builder| {
            let source = if index == 0 {
                let rx = NonBlockReceiver::new(rx);
                builder.input_from(rx)
            } else {
                builder.input_from_iter(Vec::<Option<u32>>::new().into_iter())
            }?;

            let mut incr_id = 0;
            let multiplex = source
                .scope_by(move |data| {
                    if data.is_some() {
                        Some(incr_id)
                    } else {
                        complete(incr_id);
                        incr_id += 1;
                        None
                    }
                })?
                .map_with_fn(Pipeline, |data| Ok(data.unwrap()))?;

            multiplex
                .iterate(10, |start| {
                    start
                        .exchange_with_fn(|item: &u32| *item as u64)?
                        .map_with_fn(Pipeline, |item| Ok(item + 1))
                })?
                .sink_by(|_| {
                    move |t, result| {
                        let t = t.current_uncheck();
                        if let ResultSet::Data(data) = result {
                            tx.send((t, data)).unwrap();
                        }
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    vec![0u32; 1023].into_iter().for_each(|i| input_tx.send(Some(i)).unwrap());
    input_tx.send(None).unwrap();
    vec![1u32; 1024].into_iter().for_each(|i| input_tx.send(Some(i)).unwrap());
    input_tx.send(None).unwrap();
    vec![2u32; 1025].into_iter().for_each(|i| input_tx.send(Some(i)).unwrap());
    input_tx.send(None).unwrap();

    std::mem::drop(input_tx);
    std::mem::drop(output_tx);

    let mut count = vec![0, 0, 0];
    while let Ok((id, data)) = output_rx.recv() {
        println!("id {} len {}", id, data.len());
        data.iter().for_each(|item| {
            assert_eq!(*item, id + 10);
        });
        count[id as usize] += data.len();
    }

    assert_eq!(count, vec![1023, 1024, 1025]);
    pegasus::shutdown_all();
}

#[test]
fn flat_map_iteration_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("ping_pong_test_02");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        let index = worker.id.index;
        worker.dataflow(move |builder| {
            let source = if index == 0 {
                builder.input_from_iter(0..10u32)
            } else {
                builder.input_from_iter(0..0)
            }?;

            source
                .exchange_with_fn(|item: &u32| *item as u64)?
                .iterate(2, |start| {
                    start
                        .exchange_with_fn(|item: &u32| *item as u64)?
                        .flat_map_with_fn(Pipeline, |item| Ok((item..4000 + item).map(|x| Ok(x))))?
                        .filter_with_fn(|item| Ok(*item < 100))?
                        .limit(Range::Global, 10)
                })?
                .sink_by(|_| {
                    move |_, result| {
                        if let ResultSet::Data(data) = result {
                            tx.send(data).unwrap();
                        }
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
        for d in data {
            assert!(d < 100);
        }
    }
    assert_eq!(count, 10);
    pegasus::shutdown_all();
}
