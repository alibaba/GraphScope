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
use pegasus::api::notify::Notification;
use pegasus::api::state::OperatorState;
use pegasus::api::Range::Global;
use pegasus::api::{
    Exchange, Filter, Limit, Map, Multiplexing, NonBlockReceiver, Unary, UnaryNotify, UnaryState,
};
use pegasus::api::{ResultSet, Sink};
use pegasus::box_route;
use pegasus::communication::{Aggregate, Input, Output, Pipeline};
use pegasus::errors::JobExecError;
use pegasus::{Configuration, Data, JobConf, Tag};
use std::collections::HashMap;

/// Test unary that just forward input to output;
/// Sink results to one collector, check if count is correct;
#[test]
fn unary_test_01() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("unary_test_01");
    conf.plan_print = true;
    let guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            builder
                .input_from_iter(vec![0u32; 1024].into_iter())?
                .unary("unary", Pipeline, |_meta| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            output.forward(dataset)?;
                            Ok(())
                        })
                    }
                })?
                .sink_by(move |_meta| {
                    move |_t: &Tag, result: ResultSet<u32>| match result {
                        ResultSet::Data(data) => {
                            tx.send(data).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure:");

    std::mem::drop(tx);

    let mut count = 0;
    while let Ok(data) = rx.recv() {
        count += data.len();
    }
    assert_eq!(1024, count);
    guard.unwrap().join().unwrap();
    pegasus::shutdown_all();
}

/// Test unary that just forward input to output, and then shuffle data between workers;
/// Check whether unary with shuffle is correct;
#[test]
fn unary_test_02() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("unary_test_02");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            builder
                .input_from_iter(0..1024u32)?
                .unary("unary", Pipeline, |_info| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            output.forward(dataset)?;
                            Ok(())
                        })
                    }
                })?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .sink_by(move |meta| {
                    let index = meta.worker_id.index as usize;
                    move |_t: &Tag, result: ResultSet<u32>| match result {
                        ResultSet::Data(data) => {
                            tx.send((index, data)).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure");

    std::mem::drop(tx);

    let mut count = 0;
    while let Ok((index, data)) = rx.recv() {
        for item in data.iter() {
            assert_eq!(*item % 2, index as u32)
        }
        count += data.len();
    }
    assert_eq!(2048, count);
    pegasus::shutdown_all();
}

/// Test unary that consume one input and produce one output;
#[test]
fn unary_test_03() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("unary_test_03");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            builder
                .input_from_iter(0..2048u32)?
                .unary("unary", Pipeline, |_meta| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            for item in dataset.drain(..) {
                                output.give(item % 2)?;
                            }
                            Ok(())
                        })
                    }
                })?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .sink_by(move |meta| {
                    let index = meta.worker_id.index as usize;
                    move |_t: &Tag, result: ResultSet<u32>| match result {
                        ResultSet::Data(data) => {
                            tx.send((index, data)).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure");

    std::mem::drop(tx);

    let mut count = 0;
    while let Ok((index, data)) = rx.recv() {
        for item in data.iter() {
            assert!(*item < 2);
            assert_eq!(*item, index as u32)
        }
        count += data.len();
    }
    assert_eq!(4096, count);
    pegasus::shutdown_all();
}

/// Test unary chain: shuffle-map-filter
#[test]
fn unary_test_04() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("unary_test_04");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            builder
                .input_from_iter(0..2000u32)?
                .exchange_with_fn(|item| *item as u64)?
                .map_with_fn(Pipeline, |item| Ok(item % 5))?
                .filter_with_fn(|item| Ok(*item > 0))?
                .sink_by(move |_info| {
                    move |_t: &Tag, result: ResultSet<u32>| match result {
                        ResultSet::Data(data) => {
                            tx.send(data).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);

    let mut count = 0;
    while let Ok(data) = rx.recv() {
        for item in data.iter() {
            assert!(*item > 0);
            assert!(*item < 5)
        }
        count += data.len();
    }
    assert_eq!(3200, count);
    pegasus::shutdown_all();
}

/// Test unary chain: shuffle-map-filter-limit
#[test]
fn unary_test_05() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("unary_test_04");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|builder| {
            builder
                .input_from_iter(0..2000u32)?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .map_with_fn(Pipeline, |item: u32| Ok(item % 5))?
                .filter_with_fn(|item: &u32| Ok(*item > 0))?
                .limit(Global, 1024)?
                .sink_by(move |_info| {
                    move |_t: &Tag, result: ResultSet<u32>| match result {
                        ResultSet::Data(data) => {
                            tx.send(data).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);

    let mut count = 0;
    while let Ok(data) = rx.recv() {
        for item in data.iter() {
            assert!(*item > 0);
            assert!(*item < 5)
        }
        count += data.len();
    }
    assert_eq!(1024, count);
    pegasus::shutdown_all();
}

#[test]
fn unary_test_notify_01() {
    #[derive(Default)]
    struct LocalCount {
        count: HashMap<Tag, u64>,
    }

    impl<I: Data> UnaryNotify<I, u64> for LocalCount {
        type NotifyResult = Option<u64>;

        fn on_receive(
            &mut self, input: &mut Input<I>, _: &mut Output<u64>,
        ) -> Result<(), JobExecError> {
            input.subscribe_notify();
            input.for_each_batch(|dataset| {
                let count = self.count.entry(dataset.tag()).or_insert(0);
                *count += dataset.len() as u64;
                Ok(())
            })
        }

        fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
            self.count.remove(&n.tag)
        }
    }

    #[derive(Default)]
    struct GlobalSum {
        sum: HashMap<Tag, u64>,
    }

    impl UnaryNotify<u64, u64> for GlobalSum {
        type NotifyResult = Option<u64>;

        fn on_receive(
            &mut self, input: &mut Input<u64>, _: &mut Output<u64>,
        ) -> Result<(), JobExecError> {
            input.subscribe_notify();
            input.for_each_batch(|dataset| {
                let sum = self.sum.entry(dataset.tag()).or_insert(0);
                *sum += dataset[0];
                Ok(())
            })
        }

        fn on_notify(&mut self, n: &Notification) -> Self::NotifyResult {
            self.sum.remove(&n.tag)
        }
    }

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("unary_test_notify_01");
    conf.set_workers(2);

    let (output_tx, output_rx) = crossbeam_channel::unbounded();
    let (input_tx, input_rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let index = worker.id.index;
        let rx = input_rx.clone();
        let tx = output_tx.clone();
        worker.dataflow(move |builder| {
            let stream = if index == 0 {
                let rx = NonBlockReceiver::new(rx);
                builder.input_from(rx)
            } else {
                builder.input_from_iter(Vec::<Option<u32>>::new().into_iter())
            }?;

            let mut incr_id = 0u32;
            let multiplex = stream.scope_by(move |data| {
                if data.is_some() {
                    Some(incr_id)
                } else {
                    incr_id += 1;
                    None
                }
            })?;

            let channel = box_route!(|item: &Option<u32>| (*item.as_ref().unwrap_or(&0)) as u64);
            multiplex
                .unary("filter", channel, |_meta| {
                    |input, output| {
                        input.for_each_batch(|dataset| {
                            for item in dataset.drain(..) {
                                if let Some(item) = item {
                                    output.give(item)?;
                                }
                            }
                            Ok(())
                        })
                    }
                })?
                .unary_with_notify("local_count", Pipeline, |_| LocalCount::default())?
                .unary_with_notify("global_sum", Aggregate(0), |_| GlobalSum::default())?
                .sink_by(move |_meta| {
                    move |t: &Tag, result: ResultSet<u64>| match result {
                        ResultSet::Data(data) => {
                            assert_eq!(data.len(), 1);
                            let t = t.current_uncheck();
                            tx.send((t, data[0])).unwrap();
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure");

    for _i in 0..512 {
        input_tx.send(Some(0u32)).unwrap();
    }
    input_tx.send(None).unwrap();

    for _i in 0..1024 {
        input_tx.send(Some(1u32)).unwrap();
    }
    input_tx.send(None).unwrap();

    for _i in 0..2048 {
        input_tx.send(Some(2u32)).unwrap();
    }
    input_tx.send(None).unwrap();

    std::mem::drop(input_tx);
    std::mem::drop(output_tx);

    while let Ok((id, count)) = output_rx.recv() {
        if id == 0 {
            assert_eq!(512, count);
        } else if id == 1 {
            assert_eq!(1024, count);
        } else if id == 2 {
            assert_eq!(2048, count)
        } else {
            panic!("unknown id {}", id);
        }
    }
    pegasus::shutdown_all();
}

#[test]
fn unary_test_state_01() {
    struct LocalCount<D> {
        _ph: std::marker::PhantomData<D>,
    }

    impl<D: Data> UnaryState<D, u64, u64> for LocalCount<D> {
        type NotifyResult = Option<u64>;

        fn on_receive(
            &self, input: &mut Input<D>, _: &mut Output<u64>, state: &mut OperatorState<u64>,
        ) -> Result<(), JobExecError> {
            input.for_each_batch(|dataset| {
                **state += dataset.len() as u64;
                Ok(())
            })
        }

        fn on_notify(&self, state: u64) -> Self::NotifyResult {
            Some(state)
        }
    }

    struct GlobalSum;

    impl UnaryState<u64, u64, u64> for GlobalSum {
        type NotifyResult = Option<u64>;

        fn on_receive(
            &self, input: &mut Input<u64>, _: &mut Output<u64>, state: &mut OperatorState<u64>,
        ) -> Result<(), JobExecError> {
            input.for_each_batch(|dataset| {
                **state += dataset[0];
                Ok(())
            })
        }

        fn on_notify(&self, state: u64) -> Self::NotifyResult {
            Some(state)
        }
    }

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("unary_test_state_01");
    conf.set_workers(2);

    let (input_tx, input_rx) = crossbeam_channel::unbounded();
    let (output_tx, output_rx) = crossbeam_channel::unbounded();
    let _guard = pegasus::run(conf, |worker| {
        let index = worker.id.index;
        let rx = input_rx.clone();
        let tx = output_tx.clone();
        worker.dataflow(move |builder| {
            let stream = if index == 0 {
                let rx = NonBlockReceiver::new(rx);
                builder.input_from(rx)
            } else {
                builder.input_from_iter(Vec::<Option<u32>>::new().into_iter())
            }?;

            let mut incr_id = 0u32;
            let multiplex = stream
                .scope_by(move |data| {
                    if data.is_some() {
                        Some(incr_id)
                    } else {
                        incr_id += 1;
                        None
                    }
                })?
                .map_with_fn(Pipeline, |data| Ok(data.unwrap()))?;

            multiplex
                .exchange_with_fn(|item: &u32| *item as u64)?
                .unary_with_state("local_count", Pipeline, |_| LocalCount {
                    _ph: ::std::marker::PhantomData,
                })?
                .unary_with_state("global_sum", Aggregate(0), |_| GlobalSum)?
                .sink_by(move |_meta| {
                    move |t: &Tag, result: ResultSet<u64>| match result {
                        ResultSet::Data(data) => {
                            assert_eq!(data.len(), 1);
                            let t = t.current_uncheck();
                            tx.send((t, data[0])).unwrap();
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    for i in 0..512u32 {
        input_tx.send(Some(i)).unwrap();
    }
    input_tx.send(None).unwrap();

    for i in 0..1024u32 {
        input_tx.send(Some(i)).unwrap();
    }
    input_tx.send(None).unwrap();

    for i in 0..2048u32 {
        input_tx.send(Some(i)).unwrap();
    }
    input_tx.send(None).unwrap();

    std::mem::drop(input_tx);
    std::mem::drop(output_tx);

    while let Ok((id, count)) = output_rx.recv() {
        if id == 0 {
            assert_eq!(512, count);
        } else if id == 1 {
            assert_eq!(1024, count);
        } else if id == 2 {
            assert_eq!(2048, count)
        } else {
            panic!("unknown id {}", id);
        }
    }
    pegasus::shutdown_all();
}
