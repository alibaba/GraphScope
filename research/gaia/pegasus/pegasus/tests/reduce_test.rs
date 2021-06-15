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

use pegasus::api::accum::{Count, CountAccum};
use pegasus::api::function::*;
use pegasus::api::{
    Barrier, Dedup, Exchange, Fold, Group, Map, Order, OrderBy, OrderDirect, Range, ResultSet, Sink,
};
use pegasus::communication::Pipeline;
use pegasus::compare;
use pegasus::{Configuration, JobConf, Tag};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::collections::{Collection, Set};
use std::collections::HashSet;
use std::io;

#[test]
fn barrier_test() {
    #[derive(Clone, Debug, Default)]
    struct MockExternStore {
        inner: Vec<u32>,
    }

    impl IntoIterator for MockExternStore {
        type Item = u32;
        type IntoIter = std::vec::IntoIter<u32>;

        fn into_iter(mut self) -> Self::IntoIter {
            self.inner.sort();
            self.inner.reverse();
            self.inner.into_iter()
        }
    }

    impl Collection<u32> for MockExternStore {
        fn add(&mut self, item: u32) -> Result<(), io::Error> {
            self.inner.push(item);
            Ok(())
        }

        fn clear(&mut self) {
            self.inner.clear();
        }

        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        fn len(&self) -> usize {
            self.inner.len()
        }
    }

    impl Encode for MockExternStore {
        fn write_to<W: WriteExt>(&self, _: &mut W) -> std::io::Result<()> {
            unimplemented!()
        }
    }

    impl Decode for MockExternStore {
        fn read_from<R: ReadExt>(_: &mut R) -> std::io::Result<Self> {
            unimplemented!()
        }
    }

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("barrier_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 8, 6, 0, 0, 7, 4];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .barrier::<MockExternStore>(Range::Global)?
                .flat_map_with_fn(Pipeline, |input| Ok(input.into_iter().map(|item| Ok(item))))?
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
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    assert_eq!(14, result.len());
    assert_eq!(vec![8, 8, 7, 7, 6, 6, 4, 4, 1, 1, 0, 0, 0, 0], result);
    pegasus::shutdown_all();
}

#[test]
fn group_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("group_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 1, 2, 2, 3, 3, 3, 4, 4, 5];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .group_by(Range::Global)?
                .sink_by(move |_meta| {
                    move |_t: &Tag, result| match result {
                        ResultSet::Data(data) => {
                            tx.send(data).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    for map in result {
        for (k, v) in map {
            match k {
                1 => {
                    assert_eq!(vec![1, 1, 1, 1], v);
                }
                2 => {
                    assert_eq!(vec![2, 2, 2, 2], v);
                }
                3 => {
                    assert_eq!(vec![3, 3, 3, 3, 3, 3], v);
                }
                4 => {
                    assert_eq!(vec![4, 4, 4, 4], v);
                }
                5 => {
                    assert_eq!(vec![5, 5], v);
                }
                _ => {
                    assert!(false);
                }
            }
        }
    }
    pegasus::shutdown_all();
}

#[test]
fn fold_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("fold_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .fold_with_accum(Range::Global, CountAccum::new())?
                .sink_by(move |_meta| {
                    move |_t: &Tag, result: ResultSet<Count<u32>>| match result {
                        ResultSet::Data(data) => {
                            tx.send(data).expect("send error");
                        }
                        _ => (),
                    }
                })?;
            Ok(())
        })
    })
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    assert_eq!(1, result.len());
    assert_eq!(24, result[0].value);
    pegasus::shutdown_all();
}

#[test]
fn dedup_test() {
    #[derive(Clone, Debug, Default)]
    struct MockExternSet {
        inner: HashSet<u32>,
    }

    impl Collection<u32> for MockExternSet {
        fn add(&mut self, item: u32) -> Result<(), io::Error> {
            self.inner.insert(item);
            Ok(())
        }

        fn clear(&mut self) {
            self.inner.clear()
        }

        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        fn len(&self) -> usize {
            self.inner.len()
        }
    }

    impl Set<u32> for MockExternSet {
        fn contains(&self, item: &u32) -> bool {
            self.inner.contains(item)
        }
    }

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("dedup_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .dedup::<MockExternSet>(Range::Global)?
                .sort(Range::Global, OrderDirect::Asc)?
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
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    assert_eq!(5, result.len());
    assert_eq!(vec![1, 2, 3, 4, 5], result);
    pegasus::shutdown_all();
}

#[test]
fn sort_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("sort_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 8, 6, 0, 0, 7, 4];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .sort(Range::Global, OrderDirect::Desc)?
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
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    assert_eq!(14, result.len());
    assert_eq!(vec![8, 8, 7, 7, 6, 6, 4, 4, 1, 1, 0, 0, 0, 0], result);
    pegasus::shutdown_all();
}

#[test]
fn top_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("top_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 8, 6, 0, 0, 7, 4];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .top(5, Range::Global, OrderDirect::Desc)?
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
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    assert_eq!(5, result.len());
    assert_eq!(vec![8, 8, 7, 7, 6], result);
    pegasus::shutdown_all();
}

#[test]
fn custom_top_test() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let mut conf = JobConf::new("custom_top_test");
    conf.set_workers(2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 8, 6, 0, 0, 7, 4];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .top_by(5, Range::Global, compare!(|a: &u32, b: &u32| b.cmp(a)))?
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
    .expect("");
    std::mem::drop(tx);

    let mut result = Vec::new();
    while let Ok(data) = rx.recv() {
        result.extend(data);
    }
    assert_eq!(5, result.len());
    assert_eq!(vec![8, 8, 7, 7, 6], result);
    pegasus::shutdown_all();
}
