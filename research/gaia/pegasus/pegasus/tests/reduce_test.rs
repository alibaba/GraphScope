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
    Barrier, Dedup, Exchange, Map, Order, OrderBy, OrderDirect, Range, ResultSet, Sink,
};
use pegasus::communication::Pipeline;
use pegasus::compare;
use pegasus::{Configuration, JobConf, Tag};
use pegasus_common::codec::{Decode, Encode, ReadExt, WriteExt};
use pegasus_common::collections::{Collection, Drain, DrainSet, Set};
use std::collections::HashSet;

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
        fn add(&mut self, item: u32) -> Option<u32> {
            self.inner.push(item);
            None
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
    let conf = JobConf::new(1, "unary_test_01", 2);
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = vec![1u32, 8, 6, 0, 0, 7, 4];
            dfb.input_from_iter(src.into_iter())?
                .exchange_with_fn(|item: &u32| *item as u64)?
                .barrier::<MockExternStore>(Range::Global)?
                .flat_map_with_fn(Pipeline, |input| input.into_iter().map(|item| Ok(item)))?
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
fn dedup_test() {
    #[derive(Clone, Debug, Default)]
    struct MockExternSet {
        inner: HashSet<u32>,
    }

    impl Collection<u32> for MockExternSet {
        fn add(&mut self, item: u32) -> Option<u32> {
            self.inner.insert(item);
            None
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

    impl Set<u32> for MockExternSet {}

    impl Drain<u32> for MockExternSet {
        type Target = std::collections::hash_set::IntoIter<u32>;

        fn drain(&mut self) -> Self::Target {
            self.inner.clone().into_iter()
        }
    }

    impl DrainSet<u32> for MockExternSet {}

    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let (tx, rx) = crossbeam_channel::unbounded();
    let conf = JobConf::new(1, "unary_test_01", 2);
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
    let conf = JobConf::new(1, "unary_test_01", 2);
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
    let conf = JobConf::new(1, "unary_test_01", 2);
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
    let conf = JobConf::new(1, "unary_test_01", 2);
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
