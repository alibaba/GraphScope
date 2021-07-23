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

use pegasus::api::{
    Count, Exchange, Filter, Iteration, Limit, Map, Range, ResultSet, Sink, SubTask,
};
use pegasus::communication::Pipeline;
use pegasus::{Configuration, JobConf};
use std::collections::HashMap;

#[test]
fn test_subtask_fork() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("test_subtask_fork");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = if dfb.worker_id.index == 0 {
                let vec = (0..10).collect::<Vec<u32>>();
                dfb.input_from_iter(vec.into_iter())
            } else {
                dfb.input_from_iter(Vec::<u32>::new().into_iter())
            }?;
            let p = src.exchange_with_fn(|item: &u32| *item as u64)?;
            let subtask = p.fork_subtask(|stream| {
                stream.flat_map_with_fn(Pipeline, |item| {
                    Ok(vec![item + 1; 8].into_iter().map(|x| Ok(x)))
                })
            })?;
            subtask.sink_by(|_meta| {
                move |_, r| match r {
                    ResultSet::Data(data) => {
                        for d in data {
                            tx.send(d).expect("sink result failure")
                        }
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
    let mut res_map = HashMap::new();
    while let Ok(r) = rx.recv() {
        match r.take() {
            ResultSet::Data(data) => {
                count += data.len();
                for d in data {
                    let group_count = res_map.entry(d).or_insert(0);
                    *group_count += 1;
                }
            }
            _ => (),
        }
    }
    assert_eq!(count, 80);
    assert_eq!(res_map.len(), 10);
    for i in 1..11 {
        let r = res_map.get(&i).map(|i| *i);
        assert_eq!(r, Some(8))
    }
    pegasus::shutdown_all();
}

#[test]
fn test_subtask_fork_join() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("test_subtask_fork_join");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = if dfb.worker_id.index == 0 {
                let vec = (0..2000).collect::<Vec<u32>>();
                dfb.input_from_iter(vec.into_iter())
            } else {
                dfb.input_from_iter(Vec::<u32>::new().into_iter())
            }?;
            let p = src.exchange_with_fn(|item: &u32| *item as u64)?;
            let subtask = p.fork_subtask(|stream| {
                stream.flat_map_with_fn(Pipeline, |item| {
                    Ok(vec![item + 1; 8].into_iter().map(|x| Ok(x)))
                })
            })?;
            let join = p.join_subtask(subtask, move |p, s| Some(s - *p))?;
            join.sink_by(|_| {
                move |_, r| match r {
                    ResultSet::Data(data) => {
                        tx.send(data).expect("sink data failure;");
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
    while let Ok(r) = rx.recv() {
        count += r.len();
        for d in r {
            assert_eq!(d, 1);
        }
    }
    assert_eq!(count, 8 * 2000);
    pegasus::shutdown_all();
}

#[test]
fn test_subtask_fork_count_join() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("test_subtask_count_fork_join");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = if dfb.worker_id.index == 0 {
                let vec = (0..10).collect::<Vec<u32>>();
                dfb.input_from_iter(vec.into_iter())
            } else {
                dfb.input_from_iter(Vec::<u32>::new().into_iter())
            }?;

            let p = src.exchange_with_fn(|item: &u32| *item as u64)?;
            let subtask = p.fork_subtask(|stream| {
                stream
                    .flat_map_with_fn(Pipeline, |item| {
                        let size = (item + 4000) as usize;
                        Ok(vec![item; size].into_iter().map(|x| Ok(x)))
                    })?
                    .count(Range::Global)
            })?;

            let join = p.join_subtask(subtask, move |p, s| Some((*p, s)))?;
            join.sink_by(|_| {
                move |_, r| match r {
                    ResultSet::Data(data) => {
                        tx.send(data).expect("sink data failure;");
                    }
                    _ => (),
                }
            })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);
    while let Ok(r) = rx.recv() {
        for (i, count) in r {
            assert_eq!(i + 4000, count as u32);
        }
    }
    pegasus::shutdown_all();
}

#[test]
fn test_subtask_in_iteration_01() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("test_subtask_count_fork_join");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = if dfb.worker_id.index == 0 {
                dfb.input_from_iter(0..1)
            } else {
                dfb.input_from_iter(0..0)
            }?;

            src.iterate(2, |start| {
                let parent = start.exchange_with_fn(|item: &u32| *item as u64)?;
                let sub = parent.fork_subtask(|sub| {
                    sub.flat_map_with_fn(Pipeline, |item| Ok((item..10000 + item).map(|x| Ok(x))))?
                        .filter_with_fn(|item| Ok(*item < 2))
                })?;

                parent.join_subtask(sub, |p, s| Some(*p + s))
            })?
            .sink_by(|_| {
                move |_, r| match r {
                    ResultSet::Data(data) => {
                        tx.send(data).expect("sink data failure;");
                    }
                    _ => (),
                }
            })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);
    let mut vec = vec![];
    while let Ok(r) = rx.recv() {
        for d in r {
            assert!(d <= 2);
            vec.push(d);
        }
    }
    vec.sort();
    assert_eq!(vec, vec![0, 1, 2]);
    pegasus::shutdown_all();
}

#[test]
fn test_subtask_in_iteration_02() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("test_subtask_count_fork_join");
    conf.set_workers(2);
    let (tx, rx) = crossbeam_channel::unbounded();
    pegasus::run(conf, |worker| {
        let tx = tx.clone();
        worker.dataflow(|dfb| {
            let src = if dfb.worker_id.index == 0 {
                dfb.input_from_iter(0..1)
            } else {
                dfb.input_from_iter(0..0)
            }?;

            src.iterate(2, |start| {
                let sub = start.fork_subtask(|sub| {
                    sub.exchange_with_fn(|item: &u32| *item as u64)?
                        .flat_map_with_fn(Pipeline, |item| Ok((item..10000 + item).map(|x| Ok(x))))?
                        .limit(Range::Local, 10)?
                        .exchange_with_fn(|item: &u32| *item as u64)?
                        .filter_with_fn(|item| Ok(*item < 2))
                })?;

                start.join_subtask(sub, |p, s| Some(*p + s))
            })?
            .sink_by(|_| {
                move |_, r| match r {
                    ResultSet::Data(data) => {
                        tx.send(data).expect("sink data failure;");
                    }
                    _ => (),
                }
            })?;
            Ok(())
        })
    })
    .expect("submit job failure;");

    std::mem::drop(tx);
    let mut vec = Vec::new();
    while let Ok(r) = rx.recv() {
        for d in r {
            assert!(d <= 2);
            vec.push(d);
        }
    }
    vec.sort();
    assert_eq!(vec, vec![0, 1, 2]);
    pegasus::shutdown_all();
}
