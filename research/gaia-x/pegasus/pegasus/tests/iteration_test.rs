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
//
use pegasus::api::{IterCondition, Iteration, Map, Reduce, Sink};
use pegasus::JobConf;

#[test]
fn ping_pong_test_01() {
    let mut conf = JobConf::new("ping_pong_test_01");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let inputs = if index == 0 { 0..500u32 } else { 500..1000u32 };
            input
                .input_from(inputs)?
                .iterate(10, |start| {
                    start
                        .repartition(|item: &u32| Ok(*item as u64))
                        .map(|item| Ok(item + 1))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let mut count = 0;
    let mut sum = 0;
    while let Some(Ok(data)) = result.next() {
        count += 1;
        sum += data;
    }

    assert_eq!(count, 1000);
    assert_eq!(sum, 999 * 500 + 1000 * 10);
}

#[test]
fn ping_pong_test_02() {
    let mut conf = JobConf::new("ping_pong_test_02");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            let inputs = if index == 0 { 0..500u32 } else { 500..1000u32 };
            let stream = input
                .input_from(inputs)?
                .map(|item| Ok((0u32, item)))?;
            let mut condition = IterCondition::new();
            condition.until(|item: &(u32, u32)| Ok(item.0 >= 10));
            stream
                .iterate_until(condition, |start| {
                    start
                        .repartition(|item: &(u32, u32)| Ok(item.1 as u64))
                        .map(|(index, item)| Ok((index + 1, item + 1)))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let mut count = 0;
    let mut sum = 0;
    while let Some(Ok((x, y))) = result.next() {
        count += 1;
        sum += y;
        assert_eq!(x, 10);
    }
    assert_eq!(count, 1000);
    assert_eq!(sum, 999 * 500 + 1000 * 10);
}

#[test]
fn iter_with_aggregate() {
    let mut conf = JobConf::new("iter_with_aggregate");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = index * 10;
        move |input, output| {
            input
                .input_from(1..src)?
                .iterate(3, |start| {
                    start
                        .repartition(|x| Ok(*x as u64))
                        .map(|x| Ok(x + 1))?
                        .reduce(|| |a, b| Ok(a + b))?
                        .unfold(|r| {
                            println!("get {}", r);
                            Ok((1..r).rev().take(1000))
                        })
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");
    let mut count = 0;
    let mut sum = 0;
    while let Some(Ok(x)) = result.next() {
        count += 1;
        sum += x;
    }

    let i0 = (1..10u32)
        .map(|x| x + 1)
        .reduce(|a, b| a + b)
        .unwrap();
    println!("i0 = {}", i0);
    let i1 = (1..i0)
        .rev()
        .take(1000)
        .map(|x| x + 1)
        .reduce(|a, b| a + b)
        .unwrap();
    println!("i1 = {}", i1);
    let i2 = (1..i1)
        .rev()
        .take(1000)
        .map(|x| x + 1)
        .reduce(|a, b| a + b)
        .unwrap();
    println!("i2 = {}", i2);
    assert_eq!(count, 1000);
    assert_eq!(sum, (1..i2).rev().take(1000).sum());
}

#[test]
fn iter_nested_iter_test() {
    let mut conf = JobConf::new("iter_nested_iter");
    conf.set_workers(2);
    //conf.plan_print = true;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { 0..1u32 } else { 0..0u32 };
        move |input, output| {
            input
                .input_from(src)?
                .iterate(10, |start| {
                    start.iterate(10, |start| {
                        start
                            .repartition(|x| Ok(*x as u64))
                            .map(|x| Ok(x + 1))
                    })
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");
    let mut vec = vec![];
    while let Some(Ok(item)) = result.next() {
        vec.push(item);
    }
    assert_eq!(vec, vec![100]);
}

#[test]
fn iter_nested_iter_nested_iter_test() {
    let mut conf = JobConf::new("iter_nested_iter_nested_iter");
    conf.set_workers(2);
    // conf.plan_print = true;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { 0..1u32 } else { 0..0u32 };
        move |input, output| {
            input
                .input_from(src)?
                .iterate(10, |start| {
                    start.iterate(10, |start| {
                        start.iterate(10, |start| {
                            start
                                .repartition(|x| Ok(*x as u64))
                                .map(|x| Ok(x + 1))
                        })
                    })
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");
    let mut vec = vec![];
    while let Some(Ok(item)) = result.next() {
        vec.push(item);
    }
    assert_eq!(vec, vec![1000]);
}

#[test]
fn iter_nested_iter_with_condition_test() {
    let mut conf = JobConf::new("iter_nested_iter_with_condition");
    conf.set_workers(2);
    // conf.plan_print = true;
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = if index == 0 { 0..1u32 } else { 0..0u32 };
        move |input, output| {
            let mut until = IterCondition::new();
            until.until(|x: &(u32, u32)| Ok(x.1 >= 10));
            input
                .input_from(src)?
                .map(|x| Ok((x, 0)))?
                .iterate_until(until, |start| {
                    let mut until = IterCondition::new();
                    until.until(|x: &(u32, u32, u32)| Ok(x.2 >= 10));
                    start
                        .map(|(x, y)| Ok((x, y, 0)))?
                        .iterate_until(until, |start| {
                            start
                                .repartition(|x| Ok(x.0 as u64))
                                .map(|x| Ok((x.0 + 1, x.1, x.2 + 1)))
                        })?
                        .map(|x| Ok((x.0, x.1 + 1)))
                })?
                .map(|x| Ok(x.0))?
                .sink_into(output)
        }
    })
    .expect("submit job failure");
    let mut vec = vec![];
    while let Some(Ok(item)) = result.next() {
        vec.push(item);
    }
    assert_eq!(vec, vec![100]);
}
