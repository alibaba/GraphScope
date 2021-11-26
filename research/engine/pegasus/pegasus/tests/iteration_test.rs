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
use pegasus::api::{CorrelatedSubTask, Count, IterCondition, Iteration, Map, Reduce, Sink};
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
fn iterate_x_map_x_iterate_x_map_x_test() {
    let mut conf = JobConf::new("iterate_x_map_x_iterate_x_map_x_test");
    conf.set_workers(2);
    let mut res = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = (index * 1000)..(index * 1000 + 1000);
        move |input, output| {
            input
                .input_from(src)?
                .iterate(10, |start| {
                    start
                        .repartition(|x| Ok(*x as u64))
                        .map(|x| Ok(x + 1))
                })?
                .iterate(10, |start| {
                    start
                        .repartition(|x| Ok(*x as u64))
                        .map(|x| Ok(x + 1))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let mut vec = vec![];
    while let Some(Ok(r)) = res.next() {
        vec.push(r);
    }
    assert_eq!(vec.len(), 2000);
    vec.sort();
    assert_eq!(vec, (20..2020).into_iter().collect::<Vec<u32>>());
}

#[test]
fn iterate_x_map_reduce_unfold_x_test() {
    let mut conf = JobConf::new("iterate_x_map_reduce_unfold_x_test");
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
fn iterate_x_iterate_x_map_x_x_test() {
    let mut conf = JobConf::new("iterate_x_iterate_x_map_x_x_test");
    conf.set_workers(2);
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
fn iterate_x_iterate_x_iterate_x_map_x_x_x_test() {
    let mut conf = JobConf::new("iterate_x_iterate_x_iterate_x_map_x_x_x_test");
    conf.set_workers(2);
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
fn map_iterate_until_x_iterate_until_x_map_x_map_x_test() {
    let mut conf = JobConf::new("map_iterate_until_x_iterate_until_x_map_x_map_x_test");
    conf.set_workers(2);
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

fn iterate_x_apply_x_flatmap_count_x_map_x(workers: u32) {
    let name = format!("iterate_x_apply_x_flatmap_count_x_map_x_{}_test", workers);
    let mut conf = JobConf::new(name);
    conf.set_workers(workers);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index + 1;
        let src = vec![index];
        move |input, output| {
            input
                .input_from(src)?
                .iterate(10, |start| {
                    start
                        .repartition(|x| Ok(*x as u64))
                        .apply(|stream| stream.flat_map(|x| Ok(0..x + 1))?.count())?
                        .map(|(_p, c)| Ok(c as u32))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");

    let mut vec = vec![];
    while let Some(Ok(item)) = result.next() {
        vec.push(item);
    }

    let mut expect = vec![];
    for i in 0..workers {
        let mut value = i + 1;
        for _ in 0..10 {
            value = Some(value)
                .into_iter()
                .flat_map(|x| 0..x + 1)
                .count() as u32;
        }
        expect.push(value);
    }
    vec.sort();
    println!("get result {:?}", vec);
    assert_eq!(expect, vec);
}

#[test]
fn iterate_x_apply_x_flatmap_count_x_map_x_2_test() {
    iterate_x_apply_x_flatmap_count_x_map_x(2)
}

#[test]
fn iterate_x_apply_x_flatmap_count_x_map_x_3_test() {
    iterate_x_apply_x_flatmap_count_x_map_x(3)
}

#[test]
fn iterate_x_apply_x_flatmap_count_x_map_x_4_test() {
    iterate_x_apply_x_flatmap_count_x_map_x(4)
}

#[test]
fn iterate_x_apply_x_flatmap_count_x_map_x_5_test() {
    iterate_x_apply_x_flatmap_count_x_map_x(5)
}

#[test]
fn flatmap_iterate_x_apply_x_flatmap_count_x_map_x_test() {
    let mut conf = JobConf::new("flatmap_iterate_x_apply_x_flatmap_count_x_map_x_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = vec![index];
        move |input, output| {
            input
                .input_from(src)?
                .flat_map(|i| Ok((i * 1000)..(i * 1000 + 1000)))?
                .iterate(10, |start| {
                    start
                        .repartition(|x| Ok(*x as u64))
                        .apply(|stream| stream.flat_map(|x| Ok(0..x + 1))?.count())?
                        .map(|(_p, c)| Ok(c as u32))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure;");

    let mut vec = vec![];
    while let Some(Ok(item)) = result.next() {
        vec.push(item);
    }

    let mut expect = vec![];
    for i in 0..2000 {
        let mut value = i;
        for _ in 0..10 {
            value = Some(value)
                .into_iter()
                .flat_map(|x| 0..x + 1)
                .count() as u32;
        }
        expect.push(value);
    }
    vec.sort();
    println!("get {} result from {}..{}", vec.len(), vec[0], vec.last().unwrap());
    assert_eq!(expect, vec);
}

#[test]
fn apply_x_iterate_x_flatmap_x_count_x_test() {
    let mut conf = JobConf::new("apply_x_iterate_x_flatmap_x_count_x_test");
    conf.set_workers(2);
    let mut result = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        let src = index * 10..(index + 1) * 10;
        move |input, output| {
            input
                .input_from(src)?
                .apply(|sub| {
                    sub.iterate(10, |iter| {
                        iter.repartition(|x| Ok(*x as u64))
                            .flat_map(|x| Ok(x..(x + 2)))
                    })?
                    .count()
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let mut vec = vec![];
    while let Some(Ok(item)) = result.next() {
        vec.push(item.1);
    }

    println!("get result {:?}", vec);
}

#[macro_use]
extern crate lazy_static;

lazy_static! {
    pub static ref MAP: std::collections::HashMap<u32, (Vec<u32>, Vec<u32>)> = vec![
        (1, (vec![2, 3, 4], vec![])),
        (2, (vec![], vec![1])),
        (3, (vec![], vec![1, 4, 6])),
        (4, (vec![3, 5], vec![1])),
        (5, (vec![], vec![4])),
        (6, (vec![3], vec![]))
    ]
    .into_iter()
    .collect();
}

#[test]
fn modern_graph_iter_times2_and_times2() {
    let mut conf = JobConf::new("modern_graph_iter_times2_and_times2");
    let num_workers = 2;
    conf.set_workers(num_workers);

    let result_stream = pegasus::run(conf, || {
        let index = pegasus::get_current_worker().index;
        move |input, output| {
            input
                .input_from((1..7).filter(move |x| *x % num_workers == index))?
                .iterate(2, |sub| {
                    sub.repartition(|x| Ok(*x as u64))
                        .flat_map(move |x| Ok(MAP.get(&x).unwrap().0.iter().cloned()))
                })?
                .iterate(2, |sub| {
                    sub.repartition(|x| Ok(*x as u64))
                        .flat_map(move |x| Ok(MAP.get(&x).unwrap().1.iter().cloned()))
                })?
                .sink_into(output)
        }
    })
    .expect("submit job failure");

    let mut results = result_stream
        .map(|item| item.unwrap())
        .collect::<Vec<u32>>();
    results.sort();

    let mut expected = Vec::new();
    for v in 1..7 {
        for n1 in &MAP.get(&v).unwrap_or(&(vec![], vec![])).0 {
            for n2 in &MAP.get(n1).unwrap_or(&(vec![], vec![])).0 {
                for n3 in &MAP.get(n2).unwrap_or(&(vec![], vec![])).1 {
                    for n4 in &MAP.get(n3).unwrap_or(&(vec![], vec![])).1 {
                        expected.push(*n4);
                    }
                }
            }
        }
    }
    expected.sort();
    assert_eq!(results, expected)
}
